import duckdb
import boto3
import os
import logging
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def conectar_minio():
    # Lógica inteligente: Docker (minio) vs Local (localhost)
    if os.getenv('AIRFLOW_HOME'):
        endpoint = 'http://minio:9000'
    else:
        endpoint = 'http://localhost:9000'
        
    return boto3.client(
        's3',
        endpoint_url=endpoint,
        aws_access_key_id=os.getenv('MINIO_ROOT_USER'),
        aws_secret_access_key=os.getenv('MINIO_ROOT_PASSWORD'),
        region_name='us-east-1'
    )

def analise_gold(s3_client):
    logging.info("1. Baixando dados da Silver (Parquet)...")
    
    if not os.path.exists("dados_temp"):
        os.makedirs("dados_temp")
    
    # Baixa o arquivo para processamento local
    s3_client.download_file('silver', 'products.parquet', 'dados_temp/products.parquet')
    
    logging.info("2. Rodando SQL com DuckDB...")
    
    query = """
    SELECT 
        category,
        COUNT(*) as total_produtos,
        ROUND(AVG(price), 2) as preco_medio,
        ROUND(AVG(rating_rate), 1) as nota_media
    FROM 'dados_temp/products.parquet'
    GROUP BY category
    ORDER BY preco_medio DESC
    """
    
    df_resultado = duckdb.sql(query).df()
    
    # Esse print vai aparecer nos LOGS do Airflow
    print("="*50)
    print("RESULTADO GOLD")
    print(df_resultado)
    print("="*50)
    
    return df_resultado

def salvar_gold(s3_client, df):
    logging.info("3. Salvando relatório final na Gold...")
    csv_buffer = df.to_csv(index=False)
    s3_client.put_object(Bucket='gold', Key='relatorio_vendas_categorias.csv', Body=csv_buffer)
    logging.info("Sucesso!")

if __name__ == "__main__":
    cliente = conectar_minio()
    df = analise_gold(cliente)
    salvar_gold(cliente, df)
    
    # Limpeza
    if os.path.exists("dados_temp/products.parquet"):
        os.remove("dados_temp/products.parquet")