import pandas as pd
import boto3
import os
import io
import logging
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def conectar_minio():
    # LÓGICA DE SÊNIOR: Detecção automática de ambiente
    # A variável 'AIRFLOW_HOME' só existe dentro do container do Airflow.
    if os.getenv('AIRFLOW_HOME'):
        # Se estou no Docker, uso o nome interno
        endpoint = 'http://minio:9000'
    else:
        # Se estou no PC do Hugo, uso localhost
        endpoint = 'http://localhost:9000'
    
    # OBS: Não precisamos mais da variável MINIO_ENDPOINT do .env para isso funcionar
    
    return boto3.client(
        's3',
        endpoint_url=endpoint,
        aws_access_key_id=os.getenv('MINIO_ROOT_USER'),
        aws_secret_access_key=os.getenv('MINIO_ROOT_PASSWORD'),
        region_name='us-east-1'
    )

def ler_json_bronze(s3_client, bucket, arquivo):
    """Lê o arquivo JSON do MinIO e retorna um DataFrame do Pandas"""
    try:
        logging.info(f"Lendo {arquivo} do bucket {bucket}...")
        response = s3_client.get_object(Bucket=bucket, Key=arquivo)
        # Lê o conteúdo do arquivo
        conteudo = response['Body'].read()
        # Converte para DataFrame
        df = pd.read_json(io.BytesIO(conteudo))
        return df
    except Exception as e:
        logging.error(f"Erro ao ler do Bronze: {e}")
        raise e

def processar_dados(df):
    """Aplica as regras de negócio e limpeza"""
    logging.info("Iniciando processamento...")
    
    # 1. Normalização (Flatten): O campo 'rating' é um dicionário. Vamos separar.
    # Exemplo: rating: {'rate': 3.9, 'count': 120} -> vira colunas rating_rate e rating_count
    if 'rating' in df.columns:
        df_rating = pd.json_normalize(df['rating'])
        df_rating.columns = [f'rating_{c}' for c in df_rating.columns]
        df = pd.concat([df.drop('rating', axis=1), df_rating], axis=1)

    # 2. Tipagem e Renomear
    # Vamos garantir que ID é inteiro e renomear para ficar bonito
    df['category'] = df['category'].astype(str)
    df['title'] = df['title'].astype(str)
    
    # 3. Remover Duplicatas (se houver)
    df = df.drop_duplicates(subset=['id'])
    
    logging.info(f"Dados processados! Total de linhas: {len(df)}")
    return df

def salvar_silver(s3_client, df, bucket, arquivo):
    """Salva o DataFrame como PARQUET no bucket Silver"""
    try:
        logging.info(f"Salvando {arquivo} no bucket {bucket}...")
        
        # Buffer em memória (para não salvar no disco local antes de enviar)
        out_buffer = io.BytesIO()
        df.to_parquet(out_buffer, index=False)
        
        # Upload
        s3_client.put_object(
            Bucket=bucket,
            Key=arquivo,
            Body=out_buffer.getvalue()
        )
        logging.info("Arquivo Parquet salvo com sucesso!")
    except Exception as e:
        logging.error(f"Erro ao salvar na Silver: {e}")
        raise e

if __name__ == "__main__":
    cliente = conectar_minio()
    
    # Fluxo ETL
    df_raw = ler_json_bronze(cliente, 'bronze', 'raw_products.json')
    df_clean = processar_dados(df_raw)
    salvar_silver(cliente, df_clean, 'silver', 'products.parquet')