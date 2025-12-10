import requests
import boto3
import json
import logging
import os
from botocore.exceptions import ClientError
from dotenv import load_dotenv # Importar biblioteca que lê o .env

# Carrega as variaveis do arquivo .env para a memória do Python
load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def conectar_minio():
    endpoint = os.getenv('MINIO_ENDPOINT', 'http://localhost:9000')
    
    s3_client = boto3.client(
        's3',
        endpoint_url=endpoint,
        # AGORA SIM: Pegando do .env de forma segura
        aws_access_key_id=os.getenv('MINIO_ROOT_USER'),
        aws_secret_access_key=os.getenv('MINIO_ROOT_PASSWORD'),
        region_name='us-east-1'
    )
    return s3_client

def buscar_dados_api():
    url = "https://fakestoreapi.com/products"
    try:
        logging.info(f"Buscando dados de: {url}")
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Erro ao buscar dados da API: {e}")
        return None

def salvar_no_datalake(s3_client, dados, bucket_name, arquivo_nome):
    try:
        dados_json = json.dumps(dados, indent=4)
        
        logging.info(f"Salvando arquivo {arquivo_nome} no bucket {bucket_name}...")
        
        s3_client.put_object(
            Bucket=bucket_name,
            Key=arquivo_nome,
            Body=dados_json,
            ContentType='application/json'
        )
        logging.info("Arquivo salvo com sucesso!")
        
    except ClientError as e:
        logging.error(f"Erro ao salvar no MinIO: {e}")

if __name__ == "__main__":
    cliente_minio = conectar_minio()
    dados_produtos = buscar_dados_api()
    
    if dados_produtos:
        salvar_no_datalake(cliente_minio, dados_produtos, 'bronze', 'raw_products.json')