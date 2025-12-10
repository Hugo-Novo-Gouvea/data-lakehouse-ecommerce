from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Definição dos argumentos padrão da DAG
default_args = {
    'owner': 'hugo',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definição da DAG (Nome, Agendamento, Tags)
with DAG(
    'pipeline_ecommerce_completo',  # Nome que aparece na interface do Airflow
    default_args=default_args,
    description='Pipeline Ponta a Ponta: API -> Bronze -> Silver',
    schedule_interval='0 9 * * *',  # Roda todo dia às 09:00am
    catchup=False,
    tags=['ecommerce', 'bronze', 'silver', 'etl'],
) as dag:

    # Tarefa 1: Ingestão (API -> Bronze)
    # Traz o dado bruto em JSON
    task_bronze = BashOperator(
        task_id='ingestao_bronze',
        bash_command='python /opt/airflow/src/ingestao_bronze.py'
    )

    # Tarefa 2: Transformação (Bronze -> Silver)
    # Lê o JSON, limpa com Pandas e salva em Parquet
    task_silver = BashOperator(
        task_id='transformacao_silver',
        bash_command='python /opt/airflow/src/transformacao_silver.py'
    )

    # Orquestração: Bronze roda primeiro. Se der sucesso, roda Silver.
    task_bronze >> task_silver