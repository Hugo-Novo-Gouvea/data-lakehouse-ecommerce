from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'hugo',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'pipeline_ecommerce_completo',
    default_args=default_args,
    description='Pipeline Ponta a Ponta: API -> Bronze -> Silver -> Gold',
    schedule_interval='0 9 * * *',
    catchup=False,
    tags=['ecommerce', 'bronze', 'silver', 'gold'],
) as dag:

    # 1. Bronze
    task_bronze = BashOperator(
        task_id='ingestao_bronze',
        bash_command='python /opt/airflow/src/ingestao_bronze.py'
    )

    # 2. Silver
    task_silver = BashOperator(
        task_id='transformacao_silver',
        bash_command='python /opt/airflow/src/transformacao_silver.py'
    )
    
    # 3. Gold (NOVO)
    task_gold = BashOperator(
        task_id='analytics_gold',
        bash_command='python /opt/airflow/src/analytics_gold.py'
    )

    # Orquestração em cadeia:
    # Bronze -> Silver -> Gold
    task_bronze >> task_silver >> task_gold