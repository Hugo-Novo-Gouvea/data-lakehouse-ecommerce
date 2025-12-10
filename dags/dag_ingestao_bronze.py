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
    'ingestao_bronze_ecommerce',
    default_args=default_args,
    description='Pipeline de ingestão da camada Bronze (FakeStore API)',
    schedule_interval='0 9 * * *', # Roda todo dia às 09:00am (Cron Syntax)
    catchup=False, # Não tenta rodar os dias passados se você pausar
    tags=['bronze', 'ecommerce', 'ingestao'],
) as dag:

    # Tarefa 1: Rodar o script Python que criamos
    task_ingestao_api = BashOperator(
        task_id='run_ingestao_bronze',
        # Aqui usamos o caminho mapeado DENTRO do container
        bash_command='python /opt/airflow/src/ingestao_bronze.py'
    )

    # Se tivessemos mais tarefas, fariamos: task1 >> task2
    task_ingestao_api