from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    "test_local_dag",
    start_date=datetime(2025, 8, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    print_hello = BashOperator(
        task_id='print_hello',
        bash_command='echo "Airflow is running locally!"'
    )
