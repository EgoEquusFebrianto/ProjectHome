from datetime import timedelta, datetime

# DAG class sekarang di airflow.sdk
from airflow.sdk import DAG

# Operator Bash di providers.standard
from airflow.providers.standard.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="hello_airflow",
    default_args=default_args,
    schedule=timedelta(days=1),
    start_date=datetime(2025, 8, 27),
    catchup=False,
    description="DAG pertama",
    tags=["demo", "beginner"],
) as dag:

    task1 = BashOperator(
        task_id="print_hello",
        bash_command='echo "Hello World!"',
    )

    task2 = BashOperator(
        task_id="print_date",
        bash_command="date",
    )

    task3 = BashOperator(
        task_id="print_custom",
        bash_command='echo "DAG pertama kali berjalan."',
    )

    task1 >> task2 >> task3
