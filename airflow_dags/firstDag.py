from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Fungsi sederhana yang akan dijalankan oleh task
def print_hello():
    print("Hello, Airflow!")

# Definisi DAG
with DAG(
    dag_id="hello_airflow",  # Nama DAG
    schedule_interval="@daily",  # Menjalankan DAG setiap hari
    start_date=datetime(2025, 3, 5),  # Tanggal mulai eksekusi
    catchup=False,  # Mencegah eksekusi backlog jika DAG baru dibuat
) as dag:

    # Task: Menjalankan fungsi print_hello
    task_hello = PythonOperator(
        task_id="print_hello_task",
        python_callable=print_hello
    )

    # Menentukan urutan eksekusi task
    task_hello
