from airflow.sdk import dag, task
from datetime import datetime

@dag(
    dag_id="dag_with_param_and_dep",
    start_date=datetime(2025,8,31),
    catchup=False,
    schedule="@daily",
    tags=["study", "exploration", "non-group"]
)
def my_dag():

    @task
    def task1():
        _map = {"value": 47}
        print(f"[INFO] Map {_map} berhasil dibuat.")
        return _map

    @task
    def task2(data: dict) -> int:
        print(f"[INFO] Operasi berhasil diimplementasikan.")
        return data['value'] * 2

    @task
    def task3(result: int):
        print(f"hasil akhir dari task adalah {result}")

    data = task1()
    result = task2(data)
    task3(result)

my_dag()