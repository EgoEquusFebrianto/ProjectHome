from airflow.sdk import dag, task, task_group
from datetime import datetime

@dag(
    dag_id="example_task_group_with_decorator",
    start_date=datetime(2025, 9, 10),
    schedule="@daily",
    catchup=False,
    tags=["study", "exploration", "group"]
)
def my_dag():

    @task
    def start():
        return "Mulai Proses"

    @task
    def end():
        print("Proses Selesai")

    @task_group()
    def etl_task():

        @task
        def extract():
            return [1,2,3]

        @task
        def transform(data: list):
            return [x * 2 for x in data]

        @task
        def load(data: list):
            print(f"Loaded data {data}")

        data = extract()
        transformed = transform(data)
        load(transformed)

    start() >> etl_task() >> end()

my_dag()