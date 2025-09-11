import pendulum
from airflow.sdk import DAG, task, task_group

@task
def task_start():
    return "[Task_start]"

@task
def task_1(value: int) -> str:
    return f"[Task1 {value}]"

@task
def task_2(value: str) -> str:
    return f"[Task2 {value}]"

@task
def task_3(value: str) -> None:
    print(f"[Task3 {value}]")

@task
def task_end() -> None:
    print("[Task_end]")

@task_group()
def task_group_function(value: int) -> None:
    task_3(task_2(task_1(value)))

with DAG(
    dag_id="example_task_group_decorator",
    schedule=None,
    start_date=pendulum.datetime(2025, 9, 10, tz="Asia/Jakarta"),
    catchup=False,
    tags=["study", "exploration", "group"]
) as dag:
    start_task = task_start()
    end_task = task_end()
    for i in range(5):
        current_task_group = task_group_function(i)
        start_task >> current_task_group >> end_task