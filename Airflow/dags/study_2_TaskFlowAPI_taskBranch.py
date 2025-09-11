from airflow.decorators import dag, task
from datetime import datetime
from airflow.utils.trigger_rule import TriggerRule

@dag(
    dag_id="dag_branching_example",
    start_date=datetime(2025,9,10),
    schedule="@daily",
    catchup=False,
    tags=["study", "exploration", "non-group"]
)
def my_dag():

    @task
    def start():
        print("[INFO] return 'Mulai Proses'")
        return "Mulai Proses"

    @task.branch
    def decide_branch():
        condition = True
        if condition:
            return "task1"
        return "task2"

    @task
    def task1():
        print("[INFO] Task A berjalan..")

    @task
    def task2():
        print("[INFO] Task B berjalan..")

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def finish():
        print("[INFO] Semua task berhasil berjalan..")

    s = start()
    choice = decide_branch()
    a = task1()
    b = task2()
    f = finish()

    s >> choice
    choice >> a >> f
    choice >> b >> f

my_dag()