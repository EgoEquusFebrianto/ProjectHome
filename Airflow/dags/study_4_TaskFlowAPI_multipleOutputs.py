from airflow.sdk import dag, task
import pendulum
import json

@task
def extract():
    data_string = '{"1001": 301.27, "1002": 433.21, "1003": 502.22}'

    order_data_dict = json.loads(data_string)

    return order_data_dict

@task
def transform(order_data_dict: dict):
    total_value = 0
    total_data = 0

    for value in order_data_dict.values():
        total_value += value
        total_data += 1

    average_total = total_value / total_data
    return {"total_order_value": total_value, "average_order_value": average_total}

@task
def load(total_order_value: float):
    print(f"Total order value is: {total_order_value:,.2f}")

@dag(
    dag_id="example_dag_with_multiple_outputs_true",
    schedule=None,
    start_date=pendulum.datetime(2025, 9, 10, tz="Asia/Jakarta"),
    catchup=False,
    tags=["study", "exploration", "multiple_outputs"]
)
def my_dag():
    order_data = extract()
    order_summary = transform(order_data)
    load(order_summary["total_order_value"])

my_dag()