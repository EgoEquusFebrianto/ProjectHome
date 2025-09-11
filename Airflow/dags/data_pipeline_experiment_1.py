# from airflow.sdk import dag, task
# from datetime import datetime
# import pandas as pd
#
# path = "/home/kudadiri/data/raw"
#
# def generate_dataset(ds: str) -> str:
#     file_path = f"{path}/transactions_{ds}.csv"
#
#     timestamps = pd.date_range(
#         start=f"{ds} 08:00:00",
#         periods=5,
#         freq="5min"
#     ).strftime("%Y-%m-%dT%H:%M:%SZ")
#
#     data = [
#         [1, timestamps[0], 10, "Laptop", "Electronics", 1, 1200],
#         [2, timestamps[1], 11, "Shirt", "Clothing", 2, 25],
#         [3, timestamps[2], 12, "TV", "Electronics", 1, 450],
#         [4, timestamps[3], 13, "Perfume", "Beauty", 1, 60],
#         [5, timestamps[4], 14, "Shoes", "Clothing", -1, 50],  # invalid
#     ]
#
#     df = pd.DataFrame(data, columns=[
#         "tx_id", "ts", "customer_id", "product", "category", "qty", "price"
#     ])
#
#     df.to_csv(file_path, index=False)
#     print(f"Dataset dummy dibuat: {file_path}")
#     return file_path
#
# @dag(
#     dag_id="elt_experiment_1",
#     schedule="",
#     start_date=datetime(2025,8,30),
#     catchup=False,
#     tags=["elt", "experiment"]
#     )
# def experiment_no_1():
#
#     @task
#     def extract(ds=None):
#         pass
#
# experiment_no_1()