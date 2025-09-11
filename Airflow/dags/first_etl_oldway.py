from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime

url = "https://raw.githubusercontent.com/EgoEquusFebrianto/data_resource/main/spark/transaction.csv"

user = "airflow"
password = "airflow_user_password"
host = "172.25.0.1"
db = "retail_transaction"
schema = "airflow_retail_dataset"

conn = f"postgresql+psycopg2://{user}:{password}@{host}:5432/{db}"
engine = create_engine(conn)

def extract_data():
    try:
        df = pd.read_csv(url, sep=";", index_col=False)
        with engine.begin() as connection:
            df.to_sql(
                "raw_retail_dataset",
                engine, schema=schema,
                index=False,
                if_exists='append'
            )
        print(f"[INFO] Extract Data To Database {db} with schema {schema} from url {url}, Successful.")
    except Exception as e:
        print("[ERROR] Unexpected Error Occurred When Extract: ", e)

def transform_data():
    try:
        sql = f"select * from {schema}.raw_retail_dataset"
        df = pd.read_sql(sql, engine)
        df = df.drop_duplicates(subset="transaction_id")
        df["date"] = pd.to_datetime(
            df[["year", "month", "day"]].astype(str).agg("-".join, axis=1),
            format="%y-%m-%d"
        )
        df["revenue"] = df["quantity"] * df["price_per_unit"]

        with engine.begin() as connection:
            df.to_sql(
                "clean_retail_dataset",
                engine,
                schema=schema,
                index=False,
                if_exists='append'
            )
        print(f"[INFO] Cleaning and Transform Dataset schema retail_dataset_raw, Successful.")
    except Exception as e:
        print("[ERROR] Unexpected Error Occurred When Transform: ", e)

def logic_business():
    try:
        sql = f"select * from {schema}.clean_retail_dataset"
        df = pd.read_sql(sql, engine)
        df_sales_per_product = (df
            .groupby(["product_name", "year"])
            .agg(total_sales=("revenue", "sum"))
            .sort_values("total_sales", ascending=False)
        )

        with engine.begin() as connection:
            df_sales_per_product.to_sql(
                "annual_retail_sales",
                engine,
                schema=schema,
                if_exists='append'
            )
        print(f"[INFO] Data Analysis Process, Successful.")
    except Exception as e:
        print("[ERROR] Unexpected Error Occurred When Analytics: ", e)

with DAG(
    dag_id="elt_transaction_pipeline_non_taskflow_api",
    start_date=datetime(2025, 8, 30),
    schedule="@daily",
    catchup=False,
    tags=["etl", "transaction"]
) as dag:
    task_extract = PythonOperator(
        task_id="extract_task",
        python_callable=extract_data
    )

    task_transform = PythonOperator(
        task_id="transform_task",
        python_callable=transform_data
    )

    task_logic = PythonOperator(
        task_id="logic_business_task",
        python_callable=logic_business
    )

    task_extract >> task_transform >> task_logic
