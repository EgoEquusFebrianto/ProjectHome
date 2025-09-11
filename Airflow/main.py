from sqlalchemy import create_engine
import pandas as pd

url = "https://raw.githubusercontent.com/EgoEquusFebrianto/data_resource/main/spark/transaction.csv"

user = "airflow"
password = "airflow_user_password"
host = "172.25.0.1"
db = "retail_transaction"
schema = "airflow_retail_dataset"

conn = f"postgresql+psycopg2://{user}:{password}@{host}:5432/{db}"
engine = create_engine(conn)

df = pd.read_csv(url, sep=";", index_col=False)

with engine.begin() as connection:
    df.to_sql(
        name="raw_retail_dataset",
        con=connection,
        schema=schema,
        index=False,
        if_exists="append"
    )