from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from faker import Faker
import pandas as pd
import os

os.environ["SPARK_LOCAL_IP"] = "172.25.5.7:"
os.environ["PYSPARK_PYTHON"] = "/home/kudadiri/anaconda3/envs/Home/bin/python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/home/kudadiri/anaconda3/envs/Home/bin/python"

# Inisialisasi Spark
spark = SparkSession.builder \
    .appName("SimplifiedDataPipeline") \
    .config("spark.executor.cores", 1)  \
    .config("spark.executor.instances", 1)  \
    .config("spark.driver.memory", "512m") \
    .config("spark.executor.memory", "1g")  \
    .config("spark.executor.memoryOverhead", "256m") \
    .config("spark.sql.shuffle.partitions", 10)  \
    .getOrCreate()
fake = Faker()

# Buat dataset sintetis dalam satu fungsi
def generate_data(n=1000):
    return pd.DataFrame([{
        "account_id": i,
        "name": fake.name(),
        "city": fake.city(),
        "country": fake.country(),
        "related_account": i-1 if i % 2 == 0 else i+1
    } for i in range(1, n+1)])

# Konversi ke DataFrame Spark
df = spark.createDataFrame(generate_data())

# Transformasi Data
cleaned_df = df.dropDuplicates(["account_id"]).filter(col("city").isNotNull())

# Simpan hasil
cleaned_df.write.mode("overwrite").parquet("output_data.parquet")

spark.stop()