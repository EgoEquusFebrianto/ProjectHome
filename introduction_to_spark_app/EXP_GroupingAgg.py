from pyspark.sql import SparkSession
from lib.logging import Log4j
from pyspark.sql.types import *
import pyspark.sql.functions as f
import os

os.environ["SPARK_LOCAL_IP"] = "172.25.5.7"

if __name__ == "__main__":
    # konfigurasi ini saya sarankan dibuat di file spark-defaults.conf
    location = "-Dlog4j.configurationFile=file:/home/kudadiri/anaconda3/envs/Home/lib/python3.10/site-packages/pyspark/conf/log4j2v2.properties"
    folder_log = "-Dspark.yarn.app.container.log.dir=app-logs"
    file_log = "-Dlogfile.name=AggregationDataFrame-app"

    spark = SparkSession.builder \
        .appName("Spark App") \
        .master("local[3]") \
        .config("spark.driver.extraJavaOptions", f"{location} {folder_log} {file_log}") \
        .getOrCreate()

    logger = Log4j(spark)

    logger.info("Spark App is Running...")

    df = spark.read \
        .format("csv") \
        .option("header", True) \
        .option("inferSchema", True) \
        .load("/mnt/d/data/spark/invoices.csv")

    # df.printSchema()

    NumInvoice = f.countDistinct("InvoiceNo").alias("Count Distinct Invoice Number")
    TotalQuantity = f.sum("Quantity").alias("Total Quantity")
    InvoiceValue = f.round(f.sum(f.expr("Quantity * UnitPrice")), 2).alias("InvoiceValue")

    summary = df \
        .withColumn("InvoiceDate", f.to_date(f.col("InvoiceDate"), "d-M-y H.m")) \
        .where("year(InvoiceDate) == 2010") \
        .withColumn("WeekNumber", f.weekofyear(f.col("InvoiceDate"))) \
        .groupby("Country", "WeekNumber") \
        .agg(NumInvoice, TotalQuantity, InvoiceValue)

    summary.sort("Country", "WeekNumber").show(10)

    summary.coalesce(1) \
        .write \
        .format("parquet") \
        .mode("overwrite") \
        .option("path", "/mnt/d/DataSinkResult/parquet/GroupingAgg/") \
        .save()

    logger.info("Spark Terminate")
    spark.stop()