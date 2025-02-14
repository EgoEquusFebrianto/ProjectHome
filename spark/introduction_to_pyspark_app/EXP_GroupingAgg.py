from pyspark.sql import SparkSession
from lib.logging import Log4j
from pyspark.sql.types import *
import pyspark.sql.functions as f
import os

os.environ["SPARK_LOCAL_IP"] = "IP"

if __name__ == "__main__":
    # konfigurasi ini saya sarankan dibuat di file spark-defaults.conf
    location = "-Dlog4j.configurationFile=file:/path/to/your/log4j<1 or 2>.properties"
    folder_log = "-Dspark.yarn.app.container.log.dir=/path/to/your/app-logs"
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
        .load("data/spark/invoices.csv")

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
        .option("path", "DataSinkResult/parquet/GroupingAgg/") \
        .save()

    logger.info("Spark Terminate")
    spark.stop()