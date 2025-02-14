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
    file_log = "-Dlogfile.name=ReadingFileLog-app"

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

    ## Simple Aggregation

    # # Column Object Expression
    # df.select(
    #     f.count("*").alias("Count *"),
    #     f.sum("Quantity").alias("Sum Quantity"),
    #     f.avg("UnitPrice").alias("Average UnitPrice"),
    #     f.countDistinct("InvoiceNo").alias("Count Distinct Invoice Number")
    # ).show()
    #
    # # SQL/String Expression
    # df.selectExpr(
    #     "count(1) as `count 1`",
    #     "count(StockCode) as `count field`",3
    #     "avg(UnitPrice) as AverageUnitPrice"
    # ).show()

    ## Group Aggregation

    # # SQL/String Expression
    # df.createOrReplaceTempView("sales")
    # summary = spark.sql("""
    #     Select Country, InvoiceNo,
    #         sum(Quantity) as TotalQuantity,
    #         round(sum(Quantity * UnitPrice),2) as InvoiceValue
    #     from sales
    #     group by Country, InvoiceNo
    # """)
    # summary.show()

    # Column Object Expression
    summary = df \
        .groupby("Country", "InvoiceNo") \
        .agg(
            f.sum("Quantity").alias("TotalQuantity"),
            f.round(f.sum(f.expr("Quantity * UnitPrice")), 2).alias("InvoiceValue")
        )
    summary.show()

    logger.info("Spark Terminate")
    spark.stop()