from pyspark.sql import SparkSession, Window
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
        .format("parquet") \
        .load("data/spark/summary.parquet")

    # df.printSchema()

    running_total_window = Window.partitionBy("Country") \
        .orderBy("WeekNumber") \
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    df.withColumn("RunningTotal", f.sum("InvoiceValue").over(running_total_window)).show()

    logger.info("Spark Terminate")
    spark.stop()