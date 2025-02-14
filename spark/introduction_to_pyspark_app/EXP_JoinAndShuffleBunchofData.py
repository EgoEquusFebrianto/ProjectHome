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
    file_log = "-Dlogfile.name=JoinAndShuffle-app"

    spark = SparkSession.builder \
        .appName("Spark App") \
        .master("local[3]") \
        .config("spark.driver.extraJavaOptions", f"{location} {folder_log} {file_log}") \
        .getOrCreate()

    logger = Log4j(spark)
    logger.info("Spark App is Running...")

    df_1 =  spark.read.format("json") \
        .option("inferSchema", True) \
        .load("data/spark/d/d1/")

    df_2 = spark.read.format("json") \
        .option("inferSchema", True) \
        .load("data/spark/d/d2/")

    # df_1.printSchema()
    # df_2.printSchema()

    spark.conf.set("spark.sql.shuffle.partitions", 3)

    join_expr = df_1["id"] == df_2["id"]
    join_df = df_1.join(df_2, join_expr, "inner")

    join_df.collect()
    input("press a key to stop...")
    logger.info("Spark Terminate")
    spark.stop()