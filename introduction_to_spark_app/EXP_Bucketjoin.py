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
        .enableHiveSupport() \
        .getOrCreate()

    logger = Log4j(spark)
    logger.info("Spark App is Running...")

    '''
    df_1 =  spark.read.format("json") \
        .option("inferSchema", True) \
        .load("/mnt/d/data/spark/d/d1/")

    df_2 = spark.read.format("json") \
        .option("inferSchema", True) \
        .load("/mnt/d/data/spark/d/d2/")

    # df_1.printSchema()
    # df_2.printSchema()

    spark.sql("create database if not exists projectDB")
    spark.sql("use projectDB")
    
    df_1.coalesce(1).write \
        .bucketBy(3, "id") \
        .mode("overwrite") \
        .saveAsTable("projectDB.flight_data_1")

    df_2.coalesce(1).write \
        .bucketBy(3, "id") \
        .mode("overwrite") \
        .saveAsTable("projectDB.flight_data_2")
    '''

    df_3 = spark.read.table("projectDB.flight_data_1")
    df_4 = spark.read.table("projectDB.flight_data_2")

    _on = df_3["id"] == df_4["id"]
    flight_data = df_3.join(df_4, _on, "inner")

    flight_data.show(5)
    logger.info(flight_data.schema.simpleString())

    logger.info("Spark Terminate")
    spark.stop()