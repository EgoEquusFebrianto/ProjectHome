from pyspark.sql import SparkSession
from lib.logging import Log4j
from lib.utils import read_data
import os

os.environ['SPARK_LOCAL_IP'] = "IP"

if __name__ == "__main__":
    # konfigurasi ini saya sarankan dibuat di file spark-defaults.conf
    location = "-Dlog4j.configurationFile=file:/path/to/your/log4j<1 or 2>.properties"
    folder_log = "-Dspark.yarn.app.container.log.dir=/path/to/your/app-logs"
    file_log = "-Dlogfile.name=SparkSQLTable-app"

    spark = (
        SparkSession.builder
            .appName("Spark App")
            .master("local[3]")
            .config("spark.driver.extraJavaOptions", f"{location} {folder_log} {file_log}")
            .enableHiveSupport()
            .getOrCreate()
    )

    logger = Log4j(spark)

    logger.info("Application Spark is Running...")

    flight_data = read_data("parquet", spark, "data/flight-time.parquet")

    spark.sql("create database if not exists Airline_db")
    spark.catalog.setCurrentCatalog("Airline_db")

    flight_data.write \
        .mode("overwrite") \
        .bucketBy(5, "OP_CARRIER", "ORIGIN") \
        .sortBy("OP_CARRIER", "ORIGIN") \
        .saveAsTable("flight_data_tbl")

    logger.info(spark.catalog.listTables("Airline_db"))

    logger.info("Spark Terminate")
    spark.stop()