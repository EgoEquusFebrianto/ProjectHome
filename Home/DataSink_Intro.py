from pyspark.sql import SparkSession
from lib.logging import Log4j
from lib.utils import *
import os

os.environ['SPARK_LOCAL_IP'] = "IP"

if __name__ == "__main__":
    # konfigurasi ini saya sarankan dibuat di file spark-defaults.conf
    location = "-Dlog4j.configurationFile=file:/path/to/your/log4j<1 or 2>.properties"
    folder_log = "-Dspark.yarn.app.container.log.dir=/path/to/your/app-logs"
    file_log = "-Dlogfile.name=SQL-spark-app"

    spark = (SparkSession.builder
        .appName("DataSink Example")
        .master("local[3]")
        .config("spark.driver.extraJavaOptions", f"{location} {folder_log} {file_log}")
        .getOrCreate()
    )
    logger = Log4j(spark)

    logger.info("Spark Processing Activate...")



    logger.info("Spark Terminate")
    spark.stop()
