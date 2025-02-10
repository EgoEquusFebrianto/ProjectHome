from pyspark.sql import SparkSession
from lib.logging import Log4j
from lib.utils import *
import os

os.environ['SPARK_LOCAL_IP'] = "172.25.5.7"

if __name__ == "__main__":
    # konfigurasi ini saya sarankan dibuat di file spark-defaults.conf
    location = "-Dlog4j.configurationFile=file:/home/kudadiri/anaconda3/envs/Home/lib/python3.10/site-packages/pyspark/conf/log4j2.properties"
    folder_log = "-Dspark.yarn.app.container.log.dir=app-logs"
    file_log = "-Dlogfile.name=DataSink-spark-app"

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
