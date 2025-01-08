from pyspark.sql import *
from lib.logging import Log4j
import os

os.environ['SPARK_LOCAL_IP'] = "x.x.x.x"

if __name__ == "__main__":
    location = "-Dlog4j.configurationFile=file:/path-to-log4j-file/log4j.properties"
    folder_log = "-Dspark.yarn.app.container.log.dir=app-logs"
    file_log = "-Dlogfile.name=spark-app"

    spark = (SparkSession.builder
        .appName("Spark Example")
        .config("spark.driver.extraJavaOptions", f"{location} {folder_log} {file_log}")
        .master("local[2]")
        .getOrCreate()
    )
    print(spark.sparkContext.uiWebUrl)
    spark.sparkContext.setLogLevel("INFO")
    logger = Log4j(spark)

    logger.info("Starting Spark Program")
    print("Hello World!")
    logger.info("Spark Program Terminate")

    #spark.stop()
