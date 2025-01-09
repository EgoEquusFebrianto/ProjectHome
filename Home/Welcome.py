from pyspark.sql import *
from lib.logging import Log4j
from lib.utils import get_spark_configuration
import os

os.environ['SPARK_LOCAL_IP'] = "10.255.255.254"

if __name__ == "__main__":
    conf = get_spark_configuration()

    spark = (SparkSession.builder
        .config(conf=conf)
        .getOrCreate()
    )
    print(spark.sparkContext.uiWebUrl)

    spark.sparkContext.setLogLevel("INFO")
    logger = Log4j(spark)
    conf_out = spark.sparkContext.getConf()

    logger.info("Starting Spark Program")
    # logger.info(conf_out.toDebugString())
    print("Hello World")
    logger.info("Spark Program Terminate")

    #spark.stop()