from pyspark.sql import *
from lib.logging import Log4j
from lib.utils import *
import os
import sys

os.environ['SPARK_LOCAL_IP'] = "IP"

if __name__ == "__main__":
    conf = get_spark_configuration()
    spark = (SparkSession.builder
        .config(conf=conf)
        .getOrCreate()
    )
    print(spark.sparkContext.uiWebUrl)
    # spark.sparkContext.setLogLevel("INFO")

    logger = Log4j(spark)
    conf_out = spark.sparkContext.getConf()
    # logger.info(conf_out.toDebugString())

    if len(sys.argv) < 1:
        logger.error("Usage: Home <filename>")
        sys.exit(-1)


    logger.info("Starting Spark Program")

    df = read_data(spark, sys.argv[1])
    #df_repartition = df.repartition(3)

    logger.info(df.collect())


    logger.info("Spark Program Terminate")
    input("Hello World! ")
    spark.stop()
