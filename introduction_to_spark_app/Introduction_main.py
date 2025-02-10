from pyspark.sql import *
from pyspark.sql.types import *
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
        logger.error("Usage: introduction_to_spark_app <filename>")
        sys.exit(-1)
    else:
        logger.info(f"data directory is -- {sys.argv}")

    logger.info("Starting Spark Program")

    # Spark Schema Method: Programmatically
    flightSchemaStruct = StructType([
        StructField("FL_DATE", DateType()),
        StructField("OP_CARRIER", StringType()),
        StructField("OP_CARRIER_FL_NUM", IntegerType()),
        StructField("ORIGIN", StringType()),
        StructField("ORIGIN_CITY_NAME", StringType()),
        StructField("DEST", StringType()),
        StructField("DEST_CITY_NAME", StringType()),
        StructField("CRS_DEP_TIME", IntegerType()),
        StructField("DEP_TIME", IntegerType()),
        StructField("WHEELS_ON", IntegerType()),
        StructField("TAXI_IN", IntegerType()),
        StructField("CRS_ARR_TIME", IntegerType()),
        StructField("ARR_TIME", IntegerType()),
        StructField("CANCELLED", IntegerType()),
        StructField("DISTANCE", IntegerType())
    ])

    # Spark Schema Method: DDL String
    flightSchemaDDL = """FL_DATE DATE, OP_CARRIER STRING, OP_CARRIER_FL_NUM INT, ORIGIN STRING, 
          ORIGIN_CITY_NAME STRING, DEST STRING, DEST_CITY_NAME STRING, CRS_DEP_TIME INT, DEP_TIME INT, 
          WHEELS_ON INT, TAXI_IN INT, CRS_ARR_TIME INT, ARR_TIME INT, CANCELLED INT, DISTANCE INT"""

    # df = read_data("csv", spark, sys.argv[1])
    # #df_repartition = df.repartition(3)
    # logger.info(df.collect())
    # res = count_by_country(df)
    # res.show(5)
    # logger.info(df.schema.simpleString())

    # # Implementasi menggunakan Programmatically
    # df = read_data("csv", spark, sys.argv[2], flightSchemaStruct)
    # df.show(5)
    # logger.info(df.schema.simpleString())

    # Implementasi menggunakan DDL String
    df = read_data("json", spark, sys.argv[3], flightSchemaDDL)
    df.show(5)
    logger.info(df.schema.simpleString())

    logger.info("Spark Program Terminate")
    # input("Hello World! ")
    spark.stop()
