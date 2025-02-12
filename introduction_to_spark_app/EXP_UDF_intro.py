import re
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf, expr
from pyspark.sql.types import StringType

from lib.logging import Log4j
import os

os.environ['SPARK_LOCAL_IP'] = "IP"

def parser_sex(sex):
    male_pattern = r"^f$|f.m|w.m"
    female_pattern = r"^m$|ma|m.l"

    if re.search(male_pattern, sex.lower()):
        return "Male"
    elif re.search(female_pattern, sex.lower()):
        return "Female"
    else:
        return "Unknown"

if __name__ == "__main__":
    # konfigurasi ini saya sarankan dibuat di file spark-defaults.conf
    location = "-Dlog4j.configurationFile=file:/path/to/your/log4j<1 or 2>.properties"
    folder_log = "-Dspark.yarn.app.container.log.dir=/path/to/your/app-logs"
    file_log = "-Dlogfile.name=UDF-app"

    spark = (
        SparkSession.builder
            .appName("Spark App")
            .master("local[3]")
            .config("spark.driver.extraJavaOptions", f"{location} {folder_log} {file_log}")
            .getOrCreate()
    )

    logger = Log4j(spark)
    logger.info("Application Spark is Running...")

    df = spark.read \
        .option("inferSchema", True) \
        .option("header", True) \
        .csv("data/survey.csv")

    df.show(5)

    # UDF - Column Object Expression
    parser_sex_udf = udf(parser_sex, StringType())
    logger.info("COE Catalog Entry:")
    [logger.info(value) for value in spark.catalog.listFunctions() if "parser_sex" in value.name]
    COE_udf = df.withColumn("Gender", parser_sex_udf("Gender"))
    COE_udf.show(5)

    # UDF - SQL/String Expression
    spark.udf.register("parser_sex_udf", parser_sex, StringType())
    logger.info("SQL Catalog Entry:")
    [logger.info(value) for value in spark.catalog.listFunctions() if "parser_sex" in value.name]
    SQL_udf = df.withColumn("Gender", expr("parser_sex_udf(Gender)"))
    SQL_udf.show(5)

    logger.info("Spark Terminate")
    spark.stop()