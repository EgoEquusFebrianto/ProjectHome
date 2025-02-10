from pyspark.sql import SparkSession, DataFrame
from lib.logging import Log4j
from pyspark.sql.functions import to_date, date_format
from pyspark.sql.types import StructType, StructField, Row, StringType
import os

os.environ['SPARK_LOCAL_IP'] = "IP"

def convert_date(data: DataFrame, format, field):
    # return data.withColumn(field, to_date(field, format))
    return data.withColumn(field, date_format(to_date(field, format), "dd-MM-yyyy"))

if __name__ == "__main__":
    # konfigurasi ini saya sarankan dibuat di file spark-defaults.conf
    location = "-Dlog4j.configurationFile=file:/path/to/your/log4j<1 or 2>.properties"
    folder_log = "-Dspark.yarn.app.container.log.dir=/path/to/your/app-logs"
    file_log = "-Dlogfile.name=SparkRows-app"

    spark = (
        SparkSession.builder
            .appName("Spark App")
            .master("local[3]")
            .config("spark.driver.extraJavaOptions", f"{location} {folder_log} {file_log}")
            .getOrCreate()
    )

    logger = Log4j(spark)
    logger.info("Application Spark is Running...")

    # logger.info(spark.sparkContext._conf.getAll())

    _schema = StructType([
        StructField("ID", StringType()),
        StructField("Event_Date", StringType())
    ])
    example = [Row("1", "10/2/2025"), Row("2", "11/2/2025"), Row("3", "12/2/2025"), Row("4", "13/2/2025"), Row("5", "14/2/2025")]

    df_rdd = spark.sparkContext.parallelize(example, 2)
    df = spark.createDataFrame(df_rdd, _schema)

    logger.info("Before Transform")
    df.show()

    logger.info("After Transform")
    new_df = convert_date(df, "d/M/y", "Event_Date")
    new_df.show()

    logger.info("Spark Terminate")
    spark.stop()