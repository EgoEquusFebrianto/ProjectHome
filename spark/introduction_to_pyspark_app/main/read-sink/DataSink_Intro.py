from pyspark.sql import SparkSession
from pyspark.sql.functions import spark_partition_id

from lib.logging import Log4j
from lib.utils import read_data
from lib.utils import *
import os

os.environ['SPARK_LOCAL_IP'] = "IP"

if __name__ == "__main__":
    # konfigurasi ini saya sarankan dibuat di file spark-defaults.conf
    location = "-Dlog4j.configurationFile=file:/path/to/your/log4j<1 or 2>.properties"
    folder_log = "-Dspark.yarn.app.container.log.dir=/path/to/your/app-logs"
    file_log = "-Dlogfile.name=DataSink-spark-app"

    spark = (SparkSession.builder
        .appName("DataSink Example")
        .master("local[3]")
        .config("spark.driver.extraJavaOptions", f"{location} {folder_log} {file_log}")
        # bagian ini untuk menambahkan paket jar Avro, karena kita akan menyimpan data dalam format Avro
        # .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.5.3")
        .getOrCreate()
    )
    logger = Log4j(spark)

    logger.info("Spark Processing Activate...")

    flight_data = read_data("parquet", spark, "data/flight-time.parquet")

    flight_data.show(15)
    # logger.info(flight_data.schema.simpleString())
    # logger.info(f"The number of Partitions is {flight_data.rdd.getNumPartitions()}")
    # flight_data.groupby(spark_partition_id()).count().show()

    # repartition_flight_data = flight_data.repartition(5)
    # repartition_flight_data.write \
    #     .format("avro") \
    #     .mode("overwrite") \
    #     .option("path", "DataSinkResult/avro/") \
    #     .save()

    flight_data.write \
        .format("json") \
        .mode("overwrite") \
        .option("path", "DataSinkResult/json/") \
        .option("maxRecordsPerFile", 10000) \
        .partitionBy("OP_CARRIER", "ORIGIN") \
        .save()

    logger.info("Spark Terminate")
    spark.stop()
