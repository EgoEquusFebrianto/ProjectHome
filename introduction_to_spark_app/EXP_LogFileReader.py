from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import regexp_extract, substring_index

from lib.logging import Log4j
import os

os.environ['SPARK_LOCAL_IP'] = "172.25.5.7"

if __name__ == "__main__":
    # konfigurasi ini saya sarankan dibuat di file spark-defaults.conf
    location = "-Dlog4j.configurationFile=file:/home/kudadiri/anaconda3/envs/Home/lib/python3.10/site-packages/pyspark/conf/log4j2v2.properties"
    folder_log = "-Dspark.yarn.app.container.log.dir=app-logs"
    file_log = "-Dlogfile.name=ReadingFileLog-app"

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

    file_df = spark.read.text("/mnt/d/data/apache_logs.txt")
    # file_df.printSchema()

    # Lihat Materi
    log_reg = r'^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+) "(\S+)" "([^"]*)'

    # regexp_extract(), mengekstrak substring dari suatu string berdasarkan pola regex tertentu.
    df = file_df.select(
        regexp_extract("value", log_reg, 1).alias("ip"),
        regexp_extract("value", log_reg, 2).alias("client"),
        regexp_extract("value", log_reg, 3).alias("user"),
        regexp_extract("value", log_reg, 4).alias("datetime"),
        regexp_extract("value", log_reg, 5).alias("command"),
        regexp_extract("value", log_reg, 6).alias("request"),
        regexp_extract("value", log_reg, 7).alias("protocol"),
        regexp_extract("value", log_reg, 8).alias("status"),
        regexp_extract("value", log_reg, 9).alias("bytes"),
        regexp_extract("value", log_reg, 10).alias("referrer"),
        regexp_extract("value", log_reg, 11).alias("User_Agent"),
    )

    df.printSchema()

    # substring_index(col, delimiter, count) mengambil bagian awal string berdasarkan pembatas
    df \
        .where("trim(referrer) != '-'") \
        .withColumn("referrer", substring_index("referrer", "/", 3)) \
        .groupby("referrer") \
        .count() \
        .show(100, truncate=False)


    logger.info("Spark Terminate")
    spark.stop()