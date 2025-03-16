from pyspark.sql import SparkSession, Window
from lib.logging import Log4j
from pyspark.sql.types import *
import pyspark.sql.functions as f
import os

os.environ["SPARK_LOCAL_IP"] = "IP"

orders_list = [
    ("01", "02", 350, 1),
    ("01", "04", 580, 1),
    ("01", "07", 320, 2),
    ("02", "03", 450, 1),
    ("02", "06", 220, 1),
    ("03", "01", 195, 1),
    ("04", "09", 270, 3),
    ("04", "08", 410, 2),
    ("05", "02", 350, 1)
]

product_list = [
    ("01", "Scroll Mouse", 250, 20),
    ("02", "Optical Mouse", 350, 20),
    ("03", "Wireless Mouse", 450, 50),
    ("04", "Wireless Keyboard", 580, 50),
    ("05", "Standard Keyboard", 360, 10),
    ("06", "16 GB Flash Storage", 240, 100),
    ("07", "32 GB Flash Storage", 320, 50),
    ("08", "64 GB Flash Storage", 430, 25)
]

if __name__ == "__main__":
    # konfigurasi ini saya sarankan dibuat di file spark-defaults.conf
    location = "-Dlog4j.configurationFile=file:/path/to/your/log4j<1 or 2>.properties"
    folder_log = "-Dspark.yarn.app.container.log.dir=/path/to/your/app-logs"
    file_log = "-Dlogfile.name=SparkJoin-app"

    spark = SparkSession.builder \
        .appName("Spark App") \
        .master("local[3]") \
        .config("spark.driver.extraJavaOptions", f"{location} {folder_log} {file_log}") \
        .getOrCreate()

    logger = Log4j(spark)

    logger.info("Spark App is Running...")

    # # Cara 1
    # order_schema = StructType([
    #     StructField("order_id", StringType()),
    #     StructField("prod_id", StringType()),
    #     StructField("unit_price", IntegerType()),
    #     StructField("qty", IntegerType())
    # ])

    # product_schema = StructType([
    #     StructField("prod_id", StringType()),
    #     StructField("prod_name", StringType()),
    #     StructField("list_price", IntegerType()),
    #     StructField("qty", IntegerType())
    # ])

    # order_df_rdd = spark.sparkContext.parallelize(orders_list, 2)
    # product_df_rdd = spark.sparkContext.parallelize(product_list, 2)
    #
    # order_df = spark.createDataFrame(order_df_rdd, order_schema)
    # product_df = spark.createDataFrame(product_df_rdd, product_schema)

    # Cara 2
    order_df = spark.createDataFrame(orders_list).toDF("order_id", "prod_id", "unit_price", "qty")
    product_df = spark.createDataFrame(product_list).toDF("prod_id", "prod_name", "list_price", "qty")

    # order_df.printSchema()
    # product_df.printSchema()

    product_df_new = product_df.withColumnRenamed("qty", "reorder_qty")
    _on = order_df["prod_id"] == product_df["prod_id"]

    order_df.join(product_df_new, _on, "inner") \
        .drop(product_df_new["prod_id"]) \
        .select("order_id", "prod_id", "prod_name", "unit_price", "list_price", "qty") \
        .show()

    logger.info("Spark Terminate")
    spark.stop()