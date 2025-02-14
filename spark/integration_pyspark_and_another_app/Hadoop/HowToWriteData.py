from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as f
from lib.logging import Log4j

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
    spark = SparkSession.builder \
        .getOrCreate()
        # config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") # gunakan integrasi ini untuk menghubungkan ke hdfs hadoop

    logger = Log4j(spark)
    logger.info("Spark App is Running...")

    order_schema = StructType([
        StructField("order_id", StringType()),
        StructField("prod_id", StringType()),
        StructField("unit_price", IntegerType()),
        StructField("qty", IntegerType())
    ])

    product_schema = StructType([
        StructField("prod_id", StringType()),
        StructField("prod_name", StringType()),
        StructField("list_price", IntegerType()),
        StructField("qty", IntegerType())
    ])

    df1_rdd = spark.sparkContext.parallelize(orders_list, 2)
    df2_rdd = spark.sparkContext.parallelize(product_list, 2)

    order_data = spark.createDataFrame(df1_rdd, order_schema)
    product_data = spark.createDataFrame(df2_rdd, product_schema)

    # order_data.printSchema()
    # product_data.printSchema()

    _on = order_data["prod_id"] == product_data["prod_id"]
    product_data_new = product_data.withColumnRenamed("qty", "product_quantity")
    summary = order_data.join(product_data_new, _on, 'left') \
        .drop(product_data_new["prod_id"]) \
        .withColumn("prod_name", f.expr("coalesce(prod_name, prod_id)")) \
        .withColumn("list_price", f.expr("coalesce(list_price, unit_price)"))

    try:
        hdfs_parquet = "hdfs://localhost:9000/kudadiri/transaction/output/summary.parquet"
        summary.write \
            .mode("overwrite") \
            .parquet(hdfs_parquet)
        logger.info("Data sent successfully")
    except Exception as e:
        logger.error(e)

    logger.info("Spark Terminate")
    spark.stop()