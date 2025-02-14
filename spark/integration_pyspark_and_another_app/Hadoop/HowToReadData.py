from pyspark.sql import SparkSession
from lib.logging import Log4j

if __name__ == "__main__":
    spark = SparkSession.builder \
        .getOrCreate()
        # config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") # gunakan integrasi ini untuk menghubungkan ke hdfs hadoop

    logger = Log4j(spark)
    logger.info("Spark App is Running...")

    df = spark.read \
        .format("parquet") \
        .load("hdfs://localhost:9000/kudadiri/transaction/output/summary.parquet")

    df.show()
    df.printSchema()

    logger.info("Spark Terminate")
    spark.stop()