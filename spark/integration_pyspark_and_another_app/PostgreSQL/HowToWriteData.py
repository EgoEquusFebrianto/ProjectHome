from pyspark.sql import SparkSession
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
        .config("spark.jars", "/home/kudadiri/.ivy2/jars/postgresql-42.5.1.jar") \
        .getOrCreate()
        #.config("spark.jars.packages", "postgresql:42.5.1") \

    logger = Log4j(spark)
    logger.info("Spark App is Running...")

    order_data_rdd = spark.sparkContext.parallelize(orders_list, 2)
    product_data_rdd = spark.sparkContext.parallelize(product_list, 2)

    order_data = spark.createDataFrame(order_data_rdd).toDF("order_id", "prod_id", "unit_price", "qty")
    product_data = spark.createDataFrame(product_data_rdd).toDF("prod_id", "prod_name", "list_price", "qty")

    # order_data.printSchema()
    # product_data.printSchema()
    order_data.createOrReplaceTempView("order_tbl")
    product_data.createOrReplaceTempView("product_tbl")

    query = """
        select
            t1.order_id,
            t1.prod_id,
            t1.unit_price,
            t1.qty,
            coalesce(t2.prod_name, t1.prod_id),
            coalesce(t2.list_price, t1.unit_price),
            t2.qty as product_quantity
        from order_tbl t1
        left join product_tbl t2
        on t1.prod_id = t2.prod_id
    """

    summary = spark.sql(query)

    summary.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/database") \
        .option("dbtable", "schema") \
        .option("user", "username") \
        .option("password", "password") \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

    logger.info("Spark Terminate")
    spark.stop()