package engineering.kudadiri.spark.examples

import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.logging.log4j.{LogManager, Logger}

import scala.io.StdIn

object L0_IntroductionToSpark {
  @transient lazy val log: Logger = LogManager.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    System.setProperty("logfile.name", "spark-app")
    log.info("Initialize Spark...")

    val spark = SparkSession.builder()
      .appName("Spark App")
      .master("local[3]")
      .getOrCreate()

    log.info("Application Spark Start...")

    val orders_list = Seq(
      ("01", "02", 350, 1, "2025-03-19"),
      ("01", "04", 580, 1, "2025-03-10"),
      ("01", "07", 320, 2, "2025-03-01"),
      ("02", "03", 450, 1, "2025-03-09"),
      ("02", "06", 220, 1, "2025-03-04"),
      ("03", "01", 195, 1, "2025-03-05"),
      ("04", "09", 270, 3, "2025-03-03"),
      ("04", "08", 410, 2, "2025-03-02"),
      ("05", "02", 350, 1, "2025-03-02")
    )

    val products_list = Seq(
      ("01", "Scroll Mouse", 250, 20),
      ("02", "Optical Mouse", 350, 20),
      ("03", "Wireless Mouse", 450, 50),
      ("04", "Wireless Keyboard", 580, 50),
      ("05", "Standard Keyboard", 360, 10),
      ("06", "16 GB Flash Storage", 240, 100),
      ("07", "32 GB Flash Storage", 320, 50),
      ("08", "64 GB Flash Storage", 430, 25)
    )

    val ordersSchema = StructType(Seq(
      StructField("order_id", StringType, nullable = false),
      StructField("product_id", StringType, nullable = false),
      StructField("unit_price", IntegerType, nullable = false),
      StructField("quantity", IntegerType, nullable = false),
      StructField("order_date", StringType, nullable = false),
    ))

    val productsSchema = StructType(Seq(
      StructField("product_id", StringType, nullable = false),
      StructField("product_name", StringType, nullable = false),
      StructField("price", IntegerType, nullable = false),
      StructField("quantity", IntegerType, nullable = false),
    ))

    val orders_rdd = spark.sparkContext.parallelize(orders_list, 2)
      .map{case (order_id, product_id, unit_price, quantity, order_date) =>
        Row(order_id, product_id, unit_price, quantity, order_date)
      }

    val products_rdd = spark.sparkContext.parallelize(products_list, 2)
      .map{case (product_id, product_name, price, quantity) =>
        Row(product_id, product_name, price, quantity)
      }

    val orders_df_temp = spark.createDataFrame(orders_rdd, ordersSchema)
    val products_df = spark.createDataFrame(products_rdd, productsSchema)

    val orders_df = orders_df_temp.withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))

    orders_df.show()
    products_df.show()

    log.info("Application Spark End...")

    print("Tekan Enter Untuk Melanjut ")
    StdIn.readLine()

    spark.stop()
    log.info("Spark Terminate...")
  }
}
