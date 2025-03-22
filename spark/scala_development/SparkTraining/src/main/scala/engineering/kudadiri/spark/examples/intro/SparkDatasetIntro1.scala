package engineering.kudadiri.spark.examples.intro

import org.apache.spark.sql.{SparkSession, Dataset, Encoders}
import org.apache.logging.log4j.{LogManager, Logger}

import scala.io.StdIn

case class Order(order_id: String, product_id: String, unit_price: Int, quantity: Int, order_date: String)
case class Product(product_id: String, product_name: String, price: Int, quantity: Int)

object SparkDatasetIntro1{
  @transient lazy val log:Logger = LogManager.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    System.setProperty("logfile.name", "spark-app-3")

    log.info("Initialize Spark...")
    val spark = SparkSession.builder()
      .appName("Spark App")
      .master("local[3]")
      .getOrCreate()

    log.info("Spark Job Start...")

    val orders_list = List(
      Order("01", "02", 350, 1, "2025-03-19"),
      Order("01", "04", 580, 1, "2025-03-10"),
      Order("01", "07", 320, 2, "2025-03-01"),
      Order("02", "03", 450, 1, "2025-03-09"),
      Order("02", "06", 220, 1, "2025-03-04"),
      Order("03", "01", 195, 1, "2025-03-05"),
      Order("04", "09", 270, 3, "2025-03-03"),
      Order("04", "08", 410, 2, "2025-03-02"),
      Order("05", "02", 350, 1, "2025-03-02")
    )

    val products_list = List(
      Product("01", "Scroll Mouse", 250, 20),
      Product("02", "Optical Mouse", 350, 20),
      Product("03", "Wireless Mouse", 450, 50),
      Product("04", "Wireless Keyboard", 580, 50),
      Product("05", "Standard Keyboard", 360, 10),
      Product("06", "16 GB Flash Storage", 240, 100),
      Product("07", "32 GB Flash Storage", 320, 50),
      Product("08", "64 GB Flash Storage", 430, 25)
    )

    val order_df: Dataset[Order] = spark.createDataset(orders_list)(Encoders.product[Order])
    val product_df: Dataset[Product] = spark.createDataset(products_list)(Encoders.product[Product])

    order_df.printSchema()
    product_df.printSchema()

    println()

    order_df.show(5)
    product_df.show(5)

    log.info("Spark Job End...")
    println("Press Enter To Continuous...")
    StdIn.readLine()
    spark.stop()
    log.info("Spark Terminate...")
  }
}