package Session1_SparkData.intro

import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.io.StdIn

case class Order(order_id: String, product_id: String, unit_price: Int, quantity: Int, order_date: String)

object SparkDatasetIntroduction {
  @transient private lazy val log:Logger = LoggerFactory.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    System.setProperty("logfile.name", "spark-app")

    log.info("Initialize SparkSession..")

    val spark = SparkSession.builder()
      .appName("Spark App")
      .master("local[3]")
      .getOrCreate()

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

    val order_df: Dataset[Order] = spark.createDataset(orders_list)(Encoders.product[Order])

    order_df.printSchema()
    println()
    order_df.show()

    log.info("Spark Job End..")
    StdIn.readLine()
    spark.stop()
    log.info("Spark Terminate..")
  }
}