package aggregation

import org.apache.spark.sql.{Encoders, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.io.StdIn

case class Invoice(
                  InvoiceNo: String,
                  StockCode: String,
                  Description: String,
                  Quantity: Int,
                  InvoiceDate: String,
                  UnitPrice: Double,
                  CustomerID: Int,
                  Country: String
                  )

object Dataset_SimpleAggregation {
  @transient private lazy val log:Logger = LoggerFactory.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    System.setProperty("logfile.name", "spark-app")
    val file = "D:/data/spark/invoices.csv"

    log.info("Initialize SparkSession..")

    val spark = SparkSession.builder()
      .appName("Spark App")
      .master("local[3]")
      .getOrCreate()

    log.info("Spark Job Started..")

    val df = spark.read
      .format("csv")
      .option("header", "true")
      .load(file)
      .as(Encoders.product[Invoice])

    df.printSchema()

    log.info("Spark Job End..")
    StdIn.readLine()
    spark.stop()
    log.info("Spark Terminate..")

  }
}
