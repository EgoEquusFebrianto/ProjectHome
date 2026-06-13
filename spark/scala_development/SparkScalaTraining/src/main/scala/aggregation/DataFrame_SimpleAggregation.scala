package aggregation

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.slf4j.{Logger, LoggerFactory}

import scala.io.StdIn

object DataFrame_SimpleAggregation {
  @transient private lazy val log: Logger = LoggerFactory.getLogger(getClass.getName)

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
      .option("inferSchema", "true")
      .load(file)

    df.printSchema()

    // Column Object Expression

    val res1 = df.select(
      count("*").alias("Count"),
      sum("Quantity").alias("Sum Quantity"),
      avg("UnitPrice").alias("Average UnitPrice"),
      countDistinct("InvoiceNo").alias("Count Distinct Invoice Number")
    )

    // SQL/String Expression

    val res2 = df.selectExpr(
      "count(1) as `Count 1`",
      "count(StockCode) as `Count StockCode`",
      "sum(Quantity) as `Sum Quantity`",
      "avg(UnitPrice) as `Average UnitPrice`"
    )

    res2.show()

    log.info("Spark Job End..")
    StdIn.readLine()
    spark.stop()
    log.info("Spark Terminate..")
  }
}
