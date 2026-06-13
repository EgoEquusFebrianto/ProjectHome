package spark_data.sparkDataReader

import org.apache.spark.sql.{Encoders, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.io.StdIn

case class FlightDataCSV(
                          FL_DATE: String,
                          OP_CARRIER: String,
                          OP_CARRIER_FL_NUM: Option[Int],
                          ORIGIN: String,
                          ORIGIN_CITY_NAME: String,
                          DEST: String,
                          DEST_CITY_NAME: String,
                          CRS_DEP_TIME: Option[Int],
                          DEP_TIME: Option[Int],
                          WHEELS_ON: Option[Int],
                          TAXI_IN: Option[Int],
                          CRS_ARR_TIME: Option[Int],
                          ARR_TIME: Option[Int],
                          CANCELLED: Option[Int],
                          DISTANCE: Option[Int]
                        )

object SparkReader_Dataset {
  @transient private lazy val log: Logger = LoggerFactory.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    System.setProperty("logfile.name", "spark-app")

    log.info("Initialize SparkSession..")
    val spark = SparkSession.builder()
      .appName("Spark App")
      .master("local[3]")
      .getOrCreate()

    log.info("Spark Job Started..")
    val dir: String = "D:/data/spark/flight-time.csv"

    val df = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(dir)
      .as(Encoders.product[FlightDataCSV])

    df.printSchema()

    log.info("Spark Job End..")
    StdIn.readLine()
    spark.stop()
    log.info("Spark Terminate..")

  }
}
