package engineering.kudadiri.spark.examples.intro

import org.apache.spark.sql.{SparkSession, Encoders}
import org.apache.logging.log4j.{LogManager, Logger}

import java.sql.Date
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

case class FlightDataJSON(
                         ARR_TIME: Option[Long],
                         CANCELLED: Option[Long],
                         CRS_ARR_TIME: Option[Long],
                         CRS_DEP_TIME: Option[Long],
                         DEP_TIME: Option[Long],
                         DEST: String,
                         DEST_CITY_NAME: String,
                         DISTANCE: Option[Long],
                         FL_DATE: String,
                         OP_CARRIER: String,
                         OP_CARRIER_FL_NUM: Option[Long],
                         ORIGIN: String,
                         ORIGIN_CITY_NAME: String,
                         TAXI_IN: Option[Long],
                         WHEELS_ON: Option[Long]
                       )

case class FlightDataPARQUET(
                         FL_DATE: Option[Date],
                         OP_CARRIER: Option[String],
                         OP_CARRIER_FL_NUM: Option[Int],
                         ORIGIN: Option[String],
                         ORIGIN_CITY_NAME: Option[String],
                         DEST: Option[String],
                         DEST_CITY_NAME: Option[String],
                         CRS_DEP_TIME: Option[Int],
                         DEP_TIME: Option[Int],
                         WHEELS_ON: Option[Int],
                         TAXI_IN: Option[Int],
                         CRS_ARR_TIME: Option[Int],
                         ARR_TIME: Option[Int],
                         CANCELLED: Option[Int],
                         DISTANCE: Option[Int]
                       )

object SparkDatasetIntro2{
  @transient lazy val log:Logger = LogManager.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    System.setProperty("logfile.name", "spark-app-3")

    if (args.length < 1) {
      log.info("Need Some Data Path: <>")
      sys.exit()
    }

    log.info("Initialize Spark...")
    val spark = SparkSession.builder()
      .appName("Spark App")
      .master("local[3]")
      .getOrCreate()

    log.info("Spark Job Start...")

//    val read_data1 = spark.read
//      .format("csv")
//      .option("inferSchema", "true")
//      .option("header", "true")
//      .load(args(0))
//      .as(Encoders.product[FlightDataCSV])

//    val read_data2 = spark.read
//      .format("json")
//      .option("inferSchema", "true")
//      .load(args(1))

    val read_data3 = spark.read
      .format("parquet")
      .load(args(2))

//    read_data1.printSchema()
//    read_data1.show(5)

//    read_data2.printSchema()
//    read_data2.show(5)

    read_data3.printSchema()
    read_data3.show(5)


    log.info("Spark Job End...")
    println("Press Enter To Continuous...")
    StdIn.readLine()
    spark.stop()
    log.info("Spark Terminate...")
  }
}