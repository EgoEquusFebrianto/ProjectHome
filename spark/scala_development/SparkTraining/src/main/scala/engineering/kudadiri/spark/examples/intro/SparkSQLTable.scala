package engineering.kudadiri.spark.examples.intro

import org.apache.spark.sql.SparkSession
import org.apache.logging.log4j.{LogManager, Logger}

import scala.io.StdIn

object SparkSQLTable {
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
      .config("spark.sql.catalogImplementation", "hive")
      .enableHiveSupport()
      .getOrCreate()

    log.info("Spark Job Start...")

//    val flight_df = spark
//      .read
//      .format("parquet")
//      .load(args(0))

//    flight_df.show(5)
//    flight_df.printSchema()

    spark.sql("create database if not exists airline_db")
    spark.catalog.setCurrentDatabase("airline_db")

//    flight_df.write
//      .mode("overwrite")
//      .bucketBy(5, "OP_CARRIER", "ORIGIN")
//      .sortBy("OP_CARRIER", "ORIGIN")
//      .saveAsTable("flight_data_tbl")

    log.info(spark.catalog.listTables("airline_db"))

    val df = spark.sql("SELECT * FROM flight_data_tbl")
    df.show()

    log.info("Spark Job End...")
    println("Press Enter To Continuous...")
    StdIn.readLine()
    spark.stop()
    log.info("Spark Terminate...")
  }
}
