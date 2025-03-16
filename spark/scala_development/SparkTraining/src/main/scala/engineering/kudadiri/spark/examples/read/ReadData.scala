package engineering.kudadiri.spark.examples.read

import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.spark.sql.SparkSession

import scala.io.StdIn

object ReadData {
  @transient lazy val log: Logger = LogManager.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    System.setProperty("logfile.name", "spark-app-1")

    if (args.length < 1) {
      log.info("Need Apply Some Data Path: <>")
      sys.exit()
    }

    log.info("Spark Initialize...")

    val spark = SparkSession.builder()
      .appName("Spark App")
      .master("local[3]")
      .getOrCreate()

    log.info("Spark Job Start...")

    val df = spark.read
      .format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(args(0))

    df.show(5)

    log.info("Spark Job End...")
    print("Press Enter To Continuous...")
    StdIn.readLine()
    spark.stop()
    log.info("Spark Terminate...")
  }
}
