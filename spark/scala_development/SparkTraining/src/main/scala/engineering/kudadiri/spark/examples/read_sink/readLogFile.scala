package engineering.kudadiri.spark.examples.read_sink

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.logging.log4j.{LogManager, Logger}

import scala.io.StdIn

object readLogFile {
  @transient lazy val log:Logger = LogManager.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    System.setProperty("logfile.name", "spark-app-2")
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

    val logRegex = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})] "(\S+) (\S+) (\S+)" (\d{3}) (\S+) "(\S+)" "([^"]*)"""".r
    val logRDD = spark.sparkContext.textFile(args(0))

    val parsedRDD = logRDD.flatMap{
      case logRegex(ip, client, user, datetime, command, request, protocol, status, bytes, referrer, userAgent) =>
        Some((ip, client, user, datetime, command, request, protocol, status, bytes, referrer, userAgent))
      case _ => None
    }

    val logDf = spark.createDataFrame(parsedRDD)
      .toDF("ip", "client", "user", "datetime", "command", "request", "protocol", "status", "bytes", "referrer", "userAgent")

    val seek = logDf
      .where("trim(referrer) != '-'")
      .withColumn("referrer", substring_index(col("referrer"), "/", 3))
      .groupBy("referrer")
      .count()

    seek.show()

    log.info("Spark Job End...")
    println("Press Enter To Continuous...")
    StdIn.readLine()
    spark.stop()
    log.info("Spark Terminate...")
  }
}
