package Session1_SparkData.sparkDataReader

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.slf4j.{Logger, LoggerFactory}

import scala.io.StdIn

object SparkReader_DataFrame {
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

    val flightSchemaStruct = StructType(Array(
      StructField("FL_DATE", StringType, nullable = true),
      StructField("OP_CARRIER", StringType, nullable = true),
      StructField("OP_CARRIER_FL_NUM", IntegerType, nullable = true),
      StructField("ORIGIN", StringType, nullable = true),
      StructField("ORIGIN_CITY_NAME", StringType, nullable = true),
      StructField("DEST", StringType, nullable = true),
      StructField("DEST_CITY_NAME", StringType, nullable = true),
      StructField("CRS_DEP_TIME", IntegerType, nullable = true),
      StructField("DEP_TIME", IntegerType, nullable = true),
      StructField("WHEELS_ON", IntegerType, nullable = true),
      StructField("TAXI_IN", IntegerType, nullable = true),
      StructField("CRS_ARR_TIME", IntegerType, nullable = true),
      StructField("ARR_TIME", IntegerType, nullable = true),
      StructField("CANCELLED", IntegerType, nullable = true),
      StructField("DISTANCE", IntegerType, nullable = true)
    ))

    // Dengan inferSchema
//    val df = spark.read
//      .format("csv")
//      .option("header", "true")
//      .option("inferSchema", "true")
//      .load(dir)

    // tanpa infherSchema
    val df = spark.read
      .format("csv")
      .schema(flightSchemaStruct)
      .option("header", "true")
      .load(dir)

    df.printSchema()
    df.show()

    log.info("Spark Job End..")
    StdIn.readLine()
    spark.stop()
    log.info("Spark Terminate..")

  }
}
