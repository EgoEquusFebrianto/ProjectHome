package spark_data.intro

import org.apache.spark.sql.{Row, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
import org.apache.spark.sql.types._

import scala.io.StdIn

object SparkDataFrameIntroduction {
  @transient private lazy val log:Logger = LoggerFactory.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    System.setProperty("logfile.name", "spark-app")

    log.info("Initialize SparkSession..")

    val spark = SparkSession.builder()
      .appName("Spark App")
      .master("local[3]")
      .getOrCreate()

    log.info("Spark Job Started..")

    // menggunakan schema buatan sendiri
    val data = Seq(
      Row(1, "Andi", 25, "Jakarta"),
      Row(2, "Budi", 30, "Bandung"),
      Row(3, "Citra", 28, "Surabaya")
    )

    val schema = StructType(List(
      StructField("id", IntegerType, nullable = false),
      StructField("nama", StringType, nullable = false),
      StructField("umur", IntegerType, nullable = false),
      StructField("kota", StringType, nullable = false),
    ))

    val rdd_df = spark.sparkContext.parallelize(data, 2)
    val df = spark.createDataFrame(rdd_df, schema)

    df.printSchema()
    df.show()

    // =================================================================
    // menggunakan toDf()
    // =================================================================

    import spark.implicits._

    val example = Seq(
      (1, "Andi", 25, "Jakarta"),
      (2, "Budi", 30, "Bandung"),
      (3, "Citra", 28, "Surabaya")
    )

    val dd = example.toDF("id", "nama", "umur", "kota")

    dd.printSchema()
    dd.show()

    log.info("Spark Job End..")
    StdIn.readLine()
    spark.stop()
    log.info("Spark Terminate..")
  }
}
