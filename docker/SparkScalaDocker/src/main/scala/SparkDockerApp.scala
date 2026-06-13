import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import utils.ConfigUtils

object SparkDockerApp {

  def main(args: Array[String]): Unit = {
    val sparkConf = ConfigUtils.getSparkConfig("local")
    val read = ConfigUtils.getBatchConfig("read")
    val sink = ConfigUtils.getBatchConfig("sink")

    val spark = SparkSession.builder()
      .config(conf = sparkConf)
      .getOrCreate()

    val df = spark.read
      .format(read("format"))
      .option("header", "true")
      .option("inferSchema", "true")
      .load(read("dir"))

    val df_final = df.withColumn(
      "total",
      col("harga") + col("jumlah")
    )

    df_final.printSchema()

    df_final.write
      .format(sink("format"))
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save(sink("target"))

    spark.close()
  }
}