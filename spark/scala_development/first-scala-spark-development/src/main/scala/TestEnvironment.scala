import org.apache.spark.sql.SparkSession

import scala.io.StdIn

object TestEnvironment {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("TestSparkConnection")
      .master("local[*]") // Gunakan semua core yang tersedia
      .getOrCreate()

    println("=== SPARK_HOME ===")
    println(sys.env.getOrElse("SPARK_HOME", "SPARK_HOME tidak terdeteksi!"))

    println("=== Spark Version ===")
    println(spark.version)

    println("=== Spark Config ===")
    println(spark.conf.getAll)

    // Tes operasi sederhana untuk memastikan Spark berjalan
    val df = spark.range(10)
    df.show()

    print("Tekan Enter Untuk Melanjutkan: ")
    StdIn.readLine()
    spark.stop()
  }
}