import org.apache.spark.sql.SparkSession

object SparkTesting {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SparkApp")
      .master("local[3]")
      .getOrCreate()

    val dir = "D:/Data_Engineer/Portofollio/End_To_End_Streaming_Processing_System_Project_V1/KafkaSide/Dataset/skenario2.csv"

    val df = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(dir)

    df.printSchema()
    spark.stop()
  }
}
