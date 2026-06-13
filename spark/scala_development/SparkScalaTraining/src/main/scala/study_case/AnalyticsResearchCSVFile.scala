package study_case

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import java.text.NumberFormat
import java.util.Locale
import scala.io.StdIn

object AnalyticsResearchCSVFile {
  def printSimple(data: DataFrame) = {
    val left = data.columns.take(3)
    val right = data.columns.takeRight(4)
    val concat =data.select(left.map(col) :+ lit(",,").alias("..") :++ right.map(col): _*)

    concat.show(5)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark App")
      .master("local[3]")
      .getOrCreate()

    val dir = "D:/Data_Engineer/Portofollio/End_To_End_Streaming_Processing_System_Project_V1/System_Application/Dataset/pengambilan_data_clean.csv"

    val df = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(dir)

    printSimple(df)
//    val rowCount = df.count()
//    val formatter = NumberFormat.getInstance(new Locale("id", "ID"))
//    val rowCountFormatted = formatter.format(rowCount)
//    val colCount = df.schema.length
//    println(s"\n==========\nDataFrame memiliki $rowCountFormatted baris dan $colCount kolom")

    spark.stop()
  }
}
