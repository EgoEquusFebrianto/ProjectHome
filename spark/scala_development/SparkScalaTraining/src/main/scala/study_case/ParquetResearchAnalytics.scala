package study_case

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import scala.io.StdIn

object ParquetResearchAnalytics {
  @transient private lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  def readData(spark: SparkSession, dir: String): DataFrame = {
    val df = spark.read
      .format("parquet")
      .load(dir)

    df
  }

  def sink(df: DataFrame, target: String) = {
    df.coalesce(1).write
      .format("parquet")
      .option("path", target)
      .mode("overwrite")
      .save()
  }

  def main(args: Array[String]): Unit = {
    System.setProperty("logfile.name", "spark-app")
//    val path = "D:/Data_Engineer/Portofollio/End_To_End_Streaming_Processing_System_Project_1/System_Application/analytics"
    val path = "D:/Data_Engineer/Portofollio/End_To_End_Streaming_Processing_System_Project_1/History/"

    val dir1 = path + "analytics-U-R100/"
    val dir2 = path + "analytics-U-R200/"
    val dir3 = path + "analytics-U-R400/"

    val folder1 = dir1 + "analytics-U-R100-S1"
    val folder2 = dir1 + "analytics-U-R100-S2"
    val folder3 = dir1 + "analytics-U-R100-S3"

    val folder4 = dir2 + "analytics-U-R200-S3"

    val folder5 = dir3 + "analytics-U-R400-S1"
    val folder6 = dir3 + "analytics-U-R400-S2"
    val folder7 = dir3 + "analytics-U-R400-S3"

    val res = "/parquet/*.parquet"
    val matrics = "/metrics_parquet/*.parquet"

    val sink_res = "/result"
    val matrics_res = "/matrix-result"

    val data1_1 = folder1 + res
    val data1_2 = folder1 + matrics
    val target1_1 = folder1 + sink_res
    val target1_2 = folder1 + matrics_res

    val data2_1 = folder2 + res
    val data2_2 = folder2 + matrics
    val target2_1 = folder2 + sink_res
    val target2_2 = folder2 + matrics_res

    val data3_1 = folder3 + res
    val data3_2 = folder3 + matrics
    val target3_1 = folder3 + sink_res
    val target3_2 = folder3 + matrics_res

    val data4_1 = folder4 + res
    val data4_2 = folder4 + matrics
    val target4_1 = folder4 + sink_res
    val target4_2 = folder4 + matrics_res

    val data5_1 = folder5 + res
    val data5_2 = folder5 + matrics
    val target5_1 = folder5 + sink_res
    val target5_2 = folder5 + matrics_res

    val data6_1 = folder6 + res
    val data6_2 = folder6 + matrics
    val target6_1 = folder6 + sink_res
    val target6_2 = folder6 + matrics_res

    val data7_1 = folder7 + res
    val data7_2 = folder7 + matrics
    val target7_1 = folder7 + sink_res
    val target7_2 = folder7 + matrics_res

//    println(data7_1)
//    println(data7_2)
//    println(target7_1)
//    println(target7_2)

    val spark = SparkSession.builder()
      .appName("Spark App")
      .master("local[3]")
      .getOrCreate()

    val df1_res = readData(spark, data1_1)
    val df1_matrics = readData(spark, data1_2)

    val df2_res = readData(spark, data2_1)
    val df2_matrics = readData(spark, data2_2)

    val df3_res = readData(spark, data3_1)
    val df3_matrics = readData(spark, data3_2)

    val df4_res = readData(spark, data4_1)
    val df4_matrics = readData(spark, data4_2)

    val df5_res = readData(spark, data5_1)
    val df5_matrics = readData(spark, data5_2)

    val df6_res = readData(spark, data6_1)
    val df6_matrics = readData(spark, data6_2)

    val df7_res = readData(spark, data7_1)
    val df7_matrics = readData(spark, data7_2)

    sink(df1_res, target1_1)
    sink(df1_matrics, target1_2)

    sink(df2_res, target2_1)
    sink(df2_matrics, target2_2)

    sink(df3_res, target3_1)
    sink(df3_matrics, target3_2)

    sink(df4_res, target4_1)
    sink(df4_matrics, target4_2)

    sink(df5_res, target5_1)
    sink(df5_matrics, target5_2)

    sink(df6_res, target6_1)
    sink(df6_matrics, target6_2)

    sink(df7_res, target7_1)
    sink(df7_matrics, target7_2)

    spark.stop()
  }
}
