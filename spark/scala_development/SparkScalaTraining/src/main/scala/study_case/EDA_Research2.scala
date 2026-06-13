package study_case
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object EDA_Research2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SparkApp")
      .master("local[3]")
      .getOrCreate()

    import spark.implicits._

    val baseDir = "D:/TA/03-11/*.csv"   // 🔥 LOAD SEMUA CSV

    // =========================
    // FUNCTION: CLEAN COLUMN
    // =========================
    def cleanColumns(df: DataFrame): DataFrame = {
      df.toDF(df.columns.map(_.trim): _*)
    }

    // =========================
    // LOAD SEMUA FILE
    // =========================
    val rawDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(baseDir)

    val df = cleanColumns(rawDF)
      .withColumn("Label", upper(trim($"Label")))

    // =========================
    // DEBUG LABEL (PENTING!)
    // =========================
    println("Semua jenis label:")
    df.select("Label").distinct().show(false)

    // =========================
    // FILTER BENIGN
    // =========================
    val benignDF = df.filter($"Label" === "BENIGN")

    // =========================
    // VALIDASI
    // =========================
    println("Jumlah data BENIGN:")
    println(benignDF.count())

    benignDF.groupBy("Label").count().show()

    // =========================
    // SIMPAN
    // =========================
    benignDF.write
      .mode("overwrite")
      .option("header", "true")
      .csv("D:/TA/03-11/benign_only")

    println("Spark Job End..")

    println("Spark Job End..")
    spark.stop()
  }
}
