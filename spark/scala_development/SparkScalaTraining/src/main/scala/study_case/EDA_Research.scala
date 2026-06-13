package study_case

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.slf4j.{Logger, LoggerFactory}
import scala.io.StdIn

object EDA_Research {
//  @transient private lazy val logger: Logger = LoggerFactory.getLogger(EDA_Research.getClass.getName)
  def main(args: Array[String]): Unit = {
    println("Spark App Initialize...")
    val spark = SparkSession.builder()
      .appName("SparkApp")
      .master("local[3]")
      .getOrCreate()

    import spark.implicits._

    val baseDir = "D:/TA/03-11/"

    val dir1 = baseDir + "Syn.csv"
    val dir2 = baseDir + "LDAP.csv"
    val dir3 = baseDir + "UDP.csv"

    // =========================
    // FUNCTION: CLEAN COLUMN NAME
    // =========================
    def cleanColumns(df: DataFrame): DataFrame = {
      df.toDF(df.columns.map(_.trim): _*)
    }

    // =========================
    // LOAD + CLEAN DATA
    // =========================
    val synDF = cleanColumns(
      spark.read.option("header", "true").csv(dir1)
    )

    val ldapDF = cleanColumns(
      spark.read.option("header", "true").csv(dir2)
    )

    val udpDF = cleanColumns(
      spark.read.option("header", "true").csv(dir3)
    )

    // =========================
    // DEBUG (OPSIONAL TAPI DISARANKAN)
    // =========================
    println("Kolom setelah cleaning:")
    synDF.columns.foreach(c => println(s"'$c'"))

    // =========================
    // NORMALISASI LABEL (optional tapi aman)
    // =========================
    val synClean = synDF.withColumn("Label", upper(trim($"Label")))
    val ldapClean = ldapDF.withColumn("Label", upper(trim($"Label")))
    val udpClean = udpDF.withColumn("Label", upper(trim($"Label")))

    // =========================
    // FILTER BENIGN (SEMUA DIAMBIL)
    // =========================
    val benignSyn = synClean.filter($"Label" === "BENIGN")
    val benignLdap = ldapClean.filter($"Label" === "BENIGN")
    val benignUdp = udpClean.filter($"Label" === "BENIGN")

    val benignAll = benignSyn
      .union(benignLdap)
      .union(benignUdp)

    // =========================
    // SAMPLING SERANGAN (RANDOM)
    // =========================
    val synAttack = synClean
      .filter($"Label" === "SYN")
      .orderBy(rand())
      .limit(27000)

    val ldapAttack = ldapClean
      .filter($"Label" === "LDAP")
      .orderBy(rand())
      .limit(15000)

    val udpAttack = udpClean
      .filter($"Label" === "UDP")
      .orderBy(rand())
      .limit(27000)

    // =========================
    // GABUNG SEMUA DATA
    // =========================
    val finalDF = benignAll
      .union(synAttack)
      .union(ldapAttack)
      .union(udpAttack)

    // =========================
    // VALIDASI DISTRIBUSI (WAJIB UNTUK LAPORAN)
    // =========================
    println("Distribusi Final Dataset:")
    finalDF.groupBy("Label").count().show()

    println("Total Data:")
    println(finalDF.count())

    // =========================
    // SIMPAN DATASET FINAL
    // =========================
    finalDF.write
      .mode("overwrite")
      .option("header", "true")
      .csv(baseDir + "final_dataset")

    println("Spark Job End..")
    StdIn.readLine()
    spark.stop()
    println("Spark Terminate..")
  }
}
