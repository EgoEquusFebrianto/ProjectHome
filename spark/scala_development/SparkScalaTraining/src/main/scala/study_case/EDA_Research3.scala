package study_case

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object EDA_Research3 {
  def main(args: Array[String]): Unit = {
    println("Spark App Initialize...")

    val spark = SparkSession.builder()
      .appName("SparkApp")
      .master("local[3]")
      .getOrCreate()

    import spark.implicits._

    val baseDir = "D:/TA/03-11/"

//    val dir1 = baseDir + "Syn.csv"
//    val dir2 = baseDir + "LDAP.csv"
//    val dir3 = baseDir + "UDP.csv"
    val dir1 = baseDir + "MSSQL.csv"
    val dir2 = baseDir + "NetBIOS.csv"
    val dir3 = baseDir + "Portmap.csv"
    val dir4 = baseDir + "UDPLag.csv"

    // =========================
    // FUNCTION: CLEAN COLUMN
    // =========================
    def cleanColumns(df: DataFrame): DataFrame = {
      df.toDF(df.columns.map(_.trim): _*)
    }

    // =========================
    // LOAD + CLEAN
    // =========================
    val synDF = cleanColumns(spark.read.option("header", "true").csv(dir1))
      .withColumn("Label", upper(trim($"Label")))

    val ldapDF = cleanColumns(spark.read.option("header", "true").csv(dir2))
      .withColumn("Label", upper(trim($"Label")))

    val udpDF = cleanColumns(spark.read.option("header", "true").csv(dir3))
      .withColumn("Label", upper(trim($"Label")))

//    val mssqlDF = cleanColumns(spark.read.option("header", "true").csv(dir1))
//      .withColumn("Label", upper(trim($"Label")))
//
//    val netbiosDF = cleanColumns(spark.read.option("header", "true").csv(dir2))
//      .withColumn("Label", upper(trim($"Label")))
//
//    val portmapDF = cleanColumns(spark.read.option("header", "true").csv(dir3))
//      .withColumn("Label", upper(trim($"Label")))
//
//    val udplagDF = cleanColumns(spark.read.option("header", "true").csv(dir4))
//      .withColumn("Label", upper(trim($"Label")))
    // =========================
    // SAMPLING 150K PER LABEL
    // =========================
    val synSample = synDF
      .filter($"Label" === "SYN")
      .orderBy(rand())
      .limit(45000)

    val ldapSample = ldapDF
      .filter($"Label" === "LDAP")
      .orderBy(rand())
      .limit(45000)

    val udpSample = udpDF
      .filter($"Label" === "UDP")
      .orderBy(rand())
      .limit(45000)

//    val mssqlSample = mssqlDF
//      .filter($"Label" === "MSSQL")
//      .orderBy(rand())
//      .limit(45000)
//
//    val netbiosSample = netbiosDF
//      .filter($"Label" === "NETBIOS")
//      .orderBy(rand())
//      .limit(45000)
//
//    val portmapSample = portmapDF
//      .filter($"Label" === "PORTMAP")
//      .orderBy(rand())
//      .limit(45000)
//
//    val udplagSample = udplagDF
//      .filter($"Label" === "UDPLAG")
//      .orderBy(rand())
//      .limit(45000)

    // =========================
    // GABUNG SEMUA
    // =========================

    val finalDF = synSample
      .unionByName(ldapSample)
      .unionByName(udpSample)

//    val finalDF = mssqlSample
//      .unionByName(netbiosSample)
//      .unionByName(portmapSample)
//      .unionByName(udplagSample)

    // =========================
    // VALIDASI
    // =========================
    println("Distribusi Final Dataset:")
    finalDF.groupBy("Label").count().show()

    println(s"Total Data: ${finalDF.count()}")

    // =========================
    // SIMPAN
    // =========================
    finalDF.coalesce(1).write
      .mode("overwrite")
      .option("header", "true")
      .csv(baseDir + "final_dataset_remaining_labels")

    println("Spark Job End..")
    spark.stop()
    println("Spark Terminate..")
  }
}
