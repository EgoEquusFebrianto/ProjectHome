package study_case

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object DatasetPreparation {
  def main(args: Array[String]): Unit = {

    println("Spark App Initialize...")

    val spark = SparkSession.builder()
      .appName("Dataset Preparation")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val baseDir = "D:/TA/03-11/*.csv"

    // =====================================================
    // FEATURE YANG DIGUNAKAN
    // =====================================================
    val featureCols = Seq(
      "Flow_Duration",
      "Total_Fwd_Packets",
      "Total_Backward_Packets",
      "Flow_Bytes_s",
      "Flow_Packets_s",
      "Min_Packet_Length",
      "Max_Packet_Length",
      "Packet_Length_Mean",
      "Packet_Length_Std",
      "Packet_Length_Variance",
      "Flow_IAT_Mean",
      "Flow_IAT_Std",
      "Fwd_IAT_Mean",
      "Fwd_IAT_Std",
      "Bwd_IAT_Mean",
      "Bwd_IAT_Std",
      "SYN_Flag_Count",
      "RST_Flag_Count",
      "ACK_Flag_Count",
      "Fwd_Header_Length",
      "Bwd_Header_Length",
      "Subflow_Fwd_Packets",
      "Subflow_Bwd_Packets",
      "Subflow_Fwd_Bytes",
      "Subflow_Bwd_Bytes",
      "Init_Win_bytes_forward",
      "Init_Win_bytes_backward"
    )

    // =====================================================
    // RENAME KOLOM SESUAI ATURAN AVRO
    // =====================================================
    def sanitizeColumnName(colName: String): String = {
      colName
        .trim
        .replaceAll("[^a-zA-Z0-9_]", "_")
        .replaceAll("_+", "_")
        .stripPrefix("_")
        .stripSuffix("_")
    }

    def sanitizeColumns(df: DataFrame): DataFrame = {
      df.toDF(df.columns.map(sanitizeColumnName): _*)
    }

    // =====================================================
    // LOAD DATA
    // =====================================================
    val rawDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(baseDir)

    val df = sanitizeColumns(rawDF)
      .withColumn("Label", upper(trim(col("Label"))))


    // =====================================================
    // AMBIL DATA SESUAI LABEL
    // =====================================================

    val benignDF = df
      .filter($"Label" === "BENIGN")

    val synDF = df
      .filter($"Label" === "SYN")
      .orderBy(rand())
      .limit(150000)

    val udpDF = df
      .filter($"Label" === "UDP")
      .orderBy(rand())
      .limit(150000)

    val ldapDF = df
      .filter($"Label" === "LDAP")
      .orderBy(rand())
      .limit(150000)

    // =====================================================
    // GABUNGKAN
    // =====================================================
    val selectedDF = benignDF
      .unionByName(synDF)
      .unionByName(udpDF)
      .unionByName(ldapDF)

//    println(s"Jumlah data sebelum cleaning: ${selectedDF.count()}")

    // =====================================================
    // HAPUS NULL
    // =====================================================
    val noNullDF = selectedDF.na.drop(featureCols)

    // =====================================================
    // HAPUS POSITIVE & NEGATIVE INFINITY
    // =====================================================
    val infinityCondition = featureCols
      .map(c =>
        col(c) =!= Double.PositiveInfinity &&
          col(c) =!= Double.NegativeInfinity
      )
      .reduce(_ && _)

    val cleanedDF = noNullDF
      .filter(infinityCondition)

//    println(s"Jumlah data setelah cleaning: ${cleanedDF.count()}")

//    cleanedDF.groupBy("Label").count().show(false)

    // =====================================================
    // TAMBAH ID UNTUK PEMISAHAN DATA
    // =====================================================
    val finalCleanedDF = cleanedDF
      .withColumn("row_id", monotonically_increasing_id())
      .persist()

    // =====================================================
    // DATASET TRAINING ML
    // =====================================================

    val benignTrain = finalCleanedDF
      .filter($"Label" === "BENIGN")
      .orderBy(rand())
      .limit(25000)

    val synTrain = finalCleanedDF
      .filter($"Label" === "SYN")
      .orderBy(rand())
      .limit(20000)

    val udpTrain = finalCleanedDF
      .filter($"Label" === "UDP")
      .orderBy(rand())
      .limit(20000)

    val ldapTrain = finalCleanedDF
      .filter($"Label" === "LDAP")
      .orderBy(rand())
      .limit(15000)

    val trainingDF = benignTrain
      .unionByName(synTrain)
      .unionByName(udpTrain)
      .unionByName(ldapTrain)

//    println("Distribusi Dataset Pelatihan:")
//    trainingDF.groupBy("Label").count().show(false)

    // =====================================================
    // SISA DATA SERANGAN
    // =====================================================

    val synRemaining = finalCleanedDF
      .filter($"Label" === "SYN")
      .join(
        synTrain.select("row_id"),
        Seq("row_id"),
        "left_anti"
      )

    val udpRemaining = finalCleanedDF
      .filter($"Label" === "UDP")
      .join(
        udpTrain.select("row_id"),
        Seq("row_id"),
        "left_anti"
      )

    val ldapRemaining = finalCleanedDF
      .filter($"Label" === "LDAP")
      .join(
        ldapTrain.select("row_id"),
        Seq("row_id"),
        "left_anti"
      )

    // =====================================================
    // SEMUA BENIGN MASUK PIPELINE
    // =====================================================

    val benignPipeline = finalCleanedDF
      .filter($"Label" === "BENIGN")

    // =====================================================
    // DATASET RUNNING PIPELINE
    // =====================================================

    val prePipelineDF = benignPipeline
      .unionByName(synRemaining)
      .unionByName(udpRemaining)
      .unionByName(ldapRemaining)

    val pipelineDF = prePipelineDF
      .orderBy(rand())

//    println("Distribusi Dataset Pipeline:")
//    pipelineDF.groupBy("Label").count().show(false)

    // =====================================================
    // HAPUS row_id SEBELUM DISIMPAN
    // =====================================================

    val trainingOutput =
      trainingDF.drop("row_id")

    val pipelineOutput =
      pipelineDF.drop("row_id")

    // =====================================================
    // SIMPAN DATASET PELATIHAN
    // =====================================================

    trainingOutput
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv("D:/TA/03-11/dataset_pelatihan_ml")

    // =====================================================
    // SIMPAN DATASET PIPELINE
    // =====================================================

    pipelineOutput
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv("D:/TA/03-11/dataset_simulasi_pipeline")

    // =====================================================
    // SIMPAN HASIL PROSES
    // =====================================================
    selectedDF
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv("D:/TA/03-11/data_hasil_pengambilan")

    noNullDF
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv("D:/TA/03-11/data_hasil_pembersihan_nilai_null")

    cleanedDF
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv("D:/TA/03-11/data_hasil_pembersihan_nilai_infinity")

    finalCleanedDF
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv("D:/TA/03-11/data_hasil_akhir_pembersihan_dan_pemberian_row_id")

    prePipelineDF
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv("D:/TA/03-11/data_hasil_pengambilan_untuk_simulasi_pipeline_sebelum_pengacakan")

    pipelineDF
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv("D:/TA/03-11/data_hasil_pengambilan_untuk_simulasi_pipeline_sesudah_pengacakan")

    println("Dataset berhasil dibuat.")

    spark.stop()

    println("Spark Terminate...")
  }
}