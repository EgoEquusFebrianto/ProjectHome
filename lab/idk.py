from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Inisialisasi Spark
spark = SparkSession.builder \
    .appName("SimpleDataProcessing") \
    .config("spark.executor.cores", 1)  \
    .config("spark.executor.instances", 1)  \
    .config("spark.driver.memory", "512m") \
    .config("spark.executor.memory", "1g")  \
    .config("spark.executor.memoryOverhead", "256m") \
    .config("spark.sql.shuffle.partitions", 10)  \
    .getOrCreate()


# Data contoh langsung dalam bentuk list of tuples
data = [
    (1, "Alice", 25, "alice@example.com"),
    (2, "Bob", 30, "bob@example.com"),
    (3, "Charlie", 35, "charlie@example.com"),
    (1, "Alice", 25, "alice@example.com"),  # Data duplikat
    (4, "David", 40, None)  # Data dengan nilai None
]

# Skema DataFrame
columns = ["id", "name", "age", "email"]

# Konversi ke DataFrame Spark
df = spark.createDataFrame(data, columns)

# Proses pembersihan: hapus duplikat dan baris dengan email None
df_cleaned = df.dropDuplicates().filter(col("email").isNotNull())

# Simpan sebagai Parquet
df_cleaned.write.mode("overwrite").parquet("output.parquet")

# Tampilkan hasil
df_cleaned.show()

# Hentikan Spark
spark.stop()
