from pyspark.sql import SparkSession

# Membuat session Spark
spark = SparkSession.builder \
    .appName("Contoh PySpark Job") \
    .master("yarn") \
    .getOrCreate()

# Contoh DataFrame sederhana
data = [("Alice", 1), ("Bob", 2), ("Charlie", 3)]
columns = ["Nama", "Nilai"]

# Membuat DataFrame
df = spark.createDataFrame(data, columns)

# Tampilkan DataFrame
df.show()

# Melakukan operasi sederhana
df_filtered = df.filter(df["Nilai"] > 1)

# Tampilkan hasil setelah filter
df_filtered.show()

# Hentikan session Spark
spark.stop()
