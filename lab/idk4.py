from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, to_json, struct, col

# Inisialisasi Spark dengan dukungan Avro & Kafka
spark = SparkSession.builder \
    .appName("SparkKafkaIntegration") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-avro_2.12:3.5.3,"
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
    .config("spark.executor.cores", 1)  \
    .config("spark.executor.instances", 1)  \
    .config("spark.driver.memory", "512m") \
    .config("spark.executor.memory", "1g")  \
    .config("spark.executor.memoryOverhead", "256m") \
    .config("spark.sql.shuffle.partitions", 10)  \
    .getOrCreate()

# Data pertama (DataFrame A) - Data pengguna
data_a = [
    (1, "Alice", 25, "A"), (2, "Bob", 30, "B"), (3, "Charlie", 35, "A"),
    (4, "David", 40, "C"), (5, "Eve", 28, "B"), (6, "Frank", 33, "C"),
    (7, "Grace", 29, "A"), (8, "Hannah", 31, "B"), (9, "Ian", 37, "C"),
]
columns_a = ["id", "name", "age", "category"]
df_a = spark.createDataFrame(data_a, columns_a)

# Data kedua (DataFrame B) - Informasi tambahan
data_b = [
    (1, "F", "USA"), (2, "M", "USA"), (3, "M", "Canada"),
    (4, "M", "Canada"), (5, "F", "UK"), (6, "M", "UK"),
    (7, "F", "USA"), (8, "M", "Canada"), (9, "M", "UK"),
]
columns_b = ["id", "gender", "country"]
df_b = spark.createDataFrame(data_b, columns_b)

# 🔹 JOIN berdasarkan id
df_joined = df_a.join(df_b, "id")

# 🔹 GROUP BY country dan hitung rata-rata umur + jumlah orang per negara
df_grouped = df_joined.groupBy("country") \
    .agg(avg("age").alias("avg_age"), count("*").alias("total_people"))

# 🔹 Transformasi ke Key-Value untuk Kafka
kafka_kv_df = df_grouped.select(
    col("country").alias("key"),  # Jadikan country sebagai key
    to_json(struct("*")).alias("value")  # Ubah seluruh baris ke JSON sebagai value
)

# 🔹 Konfigurasi Kafka
kafka_topic = "capstone-cloud"
kafka_bootstrap_servers = "pkc-4j8dq.southeastasia.azure.confluent.cloud:9092"
kafka_api_key = "V7XQGD6BF2GXJBUZ"
kafka_api_secret = "vKK9V8b9Pz/7PzKAzZOJcvZ76Q9whAS7ODM8iU76v4doPmtu/Gvw3cW/RqYPyNyE"

# 🔹 Kirim ke Kafka
kafka_kv_df.write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config",
            f"org.apache.kafka.common.security.plain.PlainLoginModule required "
            f"username='{kafka_api_key}' password='{kafka_api_secret}';") \
    .option("topic", kafka_topic) \
    .save()

# Stop Spark session
spark.stop()
