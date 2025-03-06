kafka-producer-perf-test.sh \
  --topic perf \
  --num-records 1000 \
  --throughput 100 \
  --record-size 1000 \
  --producer-props bootstrap.servers=localhost:9092

# kafka-producer-perf-test.sh -> Perintah ini digunakan untuk
# mengukur performa Kafka Producer dengan mengirimkan sejumlah pesan ke topik tertentu.

# --topic perf ->  untuk membuat topik
# --num-records 1000 -> untuk mengirim dan menghasilkan 1000 pesan
# --throughput 100 -> mengontrol laju pengiriman pesan hingga 100 per detik
  # jika diatur dengan nilai -1, artinya tidak ada batasan (secepat mungkin)
# --record-size 1000 -> setiap pesan berukuran 1000 Byte (1KB), dengan kata lain
  # kafka akan mengirim 1000 pesan X 1000 byte = 1 MB total data
# --producer-props bootstrap.servers=localhost:9092
  # Menentukan alamat Kafka broker yang akan menerima pesan.