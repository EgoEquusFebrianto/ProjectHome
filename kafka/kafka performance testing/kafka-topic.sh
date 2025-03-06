kafka-topics --create --topic perf \
  --bootstrap-server localhost:9092 \
  --replication-factor 3 \
  --partitions 100