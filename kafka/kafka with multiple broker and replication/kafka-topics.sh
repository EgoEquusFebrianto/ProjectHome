kafka-topics.sh --create --topic months \
  --bootstrap-server localhost:9092,localhost:9093,localhost:9094 \
  --replication-factor 3 \
  --partitions 7