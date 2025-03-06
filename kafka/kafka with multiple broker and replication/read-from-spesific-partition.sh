kafka-console-consumer.sh --topic months \
  --bootstrap-server localhost:9092,localhost:9093,localhost:9094 \
  --partition 6 \
  --from-beginning