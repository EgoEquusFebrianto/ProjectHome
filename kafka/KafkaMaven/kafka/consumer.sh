kafka-console-consumer.sh \
  --topic numbers \
  --bootstrap-server localhost:9092 \
  --property print.key=true \
  --property key.separator=: