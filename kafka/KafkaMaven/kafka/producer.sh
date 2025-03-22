kafka-console-producer.sh \
  --topic numbers \
  --bootstrap-server localhost:9092 \
  --property parse.key=true \
  --property key.separator=: