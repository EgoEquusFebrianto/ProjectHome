kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic Cars \
  --from-beginning \
  --property print.key=true \
  --property key.separator=" : "