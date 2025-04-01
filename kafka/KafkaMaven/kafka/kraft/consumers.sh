kakfa-console-consumer.sh \
  --topic cars \
  --property print.key=true \
  --property key.separator=: \
  --group java \
  --bootsrap-server localhost:9092,localhost:9096,localhost:9097 \
  --from-beginning