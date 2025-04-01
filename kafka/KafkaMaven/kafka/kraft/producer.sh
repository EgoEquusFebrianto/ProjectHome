kakfa-console-consumer.sh \
  --topic cars \
  --property parse.key=true \
  --property key.separator=: \
  --bootsrap-server localhost:9092,localhost:9096,localhost:9097