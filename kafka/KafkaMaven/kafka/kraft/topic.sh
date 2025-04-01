kafka-topics.sh --create \
    --topic cars \
    --bootstrap-server localhost:9092,localhost:9096,localhost:9097 \
    --partitions 3 \
		--replication-factor 3