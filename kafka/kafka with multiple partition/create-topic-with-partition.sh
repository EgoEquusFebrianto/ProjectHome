kafka-topics.sh --create --topics animals \
--bootstrap-server localhost:9092 \
--partitions 3 \
--replication-factor 1 