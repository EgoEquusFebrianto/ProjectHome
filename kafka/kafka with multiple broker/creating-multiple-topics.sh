kafka-topics.sh --create --topic cars \
--bootstrap-server localhost:9092,localhost:9093,localhost:9094 \
--partitions 5 \
--replication-factor 1