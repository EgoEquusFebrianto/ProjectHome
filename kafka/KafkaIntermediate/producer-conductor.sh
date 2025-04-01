kafka-console-producer.sh --topic messages \
	--producer.config playground.config \
	--bootstrap-server cluster.playground.cdkt.io:9092 \
	--producer-property acks=all
	
# --producer-property acks=all berarti semua replica partition harus menerima pesan sebelum dianggap berhasil