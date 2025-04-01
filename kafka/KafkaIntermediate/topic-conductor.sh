kafka-topics.sh --create \
	--topic messages \
	--partitions 3 \
	--command-config playground.config \
	--bootstrap-server cluster.playground.cdkt.io:9092
	
# --command-config playground.config

# FIle playground.config digunakan untuk menyediakan konfigurasi tambahan saat menjalankan perintah kafka. File ini berisi informasi autentifikasi dan otorisasi, seperti username, password, atau konfigurasi TLS/SSL.

# --bootstrap-server menggunakan hosting remote, bukan cluster kafka local.

# jenis jenis konfigurasi keamanan ini ada 2
# Certificates (SSL/TLS) dan Authmecanisms (SASL/PLAIN, SASL/SCRAM, dll)

# Contoh authentifikasi SASL dengan mekanisme SCRAM-SHA-512, playground.config berisi:

#security.protocol=SASL_SSL
#sasl.mechanism=SCRAM-SHA-512
#sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
#    username="your-username" \
#    password="your-password";

# SASL/PLAIN
#security.protocol=SASL_SSL
#sasl.mechanism=PLAIN
#sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
#    username="your-username" \
#    password="your-password";

# jika menggunakan SSL untuk keamanan
#security.protocol=SSL
#ssl.truststore.location=/path/to/truststore.jks
#ssl.truststore.password=your-password
#ssl.keystore.location=/path/to/keystore.jks
#ssl.keystore.password=your-password