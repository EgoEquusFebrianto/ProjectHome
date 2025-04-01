UUID=$(kafka-storage.sh random-uuid)

kafka-storage.sh format -t $UUID -c $KAFKA_CNF/kraft/server.properties
kafka-storage.sh format -t $UUID -c $KAFKA_CNF/kraft/server2.properties
kafka-storage.sh format -t $UUID -c $KAFKA_CNF/kraft/server1.properties