package org.example.Lacture4_DeliveryGuarantee;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;

import org.apache.kafka.common.serialization.StringSerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

public class ProducerData {
    private static final Logger log = LoggerFactory.getLogger(ProducerData.class.getName());

    public static void main(String[] args) {
        String bootstrapServers = "localhost:9092";
        String topicName = "cars";

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        Map<String, String> cars = Map.of(
                "Toyota", "Corolla",
                "Honda", "Civic",
                "Ford", "Mustang",
                "BMW", "X5",
                "Mercedes", "C-Class",
                "Hyundai", "Elantra",
                "Kia", "Sportage",
                "Nissan", "Altima",
                "Chevrolet", "Camaro",
                "Audi", "A4"
        );

        try {
            for (Map.Entry<String, String> car : cars.entrySet()) {
                ProducerRecord<String, String> record = new ProducerRecord<>(topicName, car.getKey(), car.getValue());

                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        log.info("Sent record to topic {} partition {} offset {}",
                                metadata.topic(), metadata.partition(), metadata.offset());
                    } else {
                        log.error("Error sending record", exception);
                    }
                });
            }

            producer.flush();
        } finally {
            producer.close();
        }
    }
}
