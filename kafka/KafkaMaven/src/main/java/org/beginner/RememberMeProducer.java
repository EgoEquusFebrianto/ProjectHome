package org.beginner;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Map;


public class RememberMeProducer {
    private static Logger log = LoggerFactory.getLogger(RememberMeProducer.class.getName());

    public static void main(String[] args) {
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

        Properties props = new Properties();
        props.put("bootstrap.servers", "172.25.5.7:9092,172.25.5.7:9093,172.25.5.7:9094");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        for (Map.Entry<String, String> entry : cars.entrySet()) {
            ProducerRecord<String, String> record = new ProducerRecord<>("cars", entry.getKey(), entry.getValue());

            producer.send(record, (recordMetadata, e) -> {
                if (e == null) {
                    log.info("Sent = {} value = {} to partition = {}", record.key(), record.value(), recordMetadata.partition());
                } else {
                    log.error("Unexpected Error Occurred {}", e.getMessage());
                }
            });
        }

        producer.close();
    }
}
