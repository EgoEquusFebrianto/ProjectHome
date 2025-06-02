package org.intermediate.DeliveryGuarantee.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Map;

public class AtMostOnce {
    private static final Logger log = LoggerFactory.getLogger(AtMostOnce.class.getName());
    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        // Important Settings
        props.put("acks", "0");
        props.put("retries", "0");
        props.put("enable.idempotence", "false");

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

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        for(Map.Entry<String, String> car : cars.entrySet()) {
            ProducerRecord<String, String> record = new ProducerRecord<>(
                    "car-showroom", car.getKey(), car.getValue()
            );

            producer.send(record, (metadata, e) -> {
                if (e != null) {
                    log.error("Failed Sent Data [{}:{}] => {}", car.getKey(), car.getValue(), e.getMessage());
                } else {
                    log.info("Success Sent Data [{}:{}]", car.getKey(), car.getValue());
                }
            });
        }

        producer.close();
    }
}