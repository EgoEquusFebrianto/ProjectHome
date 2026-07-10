package org.Lacture4_DeliveryGuarantee.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Map;

public class AtLeastOnce {
    private static final Logger log = LoggerFactory.getLogger(AtLeastOnce.class.getName());

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        // Important Settings
        props.put("acks", "1"); // "all" can also be used...
        props.put("retries", "3"); // set according to system needs
        // props.put("enable.idempotence", "true");

        props.put("enable.idempotence", "false");  // Just for error testing

        // More Robust Implementation
        // props.put("acks", "all");
        // props.put("retries", Integer.MAX_VALUE);
        // props.put("delivery.timeout.ms", "120000");
        // props.put("enable.idempotence", "false");
        // props.put("max.in.flight.requests.per.connection", "1");

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

        for (Map.Entry<String, String> entry : cars.entrySet()) {
            ProducerRecord<String, String> record = new ProducerRecord<>("car-showroom", entry.getKey(), entry.getValue());

            producer.send(record, (metadata, e) -> {
                if(e != null) {
                    log.error("unexpected error is appear...", e);
                } else {
                    log.info("Success Send Data [{}:{}]", entry.getKey(), entry.getValue());
                }
            });
        }

        producer.flush();
        producer.close();
    }
}
