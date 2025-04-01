package org.example.producerWithCallback;

import org.apache.kafka.clients.producer.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Properties;

public class Producer {
    private static final Logger logger = LogManager.getLogger(Producer.class);

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

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer =  new KafkaProducer<>(properties);

        for(Map.Entry<String, String> entry: cars.entrySet()) {
            ProducerRecord<String, String> record = new ProducerRecord<>("cars", entry.getKey(), entry.getValue());

            // Lambda Callback (inline)
//            producer.send(record, (metadata, exception) -> {
//                if (exception == null) {
//                    System.out.printf("Sent: Key=%s, Value=%s to Partition=%d%n",
//                            record.key(), record.value(), metadata.partition());
//                } else {
//                    logger.error("Terjadi error saat mengirim ke Kafka:%n", exception);
//                }
//            });

            // Class Callback
            producer.send(record, new ProducerCallback(logger, record));
        }

        producer.flush();
        producer.close();
    }
}
