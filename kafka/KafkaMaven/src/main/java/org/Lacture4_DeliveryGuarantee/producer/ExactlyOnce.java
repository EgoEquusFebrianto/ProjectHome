package org.Lacture4_DeliveryGuarantee.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Map;

public class ExactlyOnce {
    private static final Logger log = LoggerFactory.getLogger(ExactlyOnce.class.getName());

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        // Important Settings
        props.put("acks", "all"); // sudah default untuk versi 3.x
        props.put("enable.idempotence", "true"); // sudah default untuk versi 3.x
        props.put("retries", Integer.MAX_VALUE); // sudah default untuk versi 3.x
        props.put("transactional.id", "unique-producer-123");

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
            // 1. Init Transaction
            log.info("Initializing transaction...");
            producer.initTransactions();
            log.info("Transaction initialized successfully");

            // 2. Begin Transaction
            log.info("Starting transaction...");
            producer.beginTransaction();

            // 3. Send Records
            for (Map.Entry<String, String> car : cars.entrySet()) {
                ProducerRecord<String, String> record = new ProducerRecord<>(
                        "car-showroom", car.getKey(), car.getValue()
                );

                // Add callback for each send
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        log.error("Failed to send record: Key={}, Value={}. Error: {}",
                                record.key(), record.value(), exception.getMessage());
                    } else {
                        log.info("Successfully sent record: Key={}, Value={} | Partition={}, Offset={}",
                                record.key(), record.value(), metadata.partition(), metadata.offset()
                        );
                    }
                });
            }

            // 4. Commit Transaction
            log.info("Committing transaction...");
            producer.commitTransaction();
            log.info("Transaction committed successfully. All messages sent exactly once.");

        } catch (Exception e) {
            // 5. Handle Errors
            log.error("Transaction failed. Reason: {}", e.getMessage());
            log.error("Initiating rollback...");
            producer.abortTransaction();
            log.error("Transaction aborted. No partial data will be written.");
        } finally {
            // 6. Cleanup
            log.info("Closing producer...");
            producer.close();
            log.info("Producer closed gracefully");
        }
    }
}