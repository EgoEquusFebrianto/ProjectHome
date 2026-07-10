package org.example.Lacture4_DeliveryGuarantee.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExactlyOnce {
    private static final Logger consumerLog = LoggerFactory.getLogger("ConsumerSide");
    private static final Logger producerLog = LoggerFactory.getLogger("ProducerSide");

    private static final String servers = "localhost:9092";
    private static final String input_topic = "cars";
    private static final String output_topic = "cars-showroom";
    private static final String group_id = "exactly-once-group2";
    private static final String trans_id = "tx-producer-1";

    public static void main(String[] args) {
        Properties consumerProperties = new Properties();
        consumerProperties.put("bootstrap.servers", servers);
        consumerProperties.put("key.deserializer", StringDeserializer.class.getName());
        consumerProperties.put("value.deserializer", StringDeserializer.class.getName());
        consumerProperties.put("group.id", group_id);
        consumerProperties.put("isolation.level", "read_committed");

        Properties producerProperties = new Properties();
        producerProperties.put("bootstrap.servers", servers);
        producerProperties.put("key.serializer", StringSerializer.class.getName());
        producerProperties.put("value.serializer", StringSerializer.class.getName());
        producerProperties.put("enable.idempotence", "true");
        producerProperties.put("transactional.id", trans_id);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties);
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties);

        consumerLog.info("Subscribing to topic [{}] with group.id [{}]", input_topic, group_id);
        consumer.subscribe(Collections.singletonList(input_topic));

        producer.initTransactions();

        int idleCount = 0;
        int maxIdleCount = 5;

        try {
            while (idleCount < maxIdleCount) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

                if (records.isEmpty()) {
                    idleCount++;
                    consumerLog.info("No records polled. Idle count: {}", idleCount);
                    continue;
                }

                idleCount = 0; // reset karena ada data

                producer.beginTransaction();
                producerLog.info("Transaction started for processing {} records", records.count());

                for (ConsumerRecord<String, String> record : records) {
                    consumerLog.info("Consumed record - Key: {}, Value: {}, Partition: {}, Offset: {}",
                            record.key(), record.value(), record.partition(), record.offset());

                    String newValue = record.value().toLowerCase(); // proses sederhana

                    producerLog.info("Producing transformed record to [{}] - Key: {}, New Value: {}",
                            output_topic, record.key(), newValue);

                    ProducerRecord<String, String> outputRecord = new ProducerRecord<>(output_topic, record.key(), newValue);
                    producer.send(outputRecord);
                }

                // Kirim offset ke transaksi
                Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                for (TopicPartition partition : records.partitions()) {
                    List<ConsumerRecord<String, String>> partitionRecord = records.records(partition);
                    long lastOffset = partitionRecord
                            .get(partitionRecord.size() - 1)
                            .offset();
                    offsets.put(partition, new OffsetAndMetadata(lastOffset + 1));
                }

                producer.sendOffsetsToTransaction(offsets, group_id);
                producer.commitTransaction();
                producerLog.info("Transaction committed successfully.");
            }

            consumerLog.info("Max idle reached ({}). Exiting.", maxIdleCount);
        } catch (Exception e) {
            producerLog.error("Transaction aborted due to error: {}", e.getMessage(), e);
            producer.abortTransaction();
        } finally {
            producerLog.info("Shutting down producer.");
            producer.close();

            consumerLog.info("Shutting down consumer.");
            consumer.close();
        }
    }
}

