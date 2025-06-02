package org.intermediate.DeliveryGuarantee.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;
import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestingTopic {
    private static final Logger log = LoggerFactory.getLogger(TestingTopic.class.getName());
    private static final String servers = "localhost:9092";
    private static final String group_id = "javaCars-testing1";
    private static final String input_topic = "cars";
//    private static final String input_topic = "cars-showroom";

    public static void main(String[] args) {
        Properties consumerProperties = new Properties();
        consumerProperties.put("bootstrap.servers", servers);
        consumerProperties.put("key.deserializer", StringDeserializer.class.getName());
        consumerProperties.put("value.deserializer", StringDeserializer.class.getName());
        consumerProperties.put("group.id", group_id);
        consumerProperties.put("auto.offset.reset", "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties);
        consumer.subscribe(Collections.singletonList(input_topic));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("Success Read Data [{}:{}]", record.key(), record.value());
                }
            }
        } finally {
            consumer.close();
        }
    }
}
