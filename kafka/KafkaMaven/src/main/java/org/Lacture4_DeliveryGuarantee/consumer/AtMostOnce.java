package org.Lacture4_DeliveryGuarantee.consumer;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Collections;
import java.time.Duration;

public class AtMostOnce {
    private static final Logger log = LoggerFactory.getLogger(AtMostOnce.class.getName());

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers","localhost:9092");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("group.id", "consumer-showroom");
        props.put("auto.offset.reset", "earliest");

        // Important Settings
        props.put("enable.auto.commit", "false");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("cars"));

        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

        // Implementation must be done after consuming the data is Commit
        consumer.commitSync();

        for(ConsumerRecord<String, String> record : records) {
//            log.info("Successfully read record: Key={}, Value={} | Partition={}, Offset={}",
//                    record.key(), record.value(), record.partition(), record.offset()
//            );
            System.out.printf("Successfully read record: Key=%s, Value=%s | Partition=%d, Offset=%d%n",
                    record.key(), record.value(), record.partition(), record.offset());
        }
        consumer.close();

    }
}
