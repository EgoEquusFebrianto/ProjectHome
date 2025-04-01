package org.example.consumercooperative;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerCooperative {
    private static final Logger log = LoggerFactory.getLogger(ConsumerCooperative.class);

    public static void main(String[] args) {
        log.info("Initialize Kafka Consumer Config...");
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "java");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());

        log.info("Kafka Consumer is running...");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        final Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                consumer.wakeup();

                try {
                    mainThread.join();
                } catch(InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        log.info("Kafka Job is Started...");
        try {
            consumer.subscribe(Collections.singletonList("cars"));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("key => {} | value => {}", record.key(), record.value());
                }
            }
        } catch (WakeupException e) {
            log.info("Consumer is Strating to Shutdown...");
        } catch (Exception e) {
            log.info("Unexpected Exception is Appear... %n", e);
        } finally {
            consumer.close();
            log.info("Consumer is Terminate...");
        }
    }
}
