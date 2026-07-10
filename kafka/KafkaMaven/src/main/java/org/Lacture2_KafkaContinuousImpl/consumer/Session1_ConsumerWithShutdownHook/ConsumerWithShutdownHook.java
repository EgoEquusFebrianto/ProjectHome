package org.Lacture2_KafkaContinuousImpl.consumer.Session1_ConsumerWithShutdownHook;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;
import java.util.Collections;
import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerWithShutdownHook {
    private static Logger log = LoggerFactory.getLogger(ConsumerWithShutdownHook.class.getName());

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "172.25.5.7:9092,172.25.5.7:9093,172.25.5.7:9094");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("group.id", "java");
        props.put("auto.offset.reset", "earliest");
        props.put("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());

        log.info("Starting Kafka Consumer..");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        log.info("Kafka Consume is Running..");

        log.info("Implement Gracefully Shutdown method..");
        final Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Detect Shutdown, consumer.wakeup() is started...");
            consumer.wakeup();

            try {
                mainThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));

        log.info("Kafka Job is Started..");

        try {
            consumer.subscribe(Collections.singletonList("cars"));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("key => {} | value => {}", record.key(), record.value());
                }
            }
        } catch(WakeupException e) {
            log.info("Consumer is Started to shutdown..");
        } catch(Exception e) {
            log.info("Unexpected error occurred: {}", e.getMessage());
        } finally {
            log.info("Consumer is Terminate..");
            consumer.close();
        }
    }
}
