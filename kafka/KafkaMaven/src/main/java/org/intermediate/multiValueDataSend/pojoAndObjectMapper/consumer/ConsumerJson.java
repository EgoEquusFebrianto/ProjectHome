package org.intermediate.multiValueDataSend.pojoAndObjectMapper.consumer;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.util.Properties;
import java.util.Collections;
import java.time.Duration;

import org.intermediate.multiValueDataSend.pojoAndObjectMapper.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerJson {
    private static final Logger log = LoggerFactory.getLogger(ConsumerJson.class);

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "172.25.5.7:9092,172.25.5.7:9093,172.25.5.7:9094");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", OrderDeserializer.class.getName());
        props.put("auto.offset.reset", "earliest");
        props.put("group.id", "java-orders");
        props.put("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());

        KafkaConsumer<String, Order> consumer = new KafkaConsumer<>(props);
        final Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Detect Shutdown, consumer.wakeup() is Started...");
            consumer.wakeup();

            try {
                mainThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));

        try {
            consumer.subscribe(Collections.singletonList("orders"));

            while (true) {
                ConsumerRecords<String, Order> records = consumer.poll(Duration.ofSeconds(1));

                for (ConsumerRecord<String, Order> record : records) {
                    Order order = record.value();
                    log.info(
                            "Consumed Order -> ID: {}, Product: {}, Total: {}",
                            order.getOrderID(),  order.getItems(),  order.getTotalPrice()
                    );
                }
            }

        } catch (WakeupException e) {
            log.info("Consumer is Strating to Shutdown...");
        } catch (Exception e) {
            log.error("Unexpected Exception occurred..." , e);
        } finally {
            log.info("Consumer is Terminate...");
            consumer.close();
        }
    }
}
