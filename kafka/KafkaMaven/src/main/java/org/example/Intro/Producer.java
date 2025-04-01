package org.example.Intro;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;

public class Producer {
    private static final Logger logger = LogManager.getLogger(Producer.class);

    public static void main(String[] args) {
        logger.info("Initialize Kafka Producer Properties");
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        logger.info("Kafka Producer Job Is Running...");

        for (var i = 0; i <= 10; i++) {
            var key = String.valueOf(i % 2);
            var value = String.valueOf(i);

            ProducerRecord<String, String> record = new ProducerRecord<>("numbers", key, value);
            producer.send(record);
        }

        logger.info("Kafka Producer Job Is Complete...");

        producer.close();
        logger.info("Kafka Producer Terminate...");
    }
}