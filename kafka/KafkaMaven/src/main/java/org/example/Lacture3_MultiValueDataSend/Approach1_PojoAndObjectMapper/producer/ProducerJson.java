package org.example.Lacture3_MultiValueDataSend.Approach1_PojoAndObjectMapper.producer;

import org.example.Lacture3_MultiValueDataSend.Approach1_PojoAndObjectMapper.Order;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Arrays;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerJson {
    private static final Logger log = LoggerFactory.getLogger(ProducerJson.class.getName());

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "172.25.5.7:9092,172.25.5.7:9093,172.25.5.7:9094");
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", OrderSerializer.class.getName());
        properties.put("compression.type", "snappy");

        KafkaProducer<String, Order> producer = new KafkaProducer<>(properties);

        Order[] orders = {
                new Order("001", "Budi", Arrays.asList("Es Teh", "Nasi Goreng"), 35000),
                new Order("002", "Ani", Arrays.asList("Jus Alpukat", "Mie Ayam"), 40000),
                new Order("003", "Siti", Arrays.asList("Air Mineral", "Soto Ayam"), 30000),
                new Order("004", "Andi", Arrays.asList("Kopi", "Roti Bakar"), 25000),
                new Order("005", "Rina", Arrays.asList("Teh Tarik", "Nasi Uduk"), 32000),
                new Order("006", "Doni", Arrays.asList("Es Jeruk", "Ayam Geprek"), 38000),
                new Order("007", "Eka", Arrays.asList("Cappuccino", "Pisang Goreng"), 27000),
                new Order("008", "Lina", Arrays.asList("Wedang Jahe", "Bakso"), 36000),
                new Order("009", "Fajar", Arrays.asList("Es Campur", "Gado-gado"), 45000),
                new Order("010", "Rudi", Arrays.asList("Teh Manis", "Sate Ayam"), 50000)
        };

        for (Order order : orders) {
            ProducerRecord<String, Order> record = new ProducerRecord<>(
                    "orders",
                    order.getOrderID(),
                    order
            );

            producer.send(record, (metadata, e) -> {
               if (e == null) {
                   log.info(
                           "Order terkirim ke topic={} partition={} offset={}}",
                           metadata.topic(), metadata.partition(), metadata.offset()
                   );
               } else {
                   log.error("Error tak terduga terjadi", e);
               }
            });
        }

        producer.close();
    }
}