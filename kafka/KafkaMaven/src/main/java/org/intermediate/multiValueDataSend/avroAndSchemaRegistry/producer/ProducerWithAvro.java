package org.intermediate.multiValueDataSend.avroAndSchemaRegistry.producer;

import com.kudadiri.project.Order;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerWithAvro {
    private static final Logger log = LoggerFactory.getLogger(ProducerWithAvro.class);

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "172.25.5.7:9092");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", KafkaAvroSerializer.class.getName());
        props.put("schema.registry.url", "http://172.25.5.7:8081");

        KafkaProducer<String, Order> producer = new KafkaProducer<>(props);

        try {
            Order order = Order.newBuilder()
                    .setOrderId("ORD-10001")
                    .setCustomer("Budi")
                    .setAmount(12.5)
                    .build();

            ProducerRecord<String, Order> record = new ProducerRecord<>(
                    "orders",
                    order.getOrderId().toString(),
                    order
            );

            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    log.info("Sent record: key={} value={} to partition {} offset {}",
                            record.key(), record.value(), metadata.partition(), metadata.offset());
                } else {
                    log.error("Error sending record", exception);
                }
            });

        } catch (Exception e) {
            log.error("Unexpected Error Appear: ", e);
        } finally {
            producer.flush();
            producer.close();
        }
    }
}
