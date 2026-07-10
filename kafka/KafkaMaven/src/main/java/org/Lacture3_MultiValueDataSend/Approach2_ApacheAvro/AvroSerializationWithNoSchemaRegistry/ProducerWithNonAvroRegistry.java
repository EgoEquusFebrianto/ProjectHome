package org.Lacture3_MultiValueDataSend.Approach2_ApacheAvro.AvroSerializationWithNoSchemaRegistry;

import com.kudadiri.project.Testing;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;

public class ProducerWithNonAvroRegistry {
    private static final Logger log = LoggerFactory.getLogger(ProducerWithNonAvroRegistry.class.getName());

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "172.25.5.7:9092");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", AvroSerializerCustom.class.getName());

        KafkaProducer<String, Testing> producer = new KafkaProducer<>(props);
        Testing data = Testing.newBuilder().setId(22).setName("Febrianto").build();
        ProducerRecord<String, Testing> record = new ProducerRecord<>("name", "UDIN", data);
        producer.send(record, (metadata, e) -> {
            if (e != null) {
                log.error("An error occurred while sending to Kafka: {}", e.getMessage());
            } else {
                log.info("Successfully sent record: Key={}, Value={} | Partition={}, Offset={}",
                        record.key(), record.value(), metadata.partition(), metadata.offset()
                );
            }
        });
        producer.close();
    }
}
