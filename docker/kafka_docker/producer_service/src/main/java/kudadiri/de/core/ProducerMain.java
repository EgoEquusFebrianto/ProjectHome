package kudadiri.de.core;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerMain {
    private static final Logger log = LoggerFactory.getLogger(ProducerMain.class.getSimpleName());

    public static void main(String[] args) {    
        Map<String, List<String>> cars = Map.of(
            "Toyota", List.of("Camry", "Corolla"),
            "Honda", List.of("Civic", "Accord"),
            "Ford", List.of("Mustang", "Explorer"),
            "Chevrolet", List.of("Malibu", "Tahoe"),
            "BMW", List.of("X5", "M3"),
            "Mercedes-Benz", List.of("C-Class", "E-Class"),
            "Audi", List.of("A4", "Q5"),
            "Hyundai", List.of("Elantra", "Tucson"),
            "Nissan", List.of("Altima", "Rogue"),
            "Volkswagen", List.of("Golf", "Passat")
        );
        
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", StringSerializer.class.getName());
        
        String topic = "wikimedia";
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        
        try (producer) {
            for (Map.Entry<String, List<String>> entries: cars.entrySet()) {
                for (String entry : entries.getValue()) {
                    ProducerRecord<String, String> record = new ProducerRecord<>(topic, entries.getKey(), entry);

                    producer.send(record, ((metadata, e) -> {
                        if (e != null) {
                            log.info("Unexpected Error happenen: {}", e.getMessage());
                        } else {
                            log.info(
                                "Company={}, Model={}, Topic={}, Partition={}",
                                record.key(),
                                record.value(),
                                metadata.topic(),
                                metadata.partition()
                            );
                        }
                    }));
                }
            }
        }
        
        producer.flush();
    }
}