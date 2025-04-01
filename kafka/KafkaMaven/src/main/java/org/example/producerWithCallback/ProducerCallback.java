package org.example.producerWithCallback;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.logging.log4j.Logger;

class ProducerCallback implements Callback {
    Logger logger;
    ProducerRecord<String, String> record;

    public ProducerCallback(Logger _logger, ProducerRecord<String, String> _record) {
        logger = _logger;
        record = _record;
    }

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (exception == null) {
            System.out.printf("Sent Data to Topic %s with: Key=%s, Value=%s to Partition=%d%n",
                metadata.topic(), record.key(), record.value(), metadata.partition());
        } else {
            logger.error("An error occurred while sending to Kafka:%n", exception);
        }
    }
}
