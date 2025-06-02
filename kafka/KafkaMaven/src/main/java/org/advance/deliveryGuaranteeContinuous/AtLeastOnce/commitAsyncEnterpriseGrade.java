package org.advance.deliveryGuaranteeContinuous.AtLeastOnce;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.util.Properties;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class commitAsyncEnterpriseGrade {
    private static final Logger log = LoggerFactory.getLogger(commitAsyncEnterpriseGrade.class.getName());
    private static final int MAX_RETRIES = 3;
    private static final long RETRY_DELAY_MS = 1000;

    private static final Map<TopicPartition, Integer> retryCountMap = new ConcurrentHashMap<>();
    private static final Map<TopicPartition, Long> nextRetryAllowedAt = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("group.id", "Java");
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Detect Shutdown... consumer.wakeup() is Running...");
            consumer.wakeup();

            try {
                mainThread.join();
            } catch(InterruptedException e) {
                e.printStackTrace();
            }
        }));
        try {
            consumer.subscribe(Collections.singletonList("finance-transactions"));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                boolean hasPendingRetries = false;

                for (TopicPartition partition : records.partitions()) {
                    long now = System.currentTimeMillis();
                    if (nextRetryAllowedAt.containsKey(partition) && now < nextRetryAllowedAt.get(partition)) {
                        hasPendingRetries = true;
                        continue; // Skip partition temporarily until retry time is reached
                    }

                    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                    boolean partitionProcessedSuccessfully = true;

                    for (ConsumerRecord<String, String> record : partitionRecords) {
                        try {
                            processTransaction(record);
                            // Reset retry counters if successful
                            retryCountMap.remove(partition);
                            nextRetryAllowedAt.remove(partition);
                        } catch (Exception ex) {
                            partitionProcessedSuccessfully = false;
                            int currentRetries = retryCountMap.getOrDefault(partition, 0);
                            log.warn("Failed to process record partition={}, offset={} (attempt {}/{}) - {}",
                                    partition.partition(), record.offset(), currentRetries + 1, MAX_RETRIES, ex.getMessage());

                            if (currentRetries < MAX_RETRIES) {
                                retryCountMap.put(partition, currentRetries + 1);
                                nextRetryAllowedAt.put(partition, now + RETRY_DELAY_MS);
                                consumer.seek(partition, record.offset());
                                hasPendingRetries = true;
                                break; // Stop at this record and wait for retry
                            } else {
                                log.error("Max retries exceeded for partition={}, offset={}. Skipping to next record.",
                                        partition.partition(), record.offset());
                                retryCountMap.remove(partition);
                                nextRetryAllowedAt.remove(partition);
                                // Move to next record (could also store failed records for later processing)
                            }
                        }
                    }

                    if (partitionProcessedSuccessfully && !partitionRecords.isEmpty()) {
                        // Commit the last processed offset for this partition
                        long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                        consumer.commitSync(Collections.singletonMap(
                                partition,
                                new OffsetAndMetadata(lastOffset + 1)
                        ));
                        log.debug("Committed offset {} for partition {}", lastOffset + 1, partition);
                    }
                }

                if (!hasPendingRetries) {
                    // Additional async commit for efficiency (though sync commits already happened per partition)
                    consumer.commitAsync((offsets, exception) -> {
                        if (exception != null) {
                            log.warn("Async commit failed", exception);
                        }
                    });
                }
            }
        } catch (WakeupException e) {
            log.info("Shutdown triggered...");
        } catch (Exception e) {
            log.error("Unexpected error in consumer", e);
        } finally {
            try {
                consumer.commitSync(); // Final sync commit before shutdown
            } catch (Exception e) {
                log.warn("Failed to commit offsets during shutdown", e);
            }
            consumer.close();
            log.info("Consumer closed.");
        }
    }

    private static void processTransaction(ConsumerRecord<String, String> record) {
        if (record == null) {
            log.warn("Null record received in transaction processing");
            return;
        } else if (record.key() == null || record.key().trim().isEmpty()) {
            log.warn("Empty transaction key detected. Value: {}", record.value());
            return;
        }

        log.info("Processing transaction: Key={}, Value={}", record.key(), record.value());
    }
}