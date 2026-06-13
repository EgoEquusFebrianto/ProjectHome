from confluent_kafka import Consumer, KafkaException, KafkaError
import logging

class KafkaBaseConsumer:
    def __init__(self, config: dict, topics: list[str]):
        self.consumer = Consumer(config)
        self.consumer.subscribe(topics)
        self.logger = logging.getLogger("ConsumerKafka")
        logging.basicConfig(level=logging.INFO)

        self.logger.info(f"[Consumer] subscribed to topics: {topics}")
    
    def poll_message(self):
        msg = self.consumer.poll(1)

        if msg is None:
            return None
        
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                self.logger.warning(f"End of Partition: {msg.topic()} [{msg.partition()}]")
                return None
            else:
                raise KafkaException(msg.error())
        
        return msg
    
    def commit(self):
        self.consumer.commit(asynchronous=False)

    def close(self):
        self.consumer.close()
        self.logger.info("[Consumer] Closed connection")