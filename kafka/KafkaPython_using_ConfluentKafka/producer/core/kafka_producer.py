from confluent_kafka import Producer
import logging

class KafkaProducer:
    def __init__(self, config, key_serializer=None, value_serializer=None):
        self.logger = logging.getLogger("KafkaProducer")
        logging.basicConfig(level=logging.INFO)

        self.key_serializer = key_serializer
        self.value_serializer = value_serializer
        self.producer = Producer(config)

    def delivery_report(self, err, msg):
        if err is not None:
            self.logger.error(f"Delivery failed: {err}")
        else:
            self.logger.info(
                f"Delivery to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}"
            )
    
    def send(self, topic, key= None, value= None):
        if self.key_serializer:
            key = self.key_serializer(key)
        
        if self.value_serializer:
            value = self.value_serializer(value)
        
        try:
            self.producer.produce(
                topic= topic,
                key= key,
                value= value,
                
                callback= self.delivery_report
            )

            self.producer.poll(0)
        except BufferError:
            self.logger.warning("Buffer full, Flushing..")
            self.producer.flush()
            self.producer.produce(
                topic=topic,
                key=key,
                value=value,
                callback=self.delivery_report
            )
    
    def flush(self):
        self.logger.info("Delivery Pending messages...")
        self.producer.flush()