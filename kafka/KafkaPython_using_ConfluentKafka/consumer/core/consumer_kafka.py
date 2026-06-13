from core.consumer_service import KafkaBaseConsumer
from core.deserializer import default_deserializer

class ConsumerKafka:
    def __init__(self, config, topic):
        self.consumer = KafkaBaseConsumer(config, [topic])
        
        self.log = self.consumer.logger
        self.topic = topic
    
    def process_message(self, msg):
        key = default_deserializer(msg.key())
        value = default_deserializer(msg.value())

        self.log.info(f"[Received] {msg.topic()} | Key={key} | Value={value}")
    
    def run(self):
        self.log.info("[Consumer] Listening...")

        try:
            while True:
                msg = self.consumer.poll_message()

                if msg:
                    self.process_message(msg)
                    self.consumer.commit()
        
        except KeyboardInterrupt:
            self.log.info("[Consumer] Stopped Manually")

        finally:
            self.consumer.close()