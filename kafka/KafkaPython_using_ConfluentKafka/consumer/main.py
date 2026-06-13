from config.consumer_config import get_consumer_config
from core.consumer_kafka import ConsumerKafka

if __name__ == "__main__":
    topic = "cars"

    app = ConsumerKafka(get_consumer_config(), topic)
    app.run()