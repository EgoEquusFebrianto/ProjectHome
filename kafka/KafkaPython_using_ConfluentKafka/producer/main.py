from config.producer_config import producer_config
from core.kafka_producer import KafkaProducer
from core.serializer import default_serializer

if __name__ == "__main__":
    cars = {
        "Toyota": "Avanza",
        "Honda": "Civic",
        "Suzuki": "Ertiga",
        "Mitsubishi": "Xpander",
        "Nissan": "X-Trail",
        "BMW": "X1",
        "Mercedes-Benz": "C-Class",
        "Hyundai": "Creta",
        "Kia": "Seltos",
        "Ford": "Ranger"
    }

    producer = KafkaProducer(
        config= producer_config,
        key_serializer= default_serializer,
        value_serializer= default_serializer
    )

    for key, value in cars.items():
        producer.send(
            "cars",
            key= key,
            value= value
        )
    
    producer.flush()