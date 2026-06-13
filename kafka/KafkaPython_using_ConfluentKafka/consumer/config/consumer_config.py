def get_consumer_config():
    return {
        "bootstrap.servers": "172.25.5.7:9092",
        "group.id": "consumer-python",
        "auto.offset.reset": "earliest"
        
    }