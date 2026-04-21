from confluent_kafka import Producer, Consumer


def create_producer(bootstrap_servers: str) -> Producer:
    return Producer({"bootstrap.servers": bootstrap_servers})


def create_consumer(bootstrap_servers: str, group_id: str, topic: str) -> Consumer:
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "session.timeout.ms": 45000,
            "heartbeat.interval.ms": 10000,
        }
    )
    consumer.subscribe([topic])
    return consumer