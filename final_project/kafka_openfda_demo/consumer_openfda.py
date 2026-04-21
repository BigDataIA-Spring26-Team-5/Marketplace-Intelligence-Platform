import json
import os

from confluent_kafka import Consumer, KafkaException
from dotenv import load_dotenv

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "openfda_raw")


def main():
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": "openfda-consumer-group",
            "auto.offset.reset": "earliest",
            "session.timeout.ms": 45000,
            "heartbeat.interval.ms": 10000,
        }
    )

    consumer.subscribe([TOPIC])
    print(f"Subscribed to topic: {TOPIC}")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                raise KafkaException(msg.error())

            key = msg.key().decode("utf-8") if msg.key() else None
            value = msg.value().decode("utf-8") if msg.value() else None
            record = json.loads(value) if value else {}

            print("-" * 80)
            print(f"Topic: {msg.topic()}, Partition: {msg.partition()}, Offset: {msg.offset()}")
            print(f"Key: {key}")
            print(f"Recall number: {record.get('recall_number')}")
            print(f"Firm: {record.get('recalling_firm')}")
            print(f"Product: {record.get('product_description')}")
            print(f"Status: {record.get('status')}")

    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()