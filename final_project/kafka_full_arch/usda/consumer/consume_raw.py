import json

from confluent_kafka import KafkaException

from common.config import get_env
from common.kafka_utils import create_consumer
from usda.constants import USDA_FOODS_RAW_TOPIC, USDA_GROUP_ID


def main():
    bootstrap_servers = get_env("KAFKA_BOOTSTRAP_SERVERS", required=True)

    consumer = create_consumer(bootstrap_servers, USDA_GROUP_ID, USDA_FOODS_RAW_TOPIC)
    print(f"Subscribed to topic: {USDA_FOODS_RAW_TOPIC}")

    max_messages = 10
    seen = 0

    try:
        while seen < max_messages:
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
            print(f"fdcId: {record.get('fdcId')}")
            print(f"Description: {record.get('description')}")
            print(f"Data type: {record.get('dataType')}")
            print(f"Brand owner: {record.get('brandOwner')}")

            seen += 1

        print(f"Read {seen} messages and stopping.")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()