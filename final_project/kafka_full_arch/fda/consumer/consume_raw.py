import json

from confluent_kafka import KafkaException

from common.config import get_env
from common.kafka_utils import create_consumer
from fda.constants import FDA_GROUP_ID, FDA_RAW_TOPIC


def main():
    bootstrap_servers = get_env("KAFKA_BOOTSTRAP_SERVERS", required=True)

    consumer = create_consumer(bootstrap_servers, FDA_GROUP_ID, FDA_RAW_TOPIC)
    print(f"Subscribed to topic: {FDA_RAW_TOPIC}")

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