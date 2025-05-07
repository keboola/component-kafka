from datadirtest import TestDataDir
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.admin import AdminClient
import os


def run(context: TestDataDir):
    os.environ["KBC_PROJECTID"] = "00002"
    os.environ["KBC_CONFIGROWID"] = "00002"

    admin_client = AdminClient({"bootstrap.servers": "broker:9092", "security.protocol": "PLAINTEXT"})

    admin_client.delete_topics(topics=["text-message"])

    producer = Producer(
        {
            "bootstrap.servers": "broker:9092",
            "security.protocol": "PLAINTEXT",
            "acks": 0,
            "batch.size": 1000000,
            "linger.ms": 10000,
            "compression.type": "none",
            "enable.idempotence": "False",
        }
    )

    name = "text-message"

    for i in range(15):
        producer.poll(0)
        text = f"Test message {i}"
        value = text.encode("utf-8")
        producer.produce(topic=name, value=value, key=f"key{str(i % 4)}")  # key to distribute messages to partitions
        producer.flush()
