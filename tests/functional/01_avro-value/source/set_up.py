
from datadirtest import TestDataDir
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.admin import AdminClient
import os

def run(context: TestDataDir):
    os.environ['KBC_PROJECTID'] = '00001'
    os.environ['KBC_CONFIGROWID'] = '00001'

    admin_client = AdminClient({
        'bootstrap.servers': 'broker:9092',
        'security.protocol': 'PLAINTEXT'})

    admin_client.delete_topics(topics=["avro-value"])

    schema_reg_client = SchemaRegistryClient({'url': 'http://schema-registry:8081'})

    producer = Producer({
        'bootstrap.servers': 'broker:9092',
        'security.protocol': 'PLAINTEXT',
        'acks': 0,
        'batch.size': 1000000,
        'linger.ms': 10000,
        'compression.type': 'none',
        'enable.idempotence': "False",
    })

    name = "avro-value"

    for i in range(15):
        schema_str = '''{
                  "type": "record",
                  "name": "User",
                  "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "key", "type": "string"},
                    {"name": "text", "type": "string"}
                  ]
                }'''

        avro_serializer = AvroSerializer(schema_reg_client, schema_str)

        value = avro_serializer(
            {"id": i, "key": f"{name}-111", "text": f"Test message {i}"},
            SerializationContext(name, MessageField.VALUE)
        )

        producer.poll(0)
        producer.produce(topic=name, value=value, key=f"key{str(i % 4)}")  # key to distribute messages to partitions
        producer.flush()