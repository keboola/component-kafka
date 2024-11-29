
from datadirtest import TestDataDir
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.admin import AdminClient
import time

def run(context: TestDataDir):
    admin_client = AdminClient({
        'bootstrap.servers': 'broker:9092',
        'security.protocol': 'PLAINTEXT'})

    admin_client.delete_topics(topics=["avro-columns"])

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

    name = "avro-columns"

    for i in range(15):
        schema_str = '''{
                  "type": "record",
                  "name": "User",
                  "fields": [
                    {"name": "col_boolean", "type": "boolean"},
                    {"name": "col_int", "type": "int"},
                    {"name": "col_long", "type": "long"},
                    {"name": "col_float", "type": "float"},
                    {"name": "col_double", "type": "double"},
                    {"name": "col_bytes", "type": "bytes"},
                    {"name": "col_string", "type": "string"}
                  ]
                }'''

        avro_serializer = AvroSerializer(schema_reg_client, schema_str)

        value = avro_serializer(
            {"col_boolean": True, "col_int": i, "col_long": 1234567890, "col_float": 123.45,
             "col_double": 123456.7891234568, "col_bytes": b"test", "col_string": "Test message"},
            SerializationContext(name, MessageField.VALUE)
        )

        producer.poll(0)
        producer.produce(topic=name, value=value, key=f"key{str(i % 4)}")  # key to distribute messages to partitions
        producer.flush()

        time.sleep(3)
