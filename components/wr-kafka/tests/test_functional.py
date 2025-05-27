import csv
import io
import json
import os
import time
import unittest

import fastavro
import polars
from component import Component
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.admin import AdminClient
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext


class TestKafkaWriter(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        """
        Inti consumer and deletes all topics in the Kafka cluster that start with 'test-'.
        """
        cls.consumer = Consumer(
            {"bootstrap.servers": "broker:9092", "group.id": "test-consumer-group", "auto.offset.reset": "earliest"}
        )

        admin_client = AdminClient({"bootstrap.servers": "broker:9092"})

        # Get list of all topics
        cluster_metadata = admin_client.list_topics(timeout=10)
        all_topics = cluster_metadata.topics

        # Filter for topics starting with 'test-'
        test_topics = [topic for topic in all_topics if topic.startswith("test-")]

        if test_topics:
            try:
                admin_client.delete_topics(test_topics)
                time.sleep(2)  # Allow time for topic deletion to complete
                print(f"Deleted {len(test_topics)} test topics")
            except Exception as e:
                print(f"Error deleting test topics: {e}")

    def test_avro_with_registry(self):
        data_dir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "test-data", "avro-schema-registry", "source"
        )
        os.environ["KBC_DATADIR"] = data_dir

        topic = "test-avro-schema-registry"

        component = Component()
        component.execute_action()
        time.sleep(2)

        self.consumer.subscribe([topic])

        # Get schema for deserialization
        schema_registry_client = SchemaRegistryClient({"url": "http://schema-registry:8081"})
        subject_name = f"{topic}-value"
        schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str
        deserializer = AvroDeserializer(schema_registry_client, schema_str)

        # Read expected output data for comparison
        expected_output_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "test-data", "avro-schema-registry", "expected", "out.csv"
        )

        expected = polars.read_csv(expected_output_path).to_dicts()

        # Consume messages and verify
        consumed_messages = []
        for _ in range(len(expected)):
            msg = self.consumer.poll()
            if msg is None or msg.error():
                if msg and msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"Consumer error: {msg.error()}")
                break

            value = deserializer(msg.value(), SerializationContext(topic, MessageField.VALUE))
            consumed_messages.append(value)

        self.assertEqual(len(expected), len(consumed_messages))

        consumed_messages_sorted = sorted(consumed_messages, key=lambda x: x["row_number"])

        for i, expected in enumerate(expected):
            self.assertEqual(expected, consumed_messages_sorted[i])

    def test_avro_without_registry(self):
        data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "test-data", "avro", "source")
        os.environ["KBC_DATADIR"] = data_dir
        topic = "test-avro"

        component = Component()
        component.execute_action()
        time.sleep(2)

        self.consumer.subscribe([topic])

        # Load schema file for deserialization with fastavro
        schema_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "test-data", "avro", "source", "config.json"
        )
        with open(schema_path, "r") as f:
            config = json.load(f).get("parameters")
            schema = json.loads(config.get("schema_str"))

        # Read expected output data for comparison
        expected_output_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "test-data", "avro", "expected", "out.csv"
        )

        expected = polars.read_csv(expected_output_path).to_dicts()

        # Consume messages and verify
        consumed_messages = []
        for _ in range(len(expected)):
            msg = self.consumer.poll()
            if msg is None or msg.error():
                if msg and msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"Consumer error: {msg.error()}")
                break

            # Deserialize using fastavro
            bytes_io = io.BytesIO(msg.value())
            value = fastavro.schemaless_reader(bytes_io, schema)
            consumed_messages.append(value)

        self.assertEqual(len(expected), len(consumed_messages))

        consumed_messages_sorted = sorted(consumed_messages, key=lambda x: x["row_number"])

        for i, expected in enumerate(expected):
            self.assertEqual(expected, consumed_messages_sorted[i])

    def test_json(self):
        data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "test-data", "json", "source")
        os.environ["KBC_DATADIR"] = data_dir
        topic = "test-json"

        component = Component()
        component.execute_action()
        time.sleep(2)

        self.consumer.subscribe([topic])

        expected_output_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "test-data", "json", "expected", "out.csv"
        )

        expected = polars.read_csv(expected_output_path, infer_schema=False).to_dicts()

        # Consume messages and verify
        consumed_messages = []
        for _ in range(len(expected)):
            msg = self.consumer.poll(timeout=5.0)
            if msg is None or msg.error():
                if msg and msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"Consumer error: {msg.error()}")
                break

            value = json.loads(msg.value().decode("utf-8"))

            consumed_messages.append(value)

        self.assertEqual(
            len(expected), len(consumed_messages), "Number of consumed messages doesn't match expected count"
        )

        consumed_messages_sorted = sorted(consumed_messages, key=lambda x: x["row_number"])

        for i, expected_item in enumerate(expected):
            self.assertEqual(
                expected_item, consumed_messages_sorted[i], f"Message at index {i} doesn't match expected data"
            )

    def test_text(self):
        data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "test-data", "text", "source")
        os.environ["KBC_DATADIR"] = data_dir
        topic = "test-text"

        component = Component()
        component.execute_action()
        time.sleep(2)

        self.consumer.subscribe([topic])

        # Read expected output data for comparison
        expected_output_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "test-data", "text", "expected", "out.csv"
        )

        expected = polars.read_csv(expected_output_path, infer_schema=False).to_dicts()

        # Consume messages and verify
        consumed_messages = []
        for _ in range(len(expected)):
            msg = self.consumer.poll(timeout=5.0)
            if msg is None or msg.error():
                if msg and msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"Consumer error: {msg.error()}")
                break

            # Decode the text message - which is a string representation of a dictionary
            text_value = msg.value().decode("utf-8")

            json_str = text_value.replace("'", '"')
            dict_value = json.loads(json_str)
            consumed_messages.append(dict_value)

        self.assertEqual(
            len(expected), len(consumed_messages), "Number of consumed messages doesn't match expected count"
        )

        # Sort by row_number for comparison
        consumed_messages_sorted = sorted(consumed_messages, key=lambda x: x["row_number"])

        for i, expected_item in enumerate(expected):
            self.assertEqual(
                expected_item, consumed_messages_sorted[i], f"Message at index {i} doesn't match expected data"
            )

    def test_text_key_from_config(self):
        data_dir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "test-data", "text-key-from-config", "source"
        )
        os.environ["KBC_DATADIR"] = data_dir
        topic = "test-text-key-from-config"

        component = Component()
        component.execute_action()
        time.sleep(2)

        self.consumer.subscribe([topic])

        # Read expected output data for comparison
        expected_output_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "test-data", "text-key-from-config", "expected", "out.csv"
        )

        expected = polars.read_csv(expected_output_path, infer_schema=False).to_dicts()

        # Consume messages and verify
        consumed_messages = []
        consumed_message_keys = []
        for _ in range(len(expected)):
            msg = self.consumer.poll(timeout=5.0)
            if msg is None or msg.error():
                if msg and msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"Consumer error: {msg.error()}")
                break

            # Decode the text message - which is a string representation of a dictionary
            text_value = msg.value().decode("utf-8")

            json_str = text_value.replace("'", '"')
            dict_value = json.loads(json_str)
            consumed_messages.append(dict_value)
            consumed_message_keys.append(msg.key().decode("utf-8"))

        time.sleep(1)

        self.assertEqual(
            len(expected), len(consumed_messages), "Number of consumed messages doesn't match expected count"
        )

        # Sort by row_number for comparison
        consumed_messages_sorted = sorted(consumed_messages, key=lambda x: x["row_number"])

        for i, expected_item in enumerate(expected):
            self.assertEqual(
                expected_item, consumed_messages_sorted[i], f"Message at index {i} doesn't match expected data"
            )

        for key in consumed_message_keys:
            self.assertEqual(key, "config")


if __name__ == "__main__":
    unittest.main()
