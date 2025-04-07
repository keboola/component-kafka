"""
Template Component main class.

"""

import csv
import io
import json
import logging
import time

import fastavro
from common.src.kafka_client import KafkaProducer

# from components.common.src.kafka_client import KafkaProducer
from configuration import Configuration
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from fastavro.schema import parse_schema
from keboola.component.base import ComponentBase, sync_action
from keboola.component.exceptions import UserException
from keboola.component.sync_actions import SelectElement


class Component(ComponentBase):
    def __init__(self):
        self.params = None
        self.client = None
        self.serializer = None
        self.avro_schema = None
        self.schema_registry_client = None
        super().__init__()

    def run(self, debug=False):
        self.params = Configuration(**self.configuration.parameters)
        self._validate_stack_params()

        self.params.client_id = f"kbc-config-{self.environment_variables.config_row_id}" or "kbc-config-0"

        servers = ",".join(self.params.servers)
        self.client = self._init_client(debug, self.params, servers)

        if self.params.serialize == "avro":
            self._init_avro_serializer()

        input_tables = self.get_input_tables_definitions()

        if len(input_tables) != 1:
            raise UserException("Exactly one input table is expected.")

        input_table = input_tables[0]

        logging.info(f"Writing data from table: {input_table.name} to the topic: {self.params.topic}")

        start_time = time.time()

        with open(input_table.full_path, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                self.write_line(row)

        logging.info("Writing finished.")
        logging.info(f"Total time taken: {time.time() - start_time:.2f} seconds")

    def _validate_stack_params(self):
        image_parameters = self.configuration.image_parameters or {}
        allowed_hosts = [f"{host.get('host')}:{host.get('port')}" for host in image_parameters.get("allowed_hosts", [])]

        if allowed_hosts:
            for item in self.params.servers:
                if item not in allowed_hosts:
                    raise UserException(f"Host {item} is not allowed")

    def _init_avro_serializer(self):
        # Convert the schema string to a fastavro schema
        self.avro_schema = parse_schema(json.loads(self.params.schema_str))

        if self.params.schema_registry_url:
            config = self.params.schema_registry_extra_params
            config["url"] = self.params.schema_registry_url
            self.schema_registry_client = SchemaRegistryClient(config)

            if self.params.schema_str:
                # Let AvroSerializer handle schema registration automatically
                self.serializer = AvroSerializer(self.schema_registry_client, self.params.schema_str)
            else:
                try:
                    subject_name = f"{self.params.topic}-value"
                    metadata = self.schema_registry_client.get_latest_version(subject_name)
                    schema_str = metadata.schema.schema_str
                    self.serializer = AvroSerializer(self.schema_registry_client, schema_str)
                except Exception as e:
                    raise UserException(f"No schema string provided and could not fetch from registry: {str(e)}")
        elif self.params.schema_str:
            # No special serializer instantiation needed for fastavro
            pass
        else:
            raise UserException("Schema Registry URL or schema string must be provided for Avro serialization.")

    def write_line(self, row):
        topic = self.params.topic
        key = row.get(self.params.key_column_name, None)

        if self.params.value_column_names:
            value = {col: row[col] for col in self.params.value_column_names}
        else:
            value = row

        serialized_value = self.serialize(value, topic)

        self.client.produce_message(topic, key, serialized_value)

    def serialize(self, value, topic):
        serialize_method = self.params.serialize.lower()

        # # Ensure order_id is converted to integer if present
        # if "order_id" in value and value["order_id"] is not None and value["order_id"] != "":
        #     try:
        #         value["order_id"] = int(value["order_id"])
        #     except (ValueError, TypeError):
        #         logging.warning(f"Could not convert order_id value '{value['order_id']}' to integer")
        #         # If conversion fails, provide a default value or remove the field
        #         # depending on whether it's required in your schema
        #         value["order_id"] = 0  # Default value, adjust as needed

        if serialize_method == "avro":
            if self.params.schema_registry_url:
                return self.serializer(value, SerializationContext(topic, MessageField.VALUE))
            else:
                out = io.BytesIO()
                fastavro.schemaless_writer(out, self.avro_schema, value)
                return out.getvalue()
        elif serialize_method == "json":
            return json.dumps(value).encode("utf-8")
        else:
            return str(value).encode("utf-8")

    def _init_client(self, debug, params, servers):
        c = KafkaProducer(
            servers=servers,
            client_id=params.client_id,
            security_protocol=params.security_protocol,
            sasl_mechanisms=params.sasl_mechanisms,
            username=params.username,
            password=params.password,
            ssl_ca=params.ssl_ca,
            ssl_key=params.ssl_key,
            ssl_certificate=params.ssl_certificate,
            logger=logging.getLogger(),
            config_params=params.kafka_extra_params,
            debug=debug,
        )
        return c

    @sync_action("list_topics")
    def list_topics(self):
        params = Configuration(**self.configuration.parameters)
        servers = ",".join(params.servers)

        c = self._init_client(False, params, dict(), servers)
        topics = c.list_topics()
        topics_names = [SelectElement(topics.get(t).topic) for t in topics]

        return topics_names


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
