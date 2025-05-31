"""
Template Component main class.

"""

import csv
import io
import json
import logging
import time

import fastavro
from common.kafka_client import KafkaProducer
from configuration import Configuration, KeySource
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
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
        try:
            self.params = Configuration(**self.configuration.parameters)
            self._validate_stack_params()

            self.params.client_id = f"kbc-config-{self.environment_variables.config_row_id}" or "kbc-config-0"

            bootstrap_servers = ",".join(self.params.bootstrap_servers)
            self.client = self._init_client(debug, self.params, bootstrap_servers)

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
        except Exception as e:
            raise UserException(f"Error during run: {str(e)}")

        logging.info(f"Writing finished in {time.time() - start_time:.2f} seconds")

    def _validate_stack_params(self):
        image_parameters = self.configuration.image_parameters or {}
        allowed_hosts = [f"{host.get('host')}:{host.get('port')}" for host in image_parameters.get("allowed_hosts", [])]

        if allowed_hosts:
            for item in self.params.bootstrap_servers:
                if item not in allowed_hosts:
                    raise UserException(f"Host {item} is not allowed")

    def _init_avro_serializer(self):
        if not self.params.schema_str:
            raise UserException("Schema string is required for Avro serialization.")

        # Convert the schema string to a fastavro schema
        self.avro_schema = fastavro.schema.parse_schema(json.loads(self.params.schema_str))

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

    def write_line(self, row):
        topic = self.params.topic

        if self.params.key_source == KeySource.configuration:
            key = self.params.key
        else:
            key = row.get(self.params.key_column_name)

        if self.params.value_column_names:
            value = {col: row[col] for col in self.params.value_column_names}
        else:
            value = row

        serialized_value = self.serialize(value, topic)

        self.client.produce_message(topic, key, serialized_value)

    def serialize(self, value, topic):
        serialize_method = self.params.serialize.lower()

        if serialize_method == "avro":
            if self.params.schema_registry_url:
                converted_values = self._convert_types_for_avro(value, self.avro_schema)
                return self.serializer(converted_values, SerializationContext(topic, MessageField.VALUE))
            else:
                converted_values = self._convert_types_for_avro(value, self.avro_schema)
                out = io.BytesIO()
                fastavro.schemaless_writer(out, self.avro_schema, converted_values)
                return out.getvalue()
        elif serialize_method == "json":
            return json.dumps(value).encode("utf-8")
        else:
            return str(value).encode("utf-8")

    def _convert_types_for_avro(self, value: dict, schema: dict):
        converted_value = {}

        # For fastavro, we need to process the schema differently
        schema_fields = schema.get("fields", [])

        for field in schema_fields:
            field_name = field.get("name")

            # Skip fields not in the input data
            if field_name not in value:
                continue

            field_value = value[field_name]
            field_type = field.get("type")

            # Handle union types (represented as lists in fastavro schemas)
            if isinstance(field_type, list):
                # Use the first non-null type
                for type_option in field_type:
                    if type_option != "null":
                        field_type = type_option
                        break
                else:
                    field_type = "null"

            # Skip empty values
            if field_value is None or field_value == "":
                continue

            try:
                # Simple type casting based on Avro type
                if field_type == "string":
                    converted_value[field_name] = str(field_value)
                elif field_type == "int":
                    converted_value[field_name] = int(field_value)
                elif field_type == "long":
                    converted_value[field_name] = int(field_value)
                elif field_type == "boolean":
                    converted_value[field_name] = (
                        bool(int(field_value)) if field_value in ("0", "1") else field_value.lower() == "true"
                    )
                elif field_type == "float" or field_type == "double":
                    converted_value[field_name] = float(field_value)
                elif field_type == "bytes":
                    converted_value[field_name] = (
                        field_value.encode("utf-8") if isinstance(field_value, str) else field_value
                    )
                else:
                    # For unknown types, keep as string
                    converted_value[field_name] = str(field_value)
            except Exception as e:
                # If casting fails, just keep the original value
                logging.warning(f"Failed to cast {field_name}: {str(e)}")
                converted_value[field_name] = field_value

        return converted_value

    def _init_client(self, debug, params, bootstrap_servers):
        c = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            client_id=params.client_id,
            security_protocol=params.security_protocol,
            sasl_mechanisms=params.sasl_mechanism,
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
        bootstrap_servers = ",".join(params.bootstrap_servers)

        c = self._init_client(False, params, bootstrap_servers)
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
