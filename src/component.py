"""
Template Component main class.

"""

import logging
import os
import csv
import json
from collections import OrderedDict
import polars

from keboola.component.base import ComponentBase, sync_action
from keboola.component.sync_actions import SelectElement, ValidationResult, MessageType
from keboola.component.exceptions import UserException
from keboola.component.dao import ColumnDefinition, BaseType

from configuration import Configuration

from kafka.client import KafkaConsumer

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

# global constants
RESULT_PK = ['topic', 'timestamp_type', 'timestamp', 'partition', 'offset', 'key']
RESULT_COLS = ['topic', 'timestamp_type', 'timestamp', 'partition', 'offset', 'key', 'value']
RESULT_COLS_DTYPES = ['string', 'string', 'timestamp', 'int', 'int', 'string', 'string']


class Component(ComponentBase):

    def __init__(self):
        self.params = None
        self.client = None
        self.topics = dict()
        self.columns = dict()
        self.latest_offsets = dict()
        super().__init__()

    def run(self, debug=False):
        """
        Main execution code

        TODO - statistics when DEBUG in conf dict:
        stats_cb(json_str): Callback for statistics data. This callback is triggered by poll() or
        flush every statistics.interval.ms (needs to be configured separately).
         Function argument json_str is a str instance of a JSON document containing
         statistics data. This callback is served upon calling client.poll() or producer.flush().
         See https://github.com/edenhill/librdkafka/wiki/Statistics” for more information.
        """

        self.params = Configuration(**self.configuration.parameters)
        self._validate_stack_params()

        self.params.group_id = f"kbc-proj-{self.environment_variables.project_id}" or "kbc-proj-0"
        self.params.client_id = f"kbc-config-{self.environment_variables.config_row_id}" or "kbc-config-0"

        # Generating a string out of the list
        servers = ",".join(self.params.servers)

        self.columns = self.get_state_file().get("columns", dict())
        self.latest_offsets = self.get_state_file().get("prev_offsets", dict())

        self.client = self._init_client(debug, self.params, self.latest_offsets, servers)

        logging.info("Extracting data from topics {0}".format(self.params.topics))

        for topic in self.params.topics:
            msg_cnt, res_file_folder, schema = self.consume_topic(topic)
            self.topics[topic] = {'msg_cnt': msg_cnt, 'res_file_folder': res_file_folder, 'schema': schema}

        # Store previous offsets and columns
        state_dict = {"prev_offsets": self.latest_offsets, "columns": self.columns}
        self.write_state_file(state_dict)
        logging.info("Offset file stored.")

        self.produce_manifest()
        logging.info("Extraction finished.")

    def _validate_stack_params(self):
        image_parameters = self.configuration.image_parameters or {}
        allowed_hosts = [f"{host.get('host')}:{host.get('port')}" for host in image_parameters.get('allowed_hosts', [])]

        if allowed_hosts:
            for item in self.params.servers:
                if item not in allowed_hosts:
                    raise UserException(f"Host {item} is not allowed")

    def produce_manifest(self):
        for topic, consumed in self.topics.items():

            schema = OrderedDict()
            for col, dtype in zip(RESULT_COLS, RESULT_COLS_DTYPES):
                schema[col] = ColumnDefinition(data_types=self.convert_dtypes(dtype))

            if consumed.get('schema'):
                del schema['value']

            for col in consumed.get('schema'):
                schema[col.get('name')] = ColumnDefinition(data_types=self.convert_dtypes(col.get('type')))

            # Produce final sliced table manifest
            if consumed['msg_cnt'] > 0:
                logging.info(F'Fetched {consumed['msg_cnt']} messages from topic - {topic}')
                out_table = self.create_out_table_definition(consumed['res_file_folder'], is_sliced=True,
                                                             primary_key=RESULT_PK, schema=schema,
                                                             incremental=True)

                self.write_manifest(out_table)
            else:
                logging.info('No new messages found!')

    def consume_topic(self, topic):

        self.columns.setdefault(topic, RESULT_COLS)

        deserializer = self.get_deserializer()

        res_file_folder = os.path.join(self.tables_out_path, topic)
        msg_cnt = 0
        last_message = None
        dtypes = []
        for msg in self.client.consume_message_batch(topic):
            if msg is None:
                break
            if msg.error():
                logging.error("Consumer error: {}".format(msg.error()))
                continue

            extracted_data, last_message = self.get_message_data(deserializer, last_message, msg, topic)

            filename = (("p{0}-{1}.csv").format(
                msg.partition(),
                msg.offset() // 10_000,
            ))

            logging.debug(F'Received message: {extracted_data}')

            # Save data as a sliced table file in defined folder
            self.save_file(extracted_data, os.path.join(res_file_folder, filename), topic)
            msg_cnt += 1

            print(msg.partition())

            if msg.topic() not in self.latest_offsets:
                self.latest_offsets[msg.topic()] = {}

            self.latest_offsets[msg.topic()]['p' + str(msg.partition())] = msg.offset()

        if self.params.deserialize == 'avro' and self.params.flatten_message_value_columns:
            dtypes = self.get_topic_dtypes(last_message)

        return msg_cnt, res_file_folder, dtypes

    def get_message_data(self, deserializer, last_message, msg, topic):
        if self.params.deserialize == 'avro':
            value = deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
        else:
            value = msg.value().decode('utf-8')
        if self.params.freeze_timestamp:  # freeze for datadir tests
            timestamp = 1732104020556
        else:
            timestamp = msg.timestamp()[1]
        extracted_data = {
            'topic': msg.topic(),
            'timestamp_type': msg.timestamp()[0],
            'timestamp': timestamp,
            'partition': msg.partition(),
            'offset': msg.offset(),
            'key': msg.key()}
        if self.params.flatten_message_value_columns:
            self.safe_update(extracted_data, value)
            self.columns[topic] = list(extracted_data.keys())
            last_message = msg.value()  # to get dtypes
        else:
            extracted_data['value'] = value
        return extracted_data, last_message

    def get_deserializer(self):
        deserializer = None
        if self.params.deserialize == 'avro':
            if self.params.schema_registry_url:
                config = self.params.schema_registry_extra_params
                config['url'] = self.params.schema_registry_url
                schema_registry_client = SchemaRegistryClient(config)
                deserializer = AvroDeserializer(schema_registry_client)
            elif self.params.schema_str:
                deserializer = AvroDeserializer(self.params.schema_str)
            else:
                raise ValueError("Schema Registry URL or schema string must be provided for Avro deserialization.")
        return deserializer

    def get_topic_dtypes(self, message_value: str):
        schema = None
        if self.params.deserialize == 'avro':
            if self.params.schema_registry_url:
                config = self.params.schema_registry_extra_params
                config['url'] = self.params.schema_registry_url
                schema_registry_client = SchemaRegistryClient(config)
                schema_id = int.from_bytes(message_value[1:5])
                schema = json.loads(schema_registry_client.get_schema(schema_id).schema_str).get('fields')

            elif self.params.schema_str:
                schema = json.loads(self.params.schema_str).get('fields')

        return schema

    def convert_dtypes(self, dtype: str = 'string'):
        match dtype:
            case 'boolean':
                base_type = BaseType.boolean()
            case 'int':
                base_type = BaseType.integer()
            case 'float':
                base_type = BaseType.float()
            case 'double':
                base_type = BaseType.float()
            case _:
                base_type = BaseType.string()

        return base_type

    def _init_client(self, debug, params, prev_offsets, servers):
        c = KafkaConsumer(servers=servers,
                          group_id=params.group_id,
                          client_id=params.client_id,
                          security_protocol=params.security_protocol,
                          sasl_mechanisms=params.sasl_mechanisms,
                          username=params.username,
                          password=params.password,
                          ssl_ca=params.ssl_ca,
                          ssl_key=params.ssl_key,
                          ssl_certificate=params.ssl_certificate,
                          logger=logging.getLogger(),
                          start_offset=prev_offsets,
                          config_params=params.kafka_extra_params,
                          debug=debug)
        return c

    def safe_update(self, extracted_data, value):
        for key, val in value.items():
            if key in extracted_data:
                extracted_data[f"value_{key}"] = val
            else:
                extracted_data[key] = val

    def save_file(self, line, filename, topic):
        """
        Save text as file
        """
        logging.info(F'Writing file {filename}')

        if not os.path.exists(os.path.dirname(filename)):
            os.makedirs(os.path.dirname(filename))
        try:
            with open(filename, 'a') as file:
                writer = csv.DictWriter(file, fieldnames=self.columns[topic])
                writer.writerow(line)
            logging.info("File saved.")
        except Exception as e:
            logging.error("Could not save file! exit.", e)

    @sync_action("list_topics")
    def list_topics(self):
        params = Configuration(**self.configuration.parameters)
        servers = ",".join(params.servers)

        c = self._init_client(False, params, dict(), servers)
        topics = c.list_topics()
        topics_names = [SelectElement(topics.get(t).topic) for t in topics]

        return topics_names

    @sync_action("message_preview")
    def message_preview(self):
        self.params = Configuration(**self.configuration.parameters)
        servers = ",".join(self.params.servers)

        c = self._init_client(False, self.params, dict(), servers)
        deserializer = self.get_deserializer()
        last_message = None
        topic = self.params.topics[0]
        for msg in c.consume_message_batch(topic):
            if msg is None:
                break
            if msg.error():
                logging.error("Consumer error: {}".format(msg.error()))
                continue

            extracted_data, _ = self.get_message_data(deserializer, last_message, msg, topic)

            polars.Config.set_tbl_formatting("ASCII_MARKDOWN")
            polars.Config.set_tbl_hide_dataframe_shape(True)
            df = polars.DataFrame(extracted_data.get('value'))
            md_table_output = str(df)

            return ValidationResult(md_table_output, MessageType.SUCCESS)



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
