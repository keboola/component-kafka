"""
Template Component main class.

"""

import logging
import os
import csv

from keboola.component.base import ComponentBase, sync_action
from keboola.component.sync_actions import SelectElement
from keboola.component.exceptions import UserException
from configuration import Configuration

from kafka.client import KafkaConsumer

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

# global constants
RESULT_PK = ['topic', 'timestamp_type', 'timestamp', 'partition', 'offset', 'key']
RESULT_COLS = ['topic', 'timestamp_type', 'timestamp', 'partition', 'offset', 'key',
               'value']


class Component(ComponentBase):

    def __init__(self):
        self.params = None
        self.client = None
        self.topics = dict()
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

        # Generating a string out of the list
        servers = ",".join(self.params.servers)

        self.latest_offsets = self.get_state_file().get("prev_offsets", dict())

        self.client = self._init_client(debug, self.params, self.latest_offsets, servers)

        logging.info("Extracting data from topics {0}".format(self.params.topics))

        for topic in self.params.topics:
            msg_cnt, res_file_folder = self.consume_topic(topic)
            self.topics[topic] = {'msg_cnt': msg_cnt, 'res_file_folder': res_file_folder}

        # Store previous offsets
        state_dict = {"prev_offsets": self.latest_offsets}
        self.write_state_file(state_dict)
        logging.info("Offset file stored.")

        logging.info("Extraction finished.")

        for topic, consumed in self.topics.items():
            # Produce final sliced table manifest
            if consumed['msg_cnt'] > 0:
                logging.info(F'Fetched {consumed['msg_cnt']} messages from topic - {topic}')
                out_table = self.create_out_table_definition(consumed['res_file_folder'], is_sliced=True,
                                                             primary_key=RESULT_PK, columns=RESULT_COLS,
                                                             incremental=True)

                self.write_manifest(out_table)
            else:
                logging.info('No new messages found!')

    def consume_topic(self, topic):
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
        res_file_folder = os.path.join(self.tables_out_path, topic)
        msg_cnt = 0
        for msg in self.client.consume_message_batch(topic):
            if msg is None:
                break
            if msg.error():
                logging.error("Consumer error: {}".format(msg.error()))
                continue

            if self.params.deserialize == 'avro':
                value = deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
            else:
                value = msg.value().decode('utf-8')

            extracted_data = {
                'topic': msg.topic(),
                'timestamp_type': msg.timestamp()[0],
                'timestamp': msg.timestamp()[1],
                'partition': msg.partition(),
                'offset': msg.offset(),
                'key': msg.key(),
                'value': value}

            filename = (("{0}-{1}.csv").format(
                msg.timestamp()[0],
                msg.timestamp()[1],
            ))

            logging.debug(F'Received message: {extracted_data}')

            # Save data as a sliced table file in defined folder
            self.save_file(extracted_data, os.path.join(res_file_folder, filename))
            msg_cnt += 1

            print(msg.partition())

            if msg.topic() not in self.latest_offsets:
                self.latest_offsets[msg.topic()] = {}

            self.latest_offsets[msg.topic()]['p' + str(msg.partition())] = msg.offset()

        return msg_cnt, res_file_folder

    def _init_client(self, debug, params, prev_offsets, servers):
        c = KafkaConsumer(servers=servers,
                          group_id="%s-consumer" % params.group_id,
                          client_id="test",
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

    def save_file(self, line, filename):
        """
        Save text as file
        """
        logging.info(F'Writing file {filename}')

        if not os.path.exists(os.path.dirname(filename)):
            os.makedirs(os.path.dirname(filename))
        try:
            with open(filename, 'a') as file:
                writer = csv.DictWriter(file, fieldnames=RESULT_COLS)
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
