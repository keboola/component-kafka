"""
Template Component main class.

"""

import logging
import os
import sys

import csv
from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException
from configuration import Configuration

from kafka_kbc.client import Kbcconsumer

# global constants
RESULT_PK = ['topic', 'timestamp_type', 'timestamp', 'partition', 'offset', 'key']
RESULT_COLS = ['topic', 'timestamp_type', 'timestamp', 'partition', 'offset', 'key',
               'value']


class Component(ComponentBase):

    def __init__(self):
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

        params = Configuration(**self.configuration.parameters)

        # Generating a string out of the list
        servers = ",".join(params.servers)

        # Get current state file for offset, 0 if empty
        if params.begin_offsets:
            logging.info(F'Begin offset specified, overriding with {params.begin_offsets}')
            prev_offsets = params.begin_offsets
        else:
            logging.info('Loading state file..')
            prev_offsets = self.get_state_file().get("prev_offsets", dict())

        if not prev_offsets:
            logging.info("Extracting data from the beginning")
        else:
            logging.info("Extracting data from previous offsets: {0}".format(prev_offsets))

        topics = (params.topic).split(",")

        # Setup
        c = Kbcconsumer(servers=servers,
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
                        config_params=params.config_params,
                        debug=debug)

        logging.info("Extracting data from topics {0}".format(topics))

        res_file_folder = os.path.join(self.tables_out_path, 'kafka_results')

        latest_offsets = dict()
        msg_cnt = 0
        for msg in c.consume_message_batch(topics):
            if msg is None:
                break
            if msg.error():
                logging.error("Consumer error: {}".format(msg.error()))
                continue

            extracted_data = {
                'topic': msg.topic(),
                'timestamp_type': msg.timestamp()[0],
                'timestamp': msg.timestamp()[1],
                'partition': msg.partition(),
                'offset': msg.offset(),
                'key': msg.key(),
                'value': msg.value().decode('utf-8')}

            filename = (("{0}-{1}-{2}.csv").format(
                msg.topic(),
                msg.timestamp()[0],
                msg.timestamp()[1],
            ))

            logging.debug(F'Received message: {extracted_data}')

            # Save data as a sliced table file in defined folder
            self.save_file(extracted_data, os.path.join(res_file_folder, filename))
            msg_cnt += 1

            # will be changed
            latest_offsets['p' + str(msg.partition())] = msg.offset()

        # Store previous offsets
        state_dict = {"prev_offsets": latest_offsets}
        self.write_state_file(state_dict)
        logging.info("Offset file stored.")

        logging.info("Extraction finished.")

        # Produce final sliced table manifest
        if msg_cnt > 0:
            logging.info(F'Fetched {msg_cnt} messages.')
            out_table=self.create_out_table_definition(res_file_folder, is_sliced=True,
                                                       primary_key=RESULT_PK, columns=RESULT_COLS, incremental=True)

            self.write_manifest(out_table)
        else:
            logging.info('No new messages found!')

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