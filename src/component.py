"""
Template Component main class.

"""

import logging
import os
import sys

import csv
from kbc.env_handler import KBCEnvHandler

from kafka_kbc.client import Kbcconsumer

# global constants
RESULT_PK = ['topic', 'timestamp_type', 'timestamp', 'partition', 'offset', 'key']
RESULT_COLS = ['topic', 'timestamp_type', 'timestamp', 'partition', 'offset', 'key',
               'value']

# configuration variables
KBC_SERVERS = "servers"
KBC_GROUP_ID = "group_id"
KBC_USERNAME = "username"
KBC_PASSWORD = "#password"
KBC_TOPIC = "topic"
KBC_BEGIN_OFFSET = "begin_offsets"
DEBUG = "debug"

APP_VERSION = "0.0.1"

MANDATORY_PARS = [KBC_SERVERS, KBC_GROUP_ID, KBC_TOPIC, KBC_USERNAME, KBC_PASSWORD]


class Component(KBCEnvHandler):
    def __init__(self, debug=False):

        KBCEnvHandler.__init__(self, MANDATORY_PARS)
        # override debug from config
        if self.cfg_params.get('debug'):
            debug = True

        self.set_default_logger('DEBUG' if debug else 'INFO')
        logging.info('Running version %s', APP_VERSION)
        logging.info('Loading configuration...')

        try:
            self.validate_config(MANDATORY_PARS)

        except ValueError as e:
            logging.error(e)
            exit(1)

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

        params = self.cfg_params  # noqa

        # Generating a string out of the list
        servers = ",".join(params.get(KBC_SERVERS))

        # Get current state file for offset, 0 if empty
        if params.get(KBC_BEGIN_OFFSET):
            logging.info(F'Begin offset specified, overriding with {params.get(KBC_BEGIN_OFFSET)}')
            prev_offsets = params.get(KBC_BEGIN_OFFSET)
        else:
            logging.info('Loading state file..')
            prev_offsets = self.get_state_file().get("prev_offsets", dict())

        if not prev_offsets:
            logging.info("Extracting data from the beginning")
        else:
            logging.info("Extracting data from previous offsets: {0}".format(prev_offsets))

        topics = (params.get(KBC_TOPIC)).split(",")

        # Setup
        c = Kbcconsumer(servers, "%s-consumer" % params.get(KBC_GROUP_ID), "test", params.get(KBC_USERNAME),
                        params.get(KBC_PASSWORD), logging.getLogger(), start_offset=prev_offsets, debug=debug)

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
            self.configuration.write_table_manifest(res_file_folder,
                                                    primary_key=RESULT_PK,
                                                    columns=RESULT_COLS,
                                                    incremental=True)
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
            with open(filename, 'w') as file:
                writer = csv.DictWriter(file, fieldnames=RESULT_COLS)
                writer.writerow(line)
            logging.info("File saved.")
        except Exception as e:
            logging.error("Could not save file! exit.", e)


if __name__ == "__main__":
    """
    Main entrypoint
    """
    if len(sys.argv) > 1:
        debug = sys.argv[1]
    else:
        debug = True

    comp = Component(debug)
    comp.run(debug)
