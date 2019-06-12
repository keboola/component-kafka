"""
Template Component main class.

"""

import logging
import sys
import os
import json
#import time
#from datetime import datetime

from kbc.env_handler import KBCEnvHandler

from confluent_kafka import Consumer, KafkaException, KafkaError

# global constants

# configuration variables
KBC_SERVERS = "servers"
KBC_GROUP_ID = "group_id"
KBC_USERNAME = "username"
KBC_PASSWORD = "password"
KBC_DURATION = "duration" # in seconds
KBC_TOPIC = "topic"
DEBUG = "debug"

APP_VERSION = "0.0.1"

MANDATORY_PARS = [KBC_SERVERS, KBC_GROUP_ID, KBC_TOPIC, KBC_USERNAME, KBC_PASSWORD, KBC_DURATION, DEBUG]


class Component(KBCEnvHandler):

    def __init__(self, debug=False):
        KBCEnvHandler.__init__(self, MANDATORY_PARS)
        # override debug from config
        if self.cfg_params.get(DEBUG):
            debug = True
                # Create output folder

        self.set_default_logger("DEBUG" if debug else "INFO")
        logging.info("Running version %s", APP_VERSION)
        logging.info("Loading configuration...")

        try:
            self.validateConfig()
        except ValueError as e:
            logging.error(e)
            exit(1)


    def run(self):
        """
        Main execution code

        TODO - statistics when DEBUG in conf dict:
        stats_cb(json_str): Callback for statistics data. This callback is triggered by poll() or flush every statistics.interval.ms (needs to be configured separately). Function argument json_str is a str instance of a JSON document containing statistics data. This callback is served upon calling client.poll() or producer.flush(). See https://github.com/edenhill/librdkafka/wiki/Statistics” for more information.
        """

        params = self.cfg_params  # noqa

        # Generating a string out of the list
        servers = ",".join(params.get(KBC_SERVERS))
        

        # Get current state file for offset, 0 if empty
        try:
            offset = self.get_state_file()["offset"]
        except:
            offset = 0

        conf = {
            "bootstrap.servers": servers,
            "group.id": "%s-consumer" % params.get(KBC_GROUP_ID),
            "session.timeout.ms": 6000,
            "security.protocol": "SASL_SSL",
	        "sasl.mechanisms": "SCRAM-SHA-256",
            "sasl.username": params.get(KBC_USERNAME),
            "sasl.password": params.get(KBC_PASSWORD),
            "auto.offset.reset": "smallest"
            }

        if offset == 0:
            logging.info("Extracting data from the beginninng")
        else:
            logging.info("Extracting data from previous offset: {0}".format(offset))

        topics = (params.get(KBC_TOPIC)).split(",")
        
        # Get data
        self.extract_data(conf, offset, topics)

        # TODO there should be a function to kill this based on the offset
        logging.info("Extraction finished.")

        # Produce final sliced table
        self.create_sliced_tables(self, "kafka")


    def extract_data(self, conf, offset, topics):
        """
        Consumer configuration
        https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
        """
        
        logging.info("Extracting data from topics {0}".format(topics))

        # Destination folder is the name of the topic
        destination_folder = topics

        # Setup
        c = Consumer(**conf)
    
        # Subscribe to the topic
        c.subscribe(topics)

        logging.info("Subscribed to the topic ^")

        # Data extraction
        while True:
            msg = c.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                logging.error("Consumer error: {}".format(msg.error()))
                continue

            extracted_data = (("{0}, {1}, {2}, {3}, {4}, {5}, {6}").format(
                    msg.topic(),
                    msg.timestamp()[0],
                    msg.timestamp()[1],
                    msg.partition(),
                    msg.offset(),
                    msg.key(),
                    msg.value().decode('utf-8'),
                    ))
            
            filename = (("{0}-{1}-{2}.csv").format(
                    msg.topic(),
                    msg.timestamp()[0],
                    msg.timestamp()[1],
                    ))

            logging.info(extracted_data)
            # json.dumps(v).encode('utf-8')

            # Save data as a sliced table file in defined folder
            self.save_file(extracted_data, filename)

        # Close down consumer to commit final offsets.
        c.close()
        
        # will be changed
        new_offset = msg.offset()

        # Store previous offset
        state_dict = {"offset": new_offset}
        self.write_state_file(state_dict)


    def save_file(self, line, filename):
        """
        Save text as file
        """

        #file_path = os.path.join(filename)

        print(filename)
        logging.info(filename)

        try:
            with open(filename, 'w') as file:
                file.write(line)
                file.write('\n')
            logging.info("File saved.")
        except:
            logging.error("Could not save file! exit.")



if __name__ == "__main__":
    """
    Main entrypoint
    """
    if len(sys.argv) > 1:
        debug = sys.argv[1]
    else:
        debug = True

    comp = Component(debug)
    comp.run()
