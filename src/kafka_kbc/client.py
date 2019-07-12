import logging
import sys

from confluent_kafka import Consumer, TopicPartition

# maximum time (s) the consumer is waiting for next message (applies for first run)
NEXT_MSG_TIMEOUT = 60


class Kbcconsumer():

    def __init__(self, servers, group_id, client_id, name, password, logger, start_offset=None, config_params=None):

        configuration = {
            "bootstrap.servers": servers,
            "group.id": group_id,
            "client.id": client_id,
            "session.timeout.ms": 6000,
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "SCRAM-SHA-256",
            "sasl.username": name,
            "sasl.password": password,
            # we are controlling offset ourselves, by default start from start
            "auto.offset.reset": "smallest",
            "enable.auto.commit": True
        }

        if start_offset == 0:
            logging.info("No start offset specified, smallest offset will be used.")
        else:
            logging.info("Start offset specified, continue from previous state: {0}".format(start_offset))

        self.start_offset = start_offset
        self.consumer = Consumer(**configuration)
        print(self.consumer.assignment(servers))

    def _set_start_offsets(self, consumer, partitions, **kwargs):
        logging.debug(F'Setting starting offsets {self.start_offset} for partitions: {partitions}')
        if self.start_offset:
            for p in partitions:
                p.offset = self.start_offset.get(p.partition, -1001)

        consumer.assign(partitions)

    def consume_message_batch(self, topics):

        self.consumer.subscribe(topics, on_assign=self._set_start_offsets)

        # get highest offset for current topic
        max_offsets = self._get_max_offsets(topics[0])

        logging.info(F"Subscribed to the topic {topics}")
        # Data extraction
        do_poll = True
        # poll until timeout is reached or the max offset is received
        while do_poll:
            logging.info("Reading...")
            consume_pars = dict()
            consume_pars['timeout'] = NEXT_MSG_TIMEOUT
            # if not self.start_offset:
            #     consume_pars['num_messages'] = MAX_MSGS_COLD

            msgs = self.consumer.consume(**consume_pars)
            if not msgs:
                # polling timeouted, stop
                logging.info(F'Polling timeouted, there was no message received for more than {NEXT_MSG_TIMEOUT}s')
                do_poll = False

            for msg in msgs:
                if msg is None:
                    continue

                yield msg

    def _get_max_offsets(self, topic):
        logging.debug('Getting offset boundaries for all partitions.')
        offsets = dict()
        curr_topics = self.consumer.list_topics()
        for p in curr_topics.topics[topic].partitions:
            boundaries = self.consumer.get_watermark_offsets(TopicPartition(topic, p))
            offsets[p] = boundaries[1]

        return offsets
