import logging

from confluent_kafka import Consumer, TopicPartition

# maximum time (s) the consumer is waiting for next message
NEXT_MSG_TIMEOUT = 60


class Kbcconsumer():

    def __init__(self, servers, group_id, client_id, name, password, logger, start_offset=None, config_params=None,
                 debug=False):

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
        if debug:
            configuration['debug'] = 'consumer'

        if not start_offset:
            logging.info("No start offset specified, smallest offset will be used.")
        else:
            logging.info("Start offset specified, continue from previous state: {0}".format(start_offset))

        # convert offset keys to proper format
        conv_offsets = dict()
        for off in start_offset:
            conv_offsets[off.replace('p', '')] = start_offset[off]

        self.start_offset = conv_offsets
        self.consumer = Consumer(**configuration)
        logging.debug(self.consumer.assignment(servers))

    def _set_start_offsets(self, consumer, partitions):
        logging.debug(F'Setting starting offsets {self.start_offset} for partitions: {partitions}')
        if self.start_offset:
            for p in partitions:
                p.offset = self.start_offset.get(str(p.partition), 0)
        else:
            for p in partitions:
                p.offset = 0

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

            msgs = self.consumer.consume(**consume_pars)
            if not msgs:
                # polling timeouted, stop
                logging.info(F'Polling timeouted, there was no message received for more than {NEXT_MSG_TIMEOUT}s')
                do_poll = False

            for msg in msgs:
                if msg is None:
                    continue

                if max_offsets.get(msg.partition()) == msg.offset():
                    max_offsets.pop(msg.partition())

                # if all partitions max offset was reached, end
                if not max_offsets:
                    do_poll = False

                yield msg

    def _get_max_offsets(self, topic):
        logging.debug('Getting offset boundaries for all partitions.')
        offsets = dict()
        curr_topics = self.consumer.list_topics(timeout=60).topics
        if not curr_topics.get(topic):
            raise ValueError(F'The topic: "{topic}" does not exist. Available topics are: {curr_topics}')
        for p in curr_topics[topic].partitions:
            boundaries = self.consumer.get_watermark_offsets(TopicPartition(topic, p))
            # store only if there are some new messages
            if boundaries[1] > 0:
                # decrement to get max existing offset
                offsets[p] = boundaries[1] - 1
        logging.debug(F'Offset boundaries listed successfully. {offsets}')
        return offsets
