import logging

from .utils import str_to_file
from confluent_kafka import Producer, Consumer, TopicPartition

# maximum time (s) the consumer is waiting for next message
NEXT_MSG_TIMEOUT = 60


def build_configuration(
    bootstrap_servers,
    client_id,
    logger,
    security_protocol,
    sasl_mechanisms,
    group_id=None,
    username=None,
    password=None,
    ssl_ca=None,
    ssl_key=None,
    ssl_certificate=None,
    config_params=None,
    debug=False,
):
    configuration = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "client.id": client_id,
        "security.protocol": security_protocol,
        "sasl.mechanisms": sasl_mechanisms,
        "sasl.username": username,
        "sasl.password": password,
        "ssl.ca.location": str_to_file(ssl_ca, ".pem"),
        "ssl.key.location": str_to_file(ssl_key, ".pem"),
        "ssl.certificate.location": str_to_file(ssl_certificate, ".pem"),
        "logger": logger,
    }
    if debug:
        configuration["debug"] = "consumer, broker"

    if group_id:  # if consumer add following params
        configuration.update(
            {
                "session.timeout.ms": 6000,
                # we are controlling offset ourselves, by default start from start
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )

    if config_params:
        configuration.update(config_params)

    # kafka config can't handle None or "" values
    return {key: value for key, value in configuration.items() if value is not None}


class KafkaProducer:
    def __init__(
        self,
        bootstrap_servers,
        client_id,
        logger,
        security_protocol,
        sasl_mechanisms,
        username=None,
        password=None,
        ssl_ca=None,
        ssl_key=None,
        ssl_certificate=None,
        config_params=None,
        debug=False,
    ):
        configuration = build_configuration(
            bootstrap_servers=bootstrap_servers,
            client_id=client_id,
            logger=logger,
            security_protocol=security_protocol,
            sasl_mechanisms=sasl_mechanisms,
            username=username,
            password=password,
            ssl_ca=ssl_ca,
            ssl_key=ssl_key,
            ssl_certificate=ssl_certificate,
            config_params=config_params,
            debug=debug,
        )

        self.producer = Producer(**configuration)

    def list_topics(self):
        return self.producer.list_topics(timeout=60).topics

    def produce_message(self, topic, key, value):
        self.producer.produce(topic=topic, key=key, value=value)
        self.producer.flush()


class KafkaConsumer:
    def __init__(
        self,
        bootstrap_servers,
        group_id,
        client_id,
        logger,
        security_protocol,
        sasl_mechanisms,
        username=None,
        password=None,
        ssl_ca=None,
        ssl_key=None,
        ssl_certificate=None,
        start_offset=None,
        config_params=None,
        debug=False,
    ):
        configuration = build_configuration(
            bootstrap_servers=bootstrap_servers,
            client_id=client_id,
            logger=logger,
            security_protocol=security_protocol,
            sasl_mechanisms=sasl_mechanisms,
            group_id=group_id,
            username=username,
            password=password,
            ssl_ca=ssl_ca,
            ssl_key=ssl_key,
            ssl_certificate=ssl_certificate,
            config_params=config_params,
            debug=debug,
        )

        if not start_offset:
            logging.info("No start offset specified, smallest offset will be used.")
        else:
            logging.info("Start offset specified, continue from previous state: {0}".format(start_offset))

        self.start_offsets = start_offset
        self.consumer = Consumer(**configuration)
        logging.debug(self.consumer.assignment())

    def _set_start_offsets(self, consumer, partitions):
        topic = partitions[0].topic

        if self.start_offsets.get(topic):
            logging.info(f"Extracting data from previous offsets: {self.start_offsets.get(topic)} - topic: {topic}")
            for p in partitions:
                p.offset = self.start_offsets.get(topic).get(f"p{p.partition}", -1) + 1
        else:
            logging.info("Extracting data from the beginning")
            for p in partitions:
                p.offset = 0

        consumer.assign(partitions)

    def consume_message_batch(self, topic):
        self.consumer.subscribe([topic], on_assign=self._set_start_offsets)

        # get highest offset for current topic
        max_offsets = self._get_max_offsets(topic)

        logging.info(f"Subscribed to the topic {topic}")
        # Data extraction
        do_poll = True
        # poll until timeout is reached or the max offset is received
        while do_poll:
            logging.info("Reading...")
            consume_pars = dict()
            consume_pars["timeout"] = NEXT_MSG_TIMEOUT

            msgs = self.consumer.consume(**consume_pars)
            if not msgs:
                # polling timed out, stop
                logging.info(f"Polling timed out, there was no message received for more than {NEXT_MSG_TIMEOUT}s")
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
        logging.debug("Getting offset boundaries for all partitions.")
        offsets = dict()
        curr_topics = self.consumer.list_topics(timeout=60).topics
        if not curr_topics.get(topic):
            raise ValueError(f'The topic: "{topic}" does not exist. Available topics are: {curr_topics}')
        for p in curr_topics[topic].partitions:
            boundaries = self.consumer.get_watermark_offsets(TopicPartition(topic, p))
            # store only if there are some new messages
            if boundaries[1] > 0:
                # decrement to get max existing offset
                offsets[p] = boundaries[1] - 1
        logging.debug(f"Offset boundaries listed successfully. {offsets}")
        return offsets

    def list_topics(self):
        return self.consumer.list_topics(timeout=60).topics
