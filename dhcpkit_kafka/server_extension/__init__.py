"""
This handler provides a looking glass into DHCP server operations by sending interesting information on Kafka
"""
import logging
import socket
import time

import pykafka
from dhcpkit.ipv6.server.handlers import Handler
from dhcpkit.ipv6.server.transaction_bundle import TransactionBundle
from typing import Iterable, Tuple

from dhcpkit_kafka.messages import DHCPKafkaMessage

logger = logging.getLogger(__name__)


def _format_host_port(host: str, port: int) -> str:
    if not host:
        # Default hostname
        host = socket.getfqdn()

    if ':' in host:
        host = '[{}]'.format(host)

    if port:
        return '{}:{}'.format(host, port)
    else:
        return host


class KafkaHandler(Handler):
    """
    Option handler that provides a looking glass into DHCP server operations by logging information about requests
    and responses into an SQLite database.

    The primary key is (duid, interface_id, remote_id)

    :type source_address: Tuple[str, int]
    :type brokers: Iterable[Tuple[str, int]]
    :type topic_name: str
    :type server_name: str
    :type kafka: pykafka.KafkaClient
    :type kafka_topic: pykafka.Topic
    :type kafka_producer: pykafka.Producer
    :type last_connect_attempt: int
    """

    def __init__(self, source_address: Tuple[str, int], brokers: Iterable[Tuple[str, int]], topic_name: str,
                 server_name: str):
        super().__init__()

        self.source_address = source_address
        self.brokers = brokers
        self.topic_name = topic_name
        self.server_name = server_name or socket.getfqdn()

        self.kafka = None
        """The Kafka client"""

        self.kafka_topic = None
        """The Kafka topic we publish to"""

        self.kafka_producer = None
        """The Kafka producer"""

        self.last_connect_attempt = 0
        """Remember when the last connection attempt was for reconnect rate-limiting"""

    def worker_init(self):
        """
        Initialise the Kafka client in each worker
        """
        logging.getLogger('pykafka').setLevel(logging.WARNING)
        logging.getLogger('pykafka.cluster').setLevel(logging.CRITICAL)

        # Create the connection
        self.connect()

    def connect(self):
        now = time.time()
        if now - self.last_connect_attempt < 5:
            # Last attempt was less than 5 seconds ago, don't push it...
            return

        self.last_connect_attempt = now

        try:
            # Build as strings: the pyKafka type hinting is wrong
            hosts = [_format_host_port(host, port or 9092) for host, port in self.brokers]

            # noinspection PyTypeChecker
            self.kafka = pykafka.KafkaClient(hosts=','.join(hosts), source_address=self.source_address or '')
            """The Kafka client"""

            self.kafka_topic = self.kafka.topics[self.topic_name.encode('ascii')]
            """The Kafka topic we publish to"""

            self.kafka_producer = self.kafka_topic.get_producer(block_on_queue_full=False)
            """The Kafka producer"""
        except Exception as e:
            logger.critical("Kafka logging disabled: {}".format(e))

    def __del__(self):
        """
        Clean up when this option handler is being removed (or reloaded).
        """
        if self.kafka_producer:
            self.kafka_producer.stop()

    def analyse_pre(self, bundle: TransactionBundle):
        """
        Start building the Kafka message.

        :param bundle: The transaction bundle
        """
        bundle.handler_data[self] = DHCPKafkaMessage(server_name=self.server_name,
                                                     timestamp_in=time.time(),
                                                     message_in=bundle.incoming_message)

    def analyse_post(self, bundle: TransactionBundle):
        """
        Finish the Kafka message and send it.

        :param bundle: The transaction bundle
        """
        # Try to reconnect if necessary
        if not self.kafka_producer:
            self.connect()
            if not self.kafka_producer:
                # Ok, nowhere to send the analysis, stop
                return

        try:
            # Get the Kafka message from storage
            message = bundle.handler_data[self]

            # Add the outgoing message to the Kafka message
            message.timestamp_out = time.time()
            message.message_out = bundle.outgoing_message

            # And send the Kafka message
            self.kafka_producer.produce(bytes(message.save()))
        except Exception as e:
            logger.warning("Not logging transaction in LookingGlass: {}".format(e))
