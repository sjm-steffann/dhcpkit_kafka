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

from dhcpkit_kafka.messages import IncomingDHCPKafkaMessage, OutgoingDHCPKafkaMessage

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
    """

    def __init__(self, source_address: Tuple[str, int], brokers: Iterable[Tuple[str, int]], topic_name: str,
                 server_name: str):
        super().__init__()

        self.source_address = source_address
        self.brokers = brokers
        self.topic_name = topic_name
        self.server_name = server_name or socket.getfqdn()

        self.kafka = None
        """
        The Kafka client
        :type: pykafka.KafkaClient
        """

        self.kafka_topic = None
        """
        The Kafka topic we publish to
        :type: pykafka.Topic
        """

        self.kafka_producer = None
        """
        The Kafka producer
        :type: pykafka.Producer
        """

    def worker_init(self):
        """
        Initialise the Kafka client in each worker
        """
        logging.getLogger('pykafka').setLevel(logging.WARNING)
        logging.getLogger('pykafka.cluster').setLevel(logging.CRITICAL)

        # Build as strings: the pyKafka type hinting is wrong
        hosts = [_format_host_port(host, port or 9092) for host, port in self.brokers]

        try:
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
        Clean up when this option handler is being removed (or reloaded)
        """
        if self.kafka_producer:
            self.kafka_producer.stop()

    def pre(self, bundle: TransactionBundle):
        """
        Log the request before we start processing it.

        :param bundle: The transaction bundle
        """
        # If something went wrong just return, don't let a Kafka error ruin our server
        if not self.kafka_producer:
            return

        try:
            message = IncomingDHCPKafkaMessage(timestamp=int(time.time()),
                                               server_name=self.server_name,
                                               message=bundle.incoming_message)
            self.kafka_producer.produce(bytes(message.save()))
        except Exception as e:
            logger.warning("Not logging transaction in LookingGlass: {}".format(e))

    def handle(self, bundle: TransactionBundle):
        """
        We log in pre() and post. We don't do anything interesting here.

        :param bundle: The transaction bundle
        """
        pass

    def post(self, bundle: TransactionBundle):
        """
        Log the response before we send it to the client.

        :param bundle: The transaction bundle
        """
        # If something went wrong just return, don't let a Kafka error ruin our server
        if not self.kafka_producer:
            return

        try:
            message = OutgoingDHCPKafkaMessage(timestamp=int(time.time()),
                                               server_name=self.server_name,
                                               message=bundle.outgoing_message)
            self.kafka_producer.produce(bytes(message.save()))
        except Exception as e:
            logger.warning("Not logging transaction in LookingGlass: {}".format(e))
