"""
Configuration elements for the SOL_MAX_RT option handlers
"""
import string

from dhcpkit.ipv6.server.handlers import HandlerFactory

from dhcpkit_kafka.server_extension import KafkaHandler

topic_chars = string.ascii_letters + string.digits + '._-'
topic_max_length = 255


def topic_name(value: str) -> str:
    if value in ['', '.', '..']:
        raise ValueError("Topic name can not be empty, '.' or '..'")

    if len(value) > topic_max_length:
        raise ValueError("Topic name must be {} characters or less".format(topic_max_length))

    if any([char not in topic_chars for char in value]):
        raise ValueError("Topic names may only contain {}".format(topic_chars))

    return value


class KafkaHandlerFactory(HandlerFactory):
    """
    Create the handler for the Kafka producer.
    """

    def create(self) -> KafkaHandler:
        """
        Create a handler of this class based on the configuration in the config section.

        :return: A handler object
        """
        return KafkaHandler(source_address=self.source_address, brokers=self.brokers, topic_name=self.topic,
                            server_name=self.server_name)
