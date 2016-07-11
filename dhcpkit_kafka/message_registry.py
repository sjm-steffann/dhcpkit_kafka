"""
The option registry
"""
from dhcpkit.registry import Registry


class KafkaMessageRegistry(Registry):
    """
    Registry for DHCPKit IPv6 Options
    """
    entry_point = 'dhcpkit_kafka.messages'

    def get_name(self, item: object) -> str:
        """
        Get the name for the by_name mapping.

        :param item: The item to determine the name of
        :return: The name to use as key in the mapping
        """
        name = super().get_name(item)

        # Remove suffixes
        if name.endswith('-kafka-message'):
            name = name[:-14]

        return name


# Instantiate the option registry
kafka_message_registry = KafkaMessageRegistry()
