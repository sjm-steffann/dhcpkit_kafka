"""
Messages that are sent over Kafka
"""
import abc

# Constants for the message direction
from dhcpkit.ipv6.messages import Message
from dhcpkit.protocol_element import ProtocolElement
from struct import unpack, pack

INCOMING_DHCP_MESSAGE = 1
OUTGOING_DHCP_MESSAGE = 2


class KafkaMessage(ProtocolElement, metaclass=abc.ABCMeta):
    """
    The base class for Kafka messages.

    :type message_type: int
    """
    # These needs to be overwritten in subclasses
    message_type = 0

    @classmethod
    def determine_class(cls, buffer: bytes, offset: int = 0) -> type:
        """
        Return the appropriate subclass from the registry, or UnknownClientServerMessage if no subclass is registered.

        :param buffer: The buffer to read data from
        :param offset: The offset in the buffer where to start reading
        :return: The best known class for this message data
        """
        from dhcpkit_kafka.message_registry import kafka_message_registry
        message_type = buffer[offset]
        return kafka_message_registry.get(message_type, UnknownKafkaMessage)


class UnknownKafkaMessage(KafkaMessage):
    """
    Container for raw message content for cases where we don't know how to decode the message.

    :type message_data: bytes
    """

    def __init__(self, message_type: int = 0, message_data: bytes = b''):
        super().__init__()
        self.message_type = message_type
        self.message_data = message_data

    def validate(self):
        """
        Validate that the contents of this object conform to protocol specs.
        """
        # Check if the data is bytes
        if not isinstance(self.message_type, int) or not (0 <= self.message_type < 2 ** 8):
            raise ValueError("Message type must be an unsigned 8 bit integer")

        # Check if the data is bytes
        if not isinstance(self.message_data, bytes):
            raise ValueError("Message data must be a sequence of bytes")

    def load_from(self, buffer: bytes, offset: int = 0, length: int = None) -> int:
        """
        Load the internal state of this object from the given buffer. The buffer may contain more data after the
        structured element is parsed. This data is ignored.

        :param buffer: The buffer to read data from
        :param offset: The offset in the buffer where to start reading
        :param length: The amount of data we are allowed to read from the buffer
        :return: The number of bytes used from the buffer
        """
        my_offset = 0

        # Message always begin with a message type
        self.message_type = buffer[offset + my_offset]
        my_offset += 1

        max_length = length or (len(buffer) - offset)
        message_data_len = max_length - my_offset
        self.message_data = buffer[offset + my_offset:offset + my_offset + message_data_len]
        my_offset += message_data_len

        self.validate()

        return my_offset

    def save(self) -> bytes:
        """
        Save the internal state of this object as a buffer.

        :return: The buffer with the data from this element
        """
        self.validate()

        buffer = bytearray()
        buffer.append(self.message_type)
        buffer.extend(self.message_data)
        return buffer


class DHCPKafkaMessage(KafkaMessage, metaclass=abc.ABCMeta):
    """
    A message for publishing DHCPv6 messages over Kafka for analysis.
    """

    def __init__(self, timestamp: int = 0, server_name: str = '', message: Message = None):
        super().__init__()
        self.timestamp = timestamp
        self.server_name = server_name
        self.message = message

    def validate(self):
        """
        Validate that the contents of this object
        """
        # Check if the timestamp is a signed 64-bit integer
        if not isinstance(self.timestamp, int) or not (-2 ** 63 <= self.timestamp < 2 ** 63):
            raise ValueError("Timestamp must be a signed 64-bit integer")

        # Check if the server name can be encoded into 255 bytes
        if len(self.server_name.encode('utf-8')) > 255:
            raise ValueError("The server name encoded as UTF-8 must be 255 bytes or less")

        # Check the message
        if self.message is not None and not isinstance(self.message, Message):
            raise ValueError("The message is not a valid DHCPv6 message")

    def load_from(self, buffer: bytes, offset: int = 0, length: int = None) -> int:
        """
        Load the internal state of this object from the given buffer.

        :param buffer: The buffer to read data from
        :param offset: The offset in the buffer where to start reading
        :param length: The amount of data we are allowed to read from the buffer
        :return: The number of bytes used from the buffer
        """
        my_offset = 0

        # These message types always begin with a message type and a transaction id
        message_type = buffer[offset + my_offset]
        my_offset += 1

        if message_type != self.message_type:
            raise ValueError('The provided buffer does not contain {} data'.format(self.__class__.__name__))

        self.timestamp = unpack('!q', buffer[my_offset:my_offset + 8])[0]
        my_offset += 8

        # Parse the server name
        server_name_length = buffer[my_offset]
        my_offset += 1

        self.server_name = buffer[my_offset:my_offset + server_name_length].decode('utf-8')
        my_offset += server_name_length

        # The rest must be the DHCPv6 packet
        max_length = length or (len(buffer) - offset)
        if max_length:
            packet_length, self.message = Message.parse(buffer, my_offset, max_length - my_offset)
            my_offset += packet_length
        else:
            self.message = None

        self.validate()

        return my_offset

    def save(self) -> bytes:
        """
        Save the internal state of this object as a buffer.

        :return: The buffer with the data from this element
        """
        self.validate()

        buffer = bytearray()
        buffer.append(self.message_type)
        buffer.extend(pack('!q', self.timestamp))

        server_name_bytes = self.server_name.encode('utf-8')
        buffer.append(len(server_name_bytes))
        buffer.extend(server_name_bytes)

        buffer.extend(self.message.save())
        return buffer


class IncomingDHCPKafkaMessage(DHCPKafkaMessage):
    message_type = INCOMING_DHCP_MESSAGE


class OutgoingDHCPKafkaMessage(DHCPKafkaMessage):
    message_type = OUTGOING_DHCP_MESSAGE
