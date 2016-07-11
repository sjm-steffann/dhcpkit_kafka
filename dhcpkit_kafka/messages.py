"""
Messages that are sent over Kafka
"""
import abc

# Constants for the message direction
from dhcpkit.ipv6.messages import Message
from dhcpkit.protocol_element import ProtocolElement
from struct import unpack, pack

DHCP_MESSAGE = 1


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
    message_type = DHCP_MESSAGE

    def __init__(self, timestamp: int = 0, server_name: str = '',
                 message_in: Message = None, message_out: Message = None):
        super().__init__()
        self.timestamp = timestamp
        self.server_name = server_name
        self.message_in = message_in
        self.message_out = message_out

    def validate(self):
        """
        Validate that the contents of this object
        """
        # Check if the timestamp is a signed 64-bit integer
        if not isinstance(self.timestamp, (int, float)):
            raise ValueError("Timestamp must be a float or integer")

        # Check if the server name can be encoded into 255 bytes
        if len(self.server_name.encode('utf-8')) > 255:
            raise ValueError("The server name encoded as UTF-8 must be 255 bytes or less")

        # Check the messages
        if self.message_in is not None and not isinstance(self.message_in, Message):
            raise ValueError("The incoming message is not a valid DHCPv6 message")

        if self.message_out is not None and not isinstance(self.message_out, Message):
            raise ValueError("The outgoing message is not a valid DHCPv6 message")

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

        self.timestamp = unpack('!d', buffer[my_offset:my_offset + 8])[0]
        my_offset += 8

        # Parse the server name
        server_name_length = buffer[my_offset]
        my_offset += 1

        self.server_name = buffer[my_offset:my_offset + server_name_length].decode('utf-8')
        my_offset += server_name_length

        # Read the incoming message
        message_in_len = int(unpack('!H', buffer[my_offset:my_offset + 2])[0])
        my_offset += 2
        if message_in_len:
            packet_length, self.message_in = Message.parse(buffer, my_offset, message_in_len)
            my_offset += packet_length
        else:
            self.message_in = None

        # Read the outgoing message
        message_out_len = int(unpack('!H', buffer[my_offset:my_offset + 2])[0])
        my_offset += 2
        if message_out_len:
            packet_length, self.message_out = Message.parse(buffer, my_offset, message_out_len)
            my_offset += packet_length
        else:
            self.message_out = None

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
        buffer.extend(pack('!d', self.timestamp))

        server_name_bytes = self.server_name.encode('utf-8')
        buffer.append(len(server_name_bytes))
        buffer.extend(server_name_bytes)

        # Incoming message
        if self.message_in:
            message_bytes = self.message_in.save()
        else:
            message_bytes = b''

        buffer.extend(pack('!H', len(message_bytes)))
        buffer.extend(message_bytes)

        # Outgoing message
        if self.message_out:
            message_bytes = self.message_out.save()
        else:
            message_bytes = b''

        buffer.extend(pack('!H', len(message_bytes)))
        buffer.extend(message_bytes)

        return buffer
