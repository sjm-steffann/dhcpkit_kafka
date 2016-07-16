"""
Messages that are sent over Kafka
"""
import abc

# Constants for the message direction
from dhcpkit.ipv6.messages import Message
from dhcpkit.protocol_element import ProtocolElement
from struct import unpack, pack

from typing import Union

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

    .. code-block:: none

       0                   1                   2                   3
       0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
      |    msg-type   |   name-len    |                               |
      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+                               .
      .                 server-name (variable length)                 .
      |                                                               |
      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
      |                         timestamp-in                          |
      |                        (double float)                         |
      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
      |                                                               |
      .                                                               .
      .                          message-in                           .
      .                      (variable length)                        .
      .                                                               .
      |                                                               |
      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
      |                         timestamp-out                         |
      |                        (double float)                         |
      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
      |                                                               |
      .                                                               .
      .                          message-out                          .
      .                      (variable length)                        .
      .                                                               .
      |                                                               |
      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    """

    message_type = DHCP_MESSAGE

    def __init__(self, server_name: str = '',
                 timestamp_in: Union[int, float] = 0, message_in: Message = None,
                 timestamp_out: Union[int, float] = 0, message_out: Message = None):
        super().__init__()
        self.server_name = server_name

        self.timestamp_in = timestamp_in
        self.message_in = message_in

        self.timestamp_out = timestamp_out
        self.message_out = message_out

    def validate(self):
        """
        Validate that the contents of this object
        """
        # Check if the server name can be encoded into 255 bytes
        if len(self.server_name.encode('utf-8')) > 255:
            raise ValueError("The server name encoded as UTF-8 must be 255 bytes or less")

        # Check if the timestamp is a signed 64-bit integer
        if not isinstance(self.timestamp_in, (int, float)):
            raise ValueError("Incoming timestamp must be float or integer")

        # Check the messages
        if self.message_in is not None and not isinstance(self.message_in, Message):
            raise ValueError("Incoming message is not a valid DHCPv6 message")

        # Check if the timestamp is a signed 64-bit integer
        if not isinstance(self.timestamp_out, (int, float)):
            raise ValueError("Outgoing timestamp must be float or integer")

        if self.message_out is not None and not isinstance(self.message_out, Message):
            raise ValueError("Outgoing message is not a valid DHCPv6 message")

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

        # Parse the server name
        server_name_length = buffer[my_offset]
        my_offset += 1

        self.server_name = buffer[my_offset:my_offset + server_name_length].decode('utf-8')
        my_offset += server_name_length

        # Read the incoming timestamp
        self.timestamp_in = unpack('!d', buffer[my_offset:my_offset + 8])[0]
        my_offset += 8

        # Read the incoming packet
        message_in_len = int(unpack('!H', buffer[my_offset:my_offset + 2])[0])
        my_offset += 2
        if message_in_len:
            packet_length, self.message_in = Message.parse(buffer, my_offset, message_in_len)
            my_offset += packet_length
        else:
            self.message_in = None

        # Read the outgoing timestamp
        self.timestamp_out = unpack('!d', buffer[my_offset:my_offset + 8])[0]
        my_offset += 8

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

        server_name_bytes = self.server_name.encode('utf-8')
        buffer.append(len(server_name_bytes))
        buffer.extend(server_name_bytes)

        # Incoming message
        message_bytes = self.message_in.save() if self.message_in else b''

        # Incoming timestamp and message
        buffer.extend(pack('!dH', self.timestamp_in, len(message_bytes)))
        buffer.extend(message_bytes)

        # Outgoing message
        message_bytes = self.message_out.save() if self.message_out else b''

        # Outgoing timestamp and message
        buffer.extend(pack('!dH', self.timestamp_out, len(message_bytes)))
        buffer.extend(message_bytes)

        return buffer
