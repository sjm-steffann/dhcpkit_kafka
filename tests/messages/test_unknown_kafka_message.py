"""
Test the UnknownKafkaMessage implementation
"""
import unittest

from dhcpkit_kafka.messages import UnknownKafkaMessage
from tests.messages import test_kafka_message

unknown_message = UnknownKafkaMessage(255, b'ThisIsAnUnknownMessage')
unknown_packet = bytes.fromhex('ff') + b'ThisIsAnUnknownMessage'


class UnknownKafkaMessageTestCase(test_kafka_message.KafkaMessageTestCase):
    def setUp(self):
        self.packet_fixture = unknown_packet
        self.message_fixture = unknown_message
        self.parse_packet()

    def parse_packet(self):
        super().parse_packet()
        self.assertIsInstance(self.message, UnknownKafkaMessage)

    def test_validate_message_type(self):
        self.check_unsigned_integer_property('message_type', size=8)

    def test_validate_data(self):
        # This should be ok
        self.message.message_data = b''
        self.message.validate()

        # This shouldn't
        self.message.message_data = ''
        with self.assertRaisesRegex(ValueError, 'sequence of bytes'):
            self.message.validate()


if __name__ == '__main__':
    unittest.main()
