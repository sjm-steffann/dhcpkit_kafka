"""
Test the UnknownKafkaMessage implementation
"""

import codecs
import unittest
from ipaddress import IPv6Network, IPv6Address

from dhcpkit.ipv6.duids import LinkLayerDUID, LinkLayerTimeDUID
from dhcpkit.ipv6.extensions.dns import OPTION_DNS_SERVERS
from dhcpkit.ipv6.extensions.dns import RecursiveNameServersOption
from dhcpkit.ipv6.extensions.ntp import OPTION_NTP_SERVER
from dhcpkit.ipv6.extensions.prefix_delegation import IAPDOption, IAPrefixOption
from dhcpkit.ipv6.extensions.prefix_delegation import OPTION_IA_PD
from dhcpkit.ipv6.extensions.remote_id import RemoteIdOption
from dhcpkit.ipv6.extensions.sntp import OPTION_SNTP_SERVERS
from dhcpkit.ipv6.extensions.sol_max_rt import OPTION_SOL_MAX_RT, OPTION_INF_MAX_RT
from dhcpkit.ipv6.messages import AdvertiseMessage, RelayReplyMessage
from dhcpkit.ipv6.messages import SolicitMessage, RelayForwardMessage
from dhcpkit.ipv6.options import ClientIdOption, IANAOption, \
    ReconfigureAcceptOption, IAAddressOption, ServerIdOption, RelayMessageOption, InterfaceIdOption
from dhcpkit.ipv6.options import ElapsedTimeOption, RapidCommitOption, OptionRequestOption, OPTION_IA_NA, \
    OPTION_VENDOR_OPTS, VendorClassOption

from dhcpkit_kafka.messages import DHCPKafkaMessage
from tests.messages import test_kafka_message

# Use the relayed solicit message from dhcpkit.tests.ipv6.messages.test_relay_forward_message
relayed_solicit_message = RelayForwardMessage(
    hop_count=1,
    link_address=IPv6Address('2001:db8:ffff:1::1'),
    peer_address=IPv6Address('fe80::3631:c4ff:fe3c:b2f1'),
    options=[
        RelayMessageOption(relayed_message=RelayForwardMessage(
            hop_count=0,
            link_address=IPv6Address('::'),
            peer_address=IPv6Address('fe80::3631:c4ff:fe3c:b2f1'),
            options=[
                RelayMessageOption(relayed_message=SolicitMessage(
                    transaction_id=bytes.fromhex('f350d6'),
                    options=[
                        ElapsedTimeOption(elapsed_time=0),
                        ClientIdOption(duid=LinkLayerDUID(hardware_type=1,
                                                          link_layer_address=bytes.fromhex('3431c43cb2f1'))),
                        RapidCommitOption(),
                        IANAOption(iaid=bytes.fromhex('c43cb2f1')),
                        IAPDOption(iaid=bytes.fromhex('c43cb2f1'), options=[
                            IAPrefixOption(prefix=IPv6Network('::/0')),
                        ]),
                        ReconfigureAcceptOption(),
                        OptionRequestOption(requested_options=[
                            OPTION_DNS_SERVERS,
                            OPTION_NTP_SERVER,
                            OPTION_SNTP_SERVERS,
                            OPTION_IA_PD,
                            OPTION_IA_NA,
                            OPTION_VENDOR_OPTS,
                            OPTION_SOL_MAX_RT,
                            OPTION_INF_MAX_RT,
                        ]),
                        VendorClassOption(enterprise_number=872),
                    ],
                )),
                InterfaceIdOption(interface_id=b'Fa2/3'),
                RemoteIdOption(enterprise_number=9, remote_id=bytes.fromhex('020023000001000a0003000100211c7d486e')),
            ])
        ),
        InterfaceIdOption(interface_id=b'Gi0/0/0'),
        RemoteIdOption(enterprise_number=9, remote_id=bytes.fromhex('020000000000000a0003000124e9b36e8100')),
    ],
)

relayed_solicit_packet = codecs.decode('0c0120010db8ffff0001000000000000'
                                       '0001fe800000000000003631c4fffe3c'
                                       'b2f1000900c20c000000000000000000'
                                       '0000000000000000fe80000000000000'
                                       '3631c4fffe3cb2f10009007901f350d6'
                                       '0008000200000001000a000300013431'
                                       'c43cb2f1000e00000003000cc43cb2f1'
                                       '000000000000000000190029c43cb2f1'
                                       '0000000000000000001a001900000000'
                                       '00000000000000000000000000000000'
                                       '00000000000014000000060010001700'
                                       '38001f00190003001100520053001000'
                                       '0400000368001200054661322f330025'
                                       '001600000009020023000001000a0003'
                                       '000100211c7d486e001200074769302f'
                                       '302f3000250016000000090200000000'
                                       '00000a0003000124e9b36e8100', 'hex')

# Use the relayed advertise message from dhcpkit.tests.ipv6.messages.test_relay_reply_message
relayed_advertise_message = RelayReplyMessage(
    hop_count=1,
    link_address=IPv6Address('2001:db8:ffff:1::1'),
    peer_address=IPv6Address('fe80::3631:c4ff:fe3c:b2f1'),
    options=[
        InterfaceIdOption(interface_id=b'Gi0/0/0'),
        RelayMessageOption(relayed_message=RelayReplyMessage(
            hop_count=0,
            link_address=IPv6Address('::'),
            peer_address=IPv6Address('fe80::3631:c4ff:fe3c:b2f1'),
            options=[
                InterfaceIdOption(interface_id=b'Fa2/3'),
                RelayMessageOption(relayed_message=AdvertiseMessage(
                    transaction_id=bytes.fromhex('f350d6'),
                    options=[
                        IANAOption(iaid=bytes.fromhex('c43cb2f1'), options=[
                            IAAddressOption(address=IPv6Address('2001:db8:ffff:1:c::e09c'), preferred_lifetime=375,
                                            valid_lifetime=600),
                        ]),
                        IAPDOption(iaid=bytes.fromhex('c43cb2f1'), options=[
                            IAPrefixOption(prefix=IPv6Network('2001:db8:ffcc:fe00::/56'), preferred_lifetime=375,
                                           valid_lifetime=600),
                        ]),
                        ClientIdOption(duid=LinkLayerDUID(hardware_type=1,
                                                          link_layer_address=bytes.fromhex('3431c43cb2f1'))),
                        ServerIdOption(duid=LinkLayerTimeDUID(hardware_type=1, time=488458703,
                                                              link_layer_address=bytes.fromhex('00137265ca42'))),
                        ReconfigureAcceptOption(),
                        RecursiveNameServersOption(dns_servers=[IPv6Address('2001:4860:4860::8888')]),
                    ],
                ))
            ],
        ))
    ],
)

relayed_advertise_packet = codecs.decode('0d0120010db8ffff0001000000000000'
                                         '0001fe800000000000003631c4fffe3c'
                                         'b2f1001200074769302f302f30000900'
                                         'c40d0000000000000000000000000000'
                                         '000000fe800000000000003631c4fffe'
                                         '3cb2f1001200054661322f3300090095'
                                         '02f350d600030028c43cb2f100000000'
                                         '000000000005001820010db8ffff0001'
                                         '000c00000000e09c0000017700000258'
                                         '00190029c43cb2f10000000000000000'
                                         '001a001900000177000002583820010d'
                                         'b8ffccfe000000000000000000000100'
                                         '0a000300013431c43cb2f10002000e00'
                                         '0100011d1d49cf00137265ca42001400'
                                         '00001700102001486048600000000000'
                                         '0000008888', 'hex')


class DHCPKafkaMessageTestCase(test_kafka_message.KafkaMessageTestCase):
    def setUp(self):
        self.packet_fixture = (bytes.fromhex('0110') + b'name.example.com' +
                               bytes.fromhex('40C81C8000000000') +
                               bytes.fromhex('{:04x}'.format(len(relayed_solicit_packet))) + relayed_solicit_packet +
                               bytes.fromhex('40D6E80000000000') +
                               bytes.fromhex('{:04x}'.format(len(relayed_advertise_packet))) + relayed_advertise_packet)

        self.message_fixture = DHCPKafkaMessage(server_name='name.example.com',
                                                timestamp_in=12345,
                                                message_in=relayed_solicit_message,
                                                timestamp_out=23456,
                                                message_out=relayed_advertise_message)
        self.parse_packet()

    def parse_packet(self):
        super().parse_packet()
        self.assertIsInstance(self.message, DHCPKafkaMessage)

    def test_validate_server_name(self):
        # This shouldn't
        self.message.server_name = 'x' * 255
        self.message.validate()

        self.message.server_name = 'x' * 256
        with self.assertRaisesRegex(ValueError, '255 bytes or less'):
            self.message.validate()

    def test_validate_timestamp_in(self):
        self.message.timestamp_in = 1
        self.message.validate()

        self.message.timestamp_in = 1.5
        self.message.validate()

        self.message.timestamp_in = b''
        with self.assertRaisesRegex(ValueError, 'float or integer'):
            self.message.validate()

    def test_validate_message_in(self):
        self.message.message_in = b'12345'
        with self.assertRaisesRegex(ValueError, 'not a valid DHCPv6 message'):
            self.message.validate()

    def test_validate_timestamp_out(self):
        self.message.timestamp_out = 1
        self.message.validate()

        self.message.timestamp_out = 1.5
        self.message.validate()

        self.message.timestamp_out = b''
        with self.assertRaisesRegex(ValueError, 'float or integer'):
            self.message.validate()

    def test_validate_message_out(self):
        self.message.message_out = b'12345'
        with self.assertRaisesRegex(ValueError, 'not a valid DHCPv6 message'):
            self.message.validate()

    def test_load_wrong_type(self):
        message = DHCPKafkaMessage()
        with self.assertRaisesRegex(ValueError, 'does not contain DHCPKafkaMessage'):
            message.load_from(b'ff')


class NoInboundMessageDHCPKafkaMessageTestCase(test_kafka_message.KafkaMessageTestCase):
    def setUp(self):
        self.packet_fixture = (bytes.fromhex('0110') + b'name.example.com' +
                               bytes.fromhex('40C81C8000000000') +
                               bytes.fromhex('{:04x}'.format(0)) +
                               bytes.fromhex('40D6E80000000000') +
                               bytes.fromhex('{:04x}'.format(len(relayed_advertise_packet))) + relayed_advertise_packet)

        self.message_fixture = DHCPKafkaMessage(server_name='name.example.com',
                                                timestamp_in=12345,
                                                message_in=None,
                                                timestamp_out=23456,
                                                message_out=relayed_advertise_message)
        self.parse_packet()

    def parse_packet(self):
        super().parse_packet()
        self.assertIsInstance(self.message, DHCPKafkaMessage)


class NoOutboundMessageDHCPKafkaMessageTestCase(test_kafka_message.KafkaMessageTestCase):
    def setUp(self):
        self.packet_fixture = (bytes.fromhex('0110') + b'name.example.com' +
                               bytes.fromhex('40C81C8000000000') +
                               bytes.fromhex('{:04x}'.format(len(relayed_solicit_packet))) + relayed_solicit_packet +
                               bytes.fromhex('40D6E80000000000') +
                               bytes.fromhex('{:04x}'.format(0)))

        self.message_fixture = DHCPKafkaMessage(server_name='name.example.com',
                                                timestamp_in=12345,
                                                message_in=relayed_solicit_message,
                                                timestamp_out=23456,
                                                message_out=None)
        self.parse_packet()

    def parse_packet(self):
        super().parse_packet()
        self.assertIsInstance(self.message, DHCPKafkaMessage)


if __name__ == '__main__':
    unittest.main()
