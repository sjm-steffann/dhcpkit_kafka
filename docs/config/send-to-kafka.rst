.. _send-to-kafka:

Send-to-kafka
=============

This section specifies the Kafka server cluster that data should be sent to.


Example
-------

.. code-block:: dhcpkitconf

    <send-to-kafka>
        broker host1:9092
        broker host2:9092

        topic dhcpkit.messages
    </send-to-kafka>

.. _send-to-kafka_parameters:

Section parameters
------------------

server-name
    The name of this DHCPv6 server to label Kafka messages with

    **Default**: The FQDN of the server

source-address
    The source address to use when connecting to Kafka

    **Example**: "dhcp01.example.com"

topic
    The Kafka topic to publish DHCPKit messages on

    **Default**: "dhcpkit.messages"

broker (multiple allowed)
    Kafka broker to connect to

    **Default**: "localhost:9092"

