"""
Setup script for dhcpkit_looking_glass
"""
import os

import dhcpkit_kafka
from setuptools import find_packages, setup


# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...
def read(filename):
    """
    Read the contents of a file

    :param filename: the file name relative to this file
    :return: The contents of the file
    """
    return open(os.path.join(os.path.dirname(__file__), filename)).read()


setup(
    name='dhcpkit_kafka',
    version=dhcpkit_kafka.__version__,

    description='Kafka extensions to DHCPKit',
    long_description=read('README.rst'),
    keywords='dhcp server ipv6 kafka looking-glass',
    url='https://github.com/sjm-steffann/dhcpkit_kafka',
    license='GPLv3',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Environment :: Console',
        'Environment :: No Input/Output (Daemon)',
        'Environment :: Web Environment',
        'Framework :: Django',
        'Intended Audience :: Developers',
        'Intended Audience :: Information Technology',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Natural Language :: English',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX',
        'Operating System :: Unix',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Topic :: Internet',
        'Topic :: System :: Networking',
        'Topic :: System :: Systems Administration',
    ],

    packages=find_packages(exclude=['tests', 'tests.*']),
    include_package_data=True,
    entry_points={
        'dhcpkit_kafka.messages': [
            '1 = dhcpkit_kafka.messages:DHCPKafkaMessage',
        ],
        'dhcpkit.ipv6.server.extensions': [
            'kafka = dhcpkit_kafka.server_extension',
        ],
    },

    install_requires=[
        'dhcpkit >= 0.9.0',
        'pykafka',
    ],

    test_suite='dhcpkit_kafka.tests',

    author='Sander Steffann',
    author_email='sander@steffann.nl',

    maintainer='Sander Steffann',
    maintainer_email='sander@steffann.nl',

    zip_safe=False,
)
