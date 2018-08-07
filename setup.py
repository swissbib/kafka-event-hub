#!/usr/bin/env python

import os.path
import re

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup
import sys

if sys.argv[-1] == 'publish':
    os.system("python setup.py sdist")
    os.system('twine upload dist/* -r pypi')
    sys.exit()


def read(filename):
    return open(os.path.join(os.path.dirname(__file__), filename)).read()

description = 'Swissbib Kafka Event Hub'

long_description = """
A small library which implements various producers and consumers for the Swissbib Kafka Event Hub.

A wide range of specific data sources and data target will be covered.
"""

version = re.search(r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
                    read('kafka_event_hub/__init__.py'), re.MULTILINE).group(1)

install_require = ['confluent-kafka', 'PyYAML', 'Sickle', 'PyMongo', 'requests']

setup(
    name='swissbib_kafka_event_hub',
    packages=['kafka_event_hub'],
    description=description,
    version=version,
    author='Jonas Waeber',
    author_email='jonas.waeber@unibas.ch',
    url='https://github.com/swissbib/kafka_event_hub',
    keywords=['kafka', 'swissbib', 'oai', 'mongodb'],
    install_requires=install_require,
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Topic :: Software Development :: Libraries :: Python Modules"
        ],
    license='GPL-3.0'
    )
