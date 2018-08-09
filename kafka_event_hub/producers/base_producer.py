# coding: utf-8


__author__ = 'swissbib - UB Basel, Switzerland, Guenter Hipler'
__copyright__ = "Copyright 2018, swissbib project"
__credits__ = []
__license__ = "GNU General Public License v3.0"
__version__ = "0.2"
__maintainer__ = "Guenter Hipler"
__email__ = "guenter.hipler@unibas.ch"
__status__ = "in development"
__description__ = """

                    """
from kafka_event_hub.config import BaseConfig

from confluent_kafka import Producer

import logging


def basic_callback(err, msg):
    if err is not None:
        logging.error('Message delivery failed: %s.', err)
    else:
        logging.info('Message delivered to %s [%s].', msg.topic(), msg.partition())


class AbstractBaseProducer(object):

    def __init__(self, config: str, config_parser: type(BaseConfig), call_back=None):
        self._configuration = config_parser(config)
        config = {'bootstrap.servers': self._configuration['Kafka']['host']}
        self._producer = Producer(**config)
        self._call_back = basic_callback if call_back is None else call_back

    @property
    def configuration(self):
        return self._configuration

    def initialize(self):
        """"""
        pass

    def check_data_source(self):
        """Check the data source whether new data is available."""
        pass

    def process(self):
        """Load data from source and write it into the Kafka topic."""
        pass

    def update_configuration(self):
        """Update the configuration if necessary."""
        self._configuration.store()

    def _produce_kafka_message(self, value, **kwargs):
        self._producer.produce(self._configuration['Kafka']['topicToUse'],
                               value=value.encode('utf-8'), callback=self._call_back, **kwargs)

    def _poll(self, timeout=0):
        self._producer.poll(timeout=timeout)

    def _flush(self):
        self._producer.flush()

