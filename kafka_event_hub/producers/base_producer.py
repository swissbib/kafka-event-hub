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
from confluent_kafka.admin import AdminClient, NewPartitions, NewTopic

import logging


def basic_callback(err, msg):
    if err is not None:
        logging.error('Message delivery failed: %s.', err)
    else:
        logging.info('Message delivered to %s [%s].', msg.topic(), msg.partition())


class AbstractBaseProducer(object):

    def __init__(self, config: str, config_parser: type(BaseConfig), call_back=None, logger=logging.getLogger(__name__)):
        self._logger = logger
        self._configuration = config_parser(config)
        self._admin = AdminClient(**self._configuration['AdminClient'])
        self._prepare_topic()
        self._producer = Producer(**self._configuration['Producer'])
        self._call_back = basic_callback if call_back is None else call_back

    @property
    def configuration(self):
        return self._configuration

    def _prepare_topic(self):
        fs = self._admin.create_topics([NewTopic(**self._configuration['Topic'])])

        for topic,f in fs.items():
            try:
                f.result()
                self._logger.info('Topic %s created.', topic)
            except Exception as e:
                self._logger.error('Failed to create topic %s: %s', topic, e)

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

    def _produce_kafka_message(self, key: str, value: str, **kwargs):
        if isinstance(value, list):
            self._logger.error('LIST LIST LIST LIST %s LIST LIST LIST LIST', value)

        self._producer.produce(self._configuration['Topic']['topic'], key=key.encode('utf-8'),
                               value=value.encode('utf-8'), callback=self._call_back, **kwargs)

    def _poll(self, timeout=0):
        self._producer.poll(timeout=timeout)

    def _flush(self, timeout=0):
        self._producer.flush(timeout)

    def list_topics(self):
        return self._producer.list_topics()

    def __len__(self):
        return self._producer.__len__()

