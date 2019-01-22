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
from kafka_event_hub.admin import AdminClient
from kafka import KafkaProducer
from kafka.errors import TopicAlreadyExistsError
import logging


def callback_success(record_metadata):
    logging.info("Message delivered to topic %s, parition %s with offset %s.",
                 record_metadata.topic,
                 record_metadata.partition,
                 record_metadata.offset)


def callback_error(exception):
    logging.error("An error occurred: ", exc_info=exception)


class AbstractBaseProducer(object):

    def __init__(self, config: str, config_parser: type(BaseConfig), callback_success_param=None, callback_error_param=None, logger=logging.getLogger(__name__)):
        self._logger = logger
        self._configuration = config_parser(config)
        self._admin = AdminClient(**self.configuration.admin)
        try:
            self._admin.create_topic(**self.configuration.topic)
            # ValueError is sent due to a bug in kafka-python 1.4.4 (fixed on master branch commit: 2e0ada0
            # but the topic is already created at that point!
        except (TopicAlreadyExistsError, ValueError):
            pass
        self._producer = KafkaProducer(**self.configuration.producer)
        self._callback_success = callback_success if callback_success_param is None else callback_success_param
        self._callback_error = callback_error if callback_error_param is None else callback_error_param

    @property
    def configuration(self):
        return self._configuration

    def send(self, key: bytes, message: bytes):
        """Send message with a key to the Kafka cluster.

        Both key and message should be as bytes.
        """
        self._producer.send(self.configuration.topic['name'], **{'value': message, 'key': key})\
            .add_callback(self._callback_success)\
            .add_errback(self._callback_error)

    def process(self):
        raise NotImplementedError("Implement process to enable the behaviour.")
        
    def flush(self):
        self._producer.flush()

    def close(self):
        self._producer.close()
