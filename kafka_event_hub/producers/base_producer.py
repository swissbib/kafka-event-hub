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
from kafka import KafkaProducer
import logging


def callback_success(record_metadata):
    logging.info("Message delivered to topic %s, parition %s with offset %s.", record_metadata.topic, record_metadata.partition, record_metadata.offset)


def callback_error(excpt):
    logging.error("An error occured: ", exc_info=excpt)
    # TODO: Implement error handling if necessary!


class AbstractBaseProducer(object):

    def __init__(self, config: str, config_parser: type(BaseConfig), callback_success_param=None, callback_error_param=None, logger=logging.getLogger(__name__)):
        self._logger = logger
        self._configuration = config_parser(config)
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
        self._producer.send(self.configuration.topic, **{'value': message, 'key': key})\
            .add_callback(self._callback_success)\
            .add_errback(self._callback_error)

    def process(self):
        raise NotImplementedError("Implement process to enable the behaviour.")
        
    def flush(self):
        self._producer.flush()

    def close(self):
        self._producer.close()
