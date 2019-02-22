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
from kafka_event_hub.config import ProducerConfig
from kafka_event_hub.admin import AdminClient
from kafka import KafkaProducer
from kafka.errors import TopicAlreadyExistsError
import logging


class AbstractBaseProducer(object):
    """
    Accepts an logger handler to log to a specific location.
    """

    def __init__(self, config: str, config_parser: type(ProducerConfig),
                 config_special: str=None,
                 callback_success_param=None,
                 callback_error_param=None,
                 **kwargs):
        self._configuration = config_parser(config, config_special)
        self._error_logger = logging.getLogger(self.configuration.logger_name + '-errors')
        if 'handler' in kwargs and isinstance(kwargs['handler'], logging.Handler):
            self._error_logger.addHandler(kwargs['handler'])

        error_handler = logging.FileHandler(self.configuration.errorlogging)
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(logging.Formatter('%(asctime)s:%(levelname)s:%(name)s:%(message)s'))
        self._error_logger.addHandler(error_handler)

        self._time_logger = logging.getLogger(self.configuration.logger_name + '-summary')
        time_handler = logging.FileHandler(self.configuration.logging)
        time_handler.setLevel(logging.INFO)
        time_handler.setFormatter(logging.Formatter('%(asctime)s:%(levelname)s:%(name)s:%(message)s'))
        self._time_logger.addHandler(time_handler)
        self._init_kafka(callback_success_param, callback_error_param)

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
        self._time_logger.info("All messages from the broker received.")

    def close(self):
        self._producer.close()
        self._time_logger.info("End process.")

    def callback_success(self, record_metadata):
        self._error_logger.info("Message delivered to topic %s, partition %s with offset %s.",
                     record_metadata.topic,
                     record_metadata.partition,
                     record_metadata.offset)

    def callback_error(self, exception):
        self._error_logger.error("An error occurred: ", exc_info=exception)

    def _init_kafka(self, callback_success_param=None, callback_error_param=None):
        self._admin = AdminClient(**self.configuration.admin)
        try:
            self._admin.create_topic(**self.configuration.topic)
            # ValueError is sent due to a bug in kafka-python 1.4.4 (fixed on master branch commit: 2e0ada0
            # but the topic is already created at that point!
        except (TopicAlreadyExistsError, ValueError):
            pass
        self._producer = KafkaProducer(**self.configuration.producer)
        self._callback_success = self.callback_success if callback_success_param is None else callback_success_param
        self._callback_error = self.callback_error if callback_error_param is None else callback_error_param
        self._time_logger.info("Initialization Completed. Begin processing.")
