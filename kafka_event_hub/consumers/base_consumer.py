from kafka_event_hub.config import ConsumerConfig
from kafka import KafkaConsumer, TopicPartition
from typing import List
import logging


class AbstractBaseConsumer(object):

    def __init__(self, config: str, config_class: type(ConsumerConfig), **kwargs):
        """

        Consumer configs:
        
        """
        self._configuration = config_class(config)
        self._consumer = KafkaConsumer(**self.configuration.consumer)

        self._error_logger = logging.getLogger(self.configuration.logger_name + '-errors')
        if 'handler' in kwargs and isinstance(kwargs['handler'], logging.Handler):
            self._error_logger.addHandler(kwargs['handler'])

        error_handler = logging.FileHandler(self.configuration.errorlogging)
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(logging.Formatter('%(asctime)s:%(levelname)s:%(name)s:%(message)s'))
        self._error_logger.addHandler(error_handler)
        self._error_logger.propagate = False

        self._time_logger = logging.getLogger(self.configuration.logger_name + '-summary')
        time_handler = logging.FileHandler(self.configuration.logging)
        time_handler.setLevel(logging.INFO)
        time_handler.setFormatter(logging.Formatter('%(asctime)s:%(levelname)s:%(name)s:%(message)s'))
        self._time_logger.addHandler(time_handler)
        self._time_logger.propagate = False
        self._time_logger.info("Initialized Consumer {}.".format(self.configuration.consumer['client_id']))

        self.subscribe(self.configuration.topic)

    @property
    def configuration(self) -> ConsumerConfig:
        return self._configuration

    def assign(self, topic: TopicPartition):
        self._consumer.assign([topic])

    def close(self):
        self._consumer.close()

    def subscribe(self, topics: List[str]):
        self._consumer.subscribe(topics)
        self._time_logger.info("Subscribed to topics: {}".format(topics))

    def unsubscribe(self):
        self._consumer.unsubscribe()
