from kafka_event_hub.consumers.base_consumer import AbstractBaseConsumer
from kafka_event_hub.consumers.utility import DataTransformation
from kafka_event_hub.config import BaseConfig

from simple_elastic import ElasticIndex

import logging


class ElasticConsumer(AbstractBaseConsumer):

    def __init__(self, config_path: str,
                 transformation_class: type(DataTransformation) = DataTransformation,
                 logger=logging.getLogger(__name__)):
        super().__init__(config_path, BaseConfig, logger)
        self._index = ElasticIndex(**self.configuration['Elastic'])
        self._transform = transformation_class(**self.configuration['DataTransformation'])
        self._identifier_key = 'identifier'

    def consume(self, num_messages: int = 1, timeout: int = -1):
        messages = self._consumer.consume(num_messages, timeout)
        for message in messages:
            if message.error() is None:
                identifier, value = self._transform.run(message)
                if identifier != '' and len(value.keys()) != 0:
                    self._index.index_into(value, identifier)
                else:
                    self._logger.error('Could not transform message: %s.', message.value())
            else:
                self._logger.error('Received event: %s', message.error())
