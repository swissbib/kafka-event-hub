from kafka_event_hub.consumers.base_consumer import AbstractBaseConsumer
from kafka_event_hub.config import BaseConfig

import logging


class SimpleConsumer(AbstractBaseConsumer):

    def __init__(self, config_path: str, logger=logging.getLogger(__name__)):
        super().__init__(config_path, BaseConfig, logger)

    def consume(self, num_messages: int = 1, timeout: int = -1):
        messages = self._consumer.consume(num_messages, timeout)
        for message in messages:
            if message.error():
                self._logger.error(message.error())
            else:
                yield message.key().decode('utf-8'), message.value().decode('utf-8')
