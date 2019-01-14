import logging

from kafka_event_hub.config import ConsumerConfig
from kafka_event_hub.consumers.base_consumer import AbstractBaseConsumer

class SimpleConsumer(AbstractBaseConsumer):
    """
    Consumes a subscribed topic and returns one key / message pair at a time.
    Keys and messages are returned as strings.
    """

    def __init__(self, config_path: str, logger=logging.getLogger(__name__)):
        super().__init__(config_path, ConsumerConfig, logger)

    def consume(self, timeout: int = None) -> (str, str):
        message = next(self._consumer)
        self._logger.debug('Received message: {} with key {}'.format(message.value.decode('utf-8'), message.key.decode('utf-8')))
        return message.key.decode('utf-8'), message.value.decode('utf-8')
