import logging

from kafka_event_hub.config import BaseConfig
from kafka_event_hub.consumers.base_consumer import AbstractBaseConsumer


class SimpleConsumer(AbstractBaseConsumer):

    def __init__(self, config_path: str, logger=logging.getLogger(__name__)):
        super().__init__(config_path, BaseConfig, logger)

    def consume(self, timeout: int = None) -> (str, str):
        msg = c.poll(10)

        if msg is None:
            return None, None

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                return None, None
            else:
                logging.error(msg.error())
                return None, None

        key = msg.key().decode('utf-8')
        value = msg.value().decode('utf-8')

        logging.debug('Received message: {} with key {}'.format(value, key))
        return key, value
