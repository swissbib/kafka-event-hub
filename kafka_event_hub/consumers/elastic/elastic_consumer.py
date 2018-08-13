from kafka_event_hub.consumers.base_consumer import AbstractBaseConsumer
from kafka_event_hub.config import BaseConfig

from simple_elastic import ElasticIndex

from typing import Callable
import logging


def no_pre_filter(message):
    return False


def no_after_filter(message):
    return False


def no_update(new, old):
    return new


def no_transformation(message):
    return message


class ElasticConsumer(AbstractBaseConsumer):

    def __init__(self, config_path: str, logger=logging.getLogger(__name__)):
        super().__init__(config_path, BaseConfig, logger)
        self._index = ElasticIndex(**self.configuration['Elastic'])
        self._update_func = no_update
        self._pre_filter_func = no_pre_filter
        self._transformation_func = no_transformation
        self._after_filter_func = no_after_filter
        self._identifier_key = 'identifier'

    @property
    def identifier_key(self):
        return self._identifier_key

    @identifier_key.setter
    def identifier_key(self, value):
        self._identifier_key = value

    def set_pre_filter_policy(self, func: Callable[[str], bool]):
        """If this evaluates to true with a given message this message is dropped."""
        self._pre_filter_func = func

    def set_after_filter_policy(self, func: Callable[[dict], bool]):
        """If this evaluates to true with a given message this message is dropped."""
        self._after_filter_func = func

    def set_transformation_policy(self, func: Callable[[str], dict]):
        self._transformation_func = func

    def set_update_policy(self, func: Callable[[dict, dict], dict]):
        self._update_func = func

    def consume(self, num_messages: int = 1, timeout: int = -1):
        messages = self._consumer.consume(num_messages, timeout)
        for message in messages:
            if message.error() is None:
                key = message.key()
                value = message.value()
                if value is None:
                    self._logger.error('Message had no value. Skipped.')

                value = value.decode('utf-8')

                if not self._pre_filter_func(value):
                    value = self._transformation_func(value)

                    if not self._after_filter_func(value):
                        if key is not None:
                            key = key.decode('utf-8')
                            record = self._index.get(key)
                            if record is not None:
                                value = self._update_func(value, record)
                                self._logger.info('Message was updated before indexing.')

                        self._index.index_into(value, key if key is not None else value[self._identifier_key])
                        self._logger.debug('Message was successfully index!')
                    else:
                        self._logger.info('Message was filtered after transformation: %s.', value)
                else:
                    self._logger.info('Message was filtered before transformation: %s.', value)
            else:
                self._logger.error('Received an event instead of an message.')
                pass
