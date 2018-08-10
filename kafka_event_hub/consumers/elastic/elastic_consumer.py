from kafka_event_hub.consumers.base_consumer import AbstractBaseConsumer
from kafka_event_hub.config import BaseConfig

from simple_elastic import ElasticIndex

from typing import Callable


class ElasticConsumer(AbstractBaseConsumer):

    def __init__(self, config_path: str):
        super().__init__(config_path, BaseConfig)
        self._index = ElasticIndex(**self.configuration['Elastic'])
        self._update_func = None
        self._filter_func = None
        self._transformation_func = None
        self._identifier_key = 'identifier'

    @property
    def identifier_key(self):
        return self._identifier_key

    @identifier_key.setter
    def identifier_key(self, value):
        self._identifier_key = value

    def set_filter_policy(self, func: Callable[[str], bool]):
        self._filter_func = func

    def set_transformation_policy(self, func: Callable[[str], dict]):
        self._transformation_func = func

    def set_update_policy(self, func: Callable[[dict, dict], str]):
        self._update_func = func

    def consume(self, num_messages: int = 1, timeout: int = -1):
        message = self._consumer.consume(num_messages, timeout)
        if message.error() is None:
            key = message.key()
            value = message.value()
            if value is None:
                raise ValueError('Message has no value.')

            value = value.decode('utf-8')

            if self._filter_func(value):
                value = self._transformation_func(value)

                if key is not None:
                    key = key.decode('utf-8')
                    record = self._index.get(key)
                    if record is not None:
                        value = self._update_func(value, record)

                self._index.index_into(value, value[self._identifier_key])
        else:
            # this is an event. Discard these messages.
            pass
