from kafka_event_hub.config import BaseConfig

from kafka import KafkaConsumer, TopicPartition

from typing import List
import logging


class AbstractBaseConsumer(object):

    def __init__(self, config: str, config_class: type(BaseConfig), logger=logging.getLogger(__name__)):
        """

        Consumer configs:
        
        """
        self._configuration = config_class(config)
        self._consumer = KafkaConsumer(**self.configuration.consumer)
        self.subscribe(self.configuration.topic)
        self._logger = logger

    @property
    def configuration(self):
        return self._configuration

    def assign(self, topic: TopicPartition):
        self._consumer.assign([topic])

    def close(self):
        self._consumer.close()

    def subscribe(self, topics: List[str]):
        self._consumer.subscribe(topics)

    def unsubscribe(self):
        self._consumer.unsubscribe()
