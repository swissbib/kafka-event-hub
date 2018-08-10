from kafka_event_hub.config import BaseConfig

from confluent_kafka.admin import AdminClient, ClusterMetadata
from confluent_kafka import Consumer, Message, TopicPartition

from typing import List


class AbstractBaseConsumer(object):

    def __init__(self, config: str, config_class: type(BaseConfig)):
        self._configuration = config_class(config)
        self._admin = AdminClient(**self._configuration['AdminClient'])
        self._consumer = Consumer(**self._configuration['Consumer'])

    @property
    def configuration(self):
        return self._configuration

    @property
    def assignment(self) -> List[TopicPartition]:
        return self._consumer.assignment()

    def close(self):
        self._consumer.close()

    def subscribe(self, topics: List[str]):
        self._consumer.subscribe(topics)

    def unsubscribe(self):
        self._consumer.unsubscribe()

    def consume(self, num_messages: int = 1, timeout: int = -1) -> List[Message]:
        return self._consumer.consume(num_messages=num_messages, timeout=timeout)

    def list_topics(self, topic: str = None, timeout: int = -1) -> ClusterMetadata:
        return self._consumer.list_topics(topic, timeout)




