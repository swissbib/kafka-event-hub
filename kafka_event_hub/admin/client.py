from typing import Dict

from kafka import KafkaAdminClient
from kafka.admin import NewTopic


class AdminClient(object):

    def __init__(self, **configs):
        self._instance = KafkaAdminClient(**configs)

    def create_topic(self, name: str, num_partitions: int, replication_factor: int, **configs: Dict[str, str]):
        """

        See https://kafka.apache.org/documentation/#topicconfigs for topic configs options.

        :param name:                The name of the new topic.
        :param num_partitions:      The number of partitions within this topic.
        :param replication_factor:  The number of replicas to be created.
        :param configs:             Configs as a dict {str: str}.
        :return:                    Version of CreateTopicResponse class.
        """
        topic = NewTopic(name, num_partitions, replication_factor, topic_configs=configs)
        return self._instance.create_topics([topic])
