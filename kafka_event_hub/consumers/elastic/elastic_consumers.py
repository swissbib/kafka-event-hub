from kafka_event_hub.consumers.base_consumer import AbstractBaseConsumer
from kafka_event_hub.config import ElasticConsumerConfig

from simple_elastic import ElasticIndex

from kafka import OffsetAndMetadata

import json
import logging


class SimpleElasticConsumer(AbstractBaseConsumer):
  """
  A KafkaConsumer which consumes messages and indexes them into a ElasticIndex one by one.

  Requires the following configs:

      Consumer:
        bootstrap_servers: localhost:9092
        client_id: test
        group_id: elastic-consumer-test
        auto_offset_reset: earliest
      Topics:
        - test
      ElasticIndex:
        index: name-of-index
        doc_type: _doc (default value for elasticsearch 6)
        url: http://localhost:9200
        timeout: 300

  """
  
  def __init__(self, config, config_class=ElasticConsumerConfig, logger=logging.getLogger(__name__)):
    super().__init__(config, config_class, logger=logger)
    self._index = ElasticIndex(**self.configuration.elastic_settings)

  def consume(self) -> bool:
    """
    Consumes a single message from the subscribed topic and indexes it into the elasticsearch index.

    Returns True if successful, False otherwise.
    """
    message = next(self._consumer)

    key = message.key.decode('utf-8')
    value = json.loads(message.value.decode('utf-8'))

    self._logger.debug("Key: %s", key)
    self._logger.debug("Value: %s", value)
    result = self._index.index_into(value, key)

    if result:
      for assignment in self._consumer.assignment():
        pos = self._consumer.position(assignment)
        if pos != self._consumer.committed(assignment):
          self._consumer.commit({assignment: OffsetAndMetadata(pos, "")})

    return result


class BulkElasticConsumer(AbstractBaseConsumer):
  """
  Will attempt to collect a number of messages and then bulk index them. Collection will either wait some time or collect
  10'000 messages.


  Consumer:
    bootstrap_servers: localhost:9092
    client_id: test
    group_id: elastic-consumer-test
    auto_offset_reset: earliest
  Topics:
    - test
  ElasticIndex:
    index: name-of-index
    doc_type: _doc (default value for elasticsearch 6)
    url: http://localhost:9200
    timeout: 300
  IdentifierKey: name-of-key-value (optional, if not specified the Kafka key value will be used.)
  """

  def __init__(self, config, config_class=ElasticConsumerConfig, logger=logging.getLogger(__name__)):
    super().__init__(config, config_class, logger=logger)
    self._index = ElasticIndex(**self.configuration.elastic_settings)
    self._key = self.configuration.id_name

  def consume(self) -> bool:
    data = list()
    messages = self._consumer.poll(100, 10000)

    if messages:
      # TODO: Currently probably only works if there is a single partition. needs a more robust implementation.
      for message in messages[self._consumer.assignment().pop()]:
        key = message.key.decode('utf-8')
        value = json.loads(message.value.decode('utf-8'))

        self._logger.debug("Key: %s", key)
        self._logger.debug("Value: %s", value)

        if self._key not in value:
          value['_key'] = key
        data.append(value)

    result = self._index.bulk(data, self._key)

    if result:
      for assignment in self._consumer.assignment():
        pos = self._consumer.position(assignment)
        if pos != self._consumer.committed(assignment):
          self._consumer.commit({assignment: OffsetAndMetadata(pos, "")})

    return result