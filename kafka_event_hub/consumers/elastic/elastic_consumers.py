from kafka_event_hub.consumers.base_consumer import AbstractBaseConsumer
from kafka_event_hub.consumers.utility import DataTransformation
from kafka_event_hub.config import BaseConfig

from simple_elastic import ElasticIndex

import time
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
  
  def __init__(self, config, config_class=BaseConfig, logger=logging.getLogger(__name__)):
    super().__init__(config, config_class, logger=logger)
    self._index = ElasticIndex(**self.configuration['ElasticIndex'], enable_auto_commit=False)

  def consume(self) -> bool:
    """
    Consumes a single message from the subscribed topic and indexes it into the elasticsearch index.

    Returns True if successfull, False otherwise.
    """
    message = next(self._consumer)

    key = message.key.decode('utf-8')
    value = json.loads(message.value.decode('utf-8'))
  
    self._logger.debug("Key: %s", key)
    self._logger.debug("Value: %s", value)
    result = self._index.index_into(value, key)
    
    if result:
      next_position = self._consumer.position()
      self._consumer.commit(next_position)
    
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
  `key`  name-of-key-value (optional) -> Default will use the key field of the Kafka Message to define the unique _id. 
  """
  
  def __init__(self, config, config_class=BaseConfig, logger=logging.getLogger(__name__)):
    super().__init__(config, config_class, logger=logger)
    self._index = ElasticIndex(**self.configuration['ElasticIndex'], enable_auto_commit=False)
    try:
      self._key = self.configuration['key']
    except KeyError:
      self._key = '_key'

  def consume(self) -> bool:
    data = list()
    current = time.time()
    messages = self._consumer.poll(100, 10000)

    for message in messages:
      key = message.key.decode('utf-8')
      value = json.loads(message.value.decode('utf-8'))
        
      self._logger.debug("Key: %s", key)
      self._logger.debug("Value: %s", value)

      if self._key not in value:
        value['_key'] = key
      data.append(value)

    result = self._index.bulk(data, self._key)

    if result:
      next_position = self._consumer.position(self.assignment())
      self._consumer.commit(next_position)
    
    return result