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
    self._index = ElasticIndex(**self.configuration['ElasticIndex'])

  def consume(self) -> bool:
    message = next(self._consumer)

    key = message.key.decode('utf-8')
    value = json.loads(message.value.decode('utf-8'))
  
    logging.debug("Key: %s", key)
    logging.debug("Value: %s", value)
    self._index.index_into(value, key)

  def close(self):
    self._consumer.close()


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
    self._index = ElasticIndex(**self.configuration['ElasticIndex'])
    try:
      self._key = self.configuration['key']
    except KeyError:
      self._key = '_key'

  def consume(self) -> bool:
    data = list()
    current = time.time()
    for message in self._consumer:
      key = message.key.decode('utf-8')
      value = json.loads(message.value.decode('utf-8'))
      
      if self._key not in value:
        value['_key'] = key

      logging.debug("Key: %s", key)
      logging.debug("Value: %s", value)

      data.append(value)

      if len(data) >= 10000:
        self._index.bulk(data, '_key')
        return

      if time.time() - current > 500:
        self._index.bulk(data, '_key')
        return


  def close(self):
    self._consumer.close()


class ElasticConsumer(AbstractBaseConsumer):
    """
    Specialized elasticsearch consumer which accepts a transformation class. This transformation class must SubClass `DataTransformation`
    and overwrite the `run(self)` method.

    Each document is indexed one after the other.
    """
    def __init__(self, config_path: str,
                 transformation_class: type(DataTransformation) = DataTransformation,
                 logger=logging.getLogger(__name__)):
        super().__init__(config_path, BaseConfig, logger)
        self._index = ElasticIndex(**self.configuration['Elastic'])
        self._transform = transformation_class(**self.configuration['DataTransformation'])
        self._identifier_key = 'identifier'

    def consume(self, num_messages: int = 1, timeout: int = -1):
        messages = self._consumer.consume(num_messages, timeout)
        for message in messages:
            if message.error() is None:
                identifier, value = self._transform.run(message)
                if identifier != '' and len(value.keys()) != 0:
                    self._index.index_into(value, identifier)
                elif identifier == 'error':
                    self._logger.error('Could not transform message: %s.', message.value())
                elif identifier == 'filter':
                    pass
            else:
                self._logger.error('Received event: %s', message.error())
