from kafka_event_hub.consumers.base_consumer import AbstractBaseConsumer
from kafka_event_hub.consumers.utility import DataTransformation
from kafka_event_hub.config import BaseConfig

from simple_elastic import ElasticIndex

import json
import logging



class SimpleElasticConsumer(AbstractBaseConsumer):
  
  def __init__(self, config, config_class=BaseConfig, logger=logging.getLogger(__name__)):
    super().__init__(config, config_class, logger=logger)
    self._index = ElasticIndex(**self.configuration['ElasticIndex'])
 
  def consume(self, num_messages=1, timeout=-1) -> bool:
    message = self._consumer.poll(1.0)
    if message is None:
      return False
    if message.error():
      logging.error("Consumer Error: %s", message.error())
      return False
    
    key = message.key().decode('utf-8')
    value = message.value().decode('utf-8')
    logging.debug("Key: %s", key)
    logging.debug("Value: %s", value)
    self._index.index_into(json.loads(value), key)
    return True