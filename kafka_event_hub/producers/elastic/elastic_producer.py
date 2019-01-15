from kafka_event_hub.producers.base_producer import AbstractBaseProducer
from kafka_event_hub.config import ElasticProducerConfig

from simple_elastic import ElasticIndex
import json


class ElasticProducer(AbstractBaseProducer):

    def __init__(self, config: str):
        super().__init__(config, config_parser=ElasticProducerConfig)
        self._index = ElasticIndex(**self.configuration.elastic_settings)

    def process(self):
        for results in self._index.scroll(**self.configuration.scroll):
            for record in results:
                key: str = record['_id']
                value: str = json.dumps(record['_source'])
                self.send(key.encode('utf-8'), value.encode('utf-8'))
        
        self.flush()
        self.close()
