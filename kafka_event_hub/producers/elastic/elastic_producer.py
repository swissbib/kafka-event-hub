from kafka_event_hub.producers.base_producer import AbstractBaseProducer
from kafka_event_hub.config import BaseConfig

from simple_elastic import ElasticIndex
import json


class ElasticProducer(AbstractBaseProducer):

    def __init__(self, config: str):
        super().__init__(config, config_parser=BaseConfig)
        self._index = ElasticIndex(**self.configuration['ElasticIndex'])
        self._scroll = self.configuration['Scroll']

    def process(self):
        for results in self._index.scroll(**self._scroll):
            for record in results:
                self._produce_kafka_message(key=record[self.configuration['identifier_key']],
                                            value=json.dumps(record, ensure_ascii=False))
