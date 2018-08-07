from kafka_event_hub.producers.base_producer import AbstractBaseProducer
from kafka_event_hub.config import BaseConfig

import requests
import json


class SRUProducer(AbstractBaseProducer):
    """

    """
    _domain = 'http://sru.swissbib.ch/sru/search/'

    _schemas = {
        'marc/xml': 'info:srw/schema/1/marcxml-v1.1-light',
        'dc/xml': 'info:srw/schema/1/dc-v1.1-light',
        'marc/json': 'info:srw/schema/json'
    }

    def __init__(self, configuration: BaseConfig):
        super().__init__(configuration)
        self._search = list()
        self._db = self.configuration['SRU']['database']
        self._schema = self._schemas[self.configuration['SRU']['schema']]
        self._max_records = self.configuration['SRU']['max_records']
        for query in self.configuration['SRU']['queries']:
            self._search.append(query)
        self._record_count = 0

    def _params(self, start_record):
        return {
            'query': self._query(),
            'operation': 'searchRetrieve',
            'recordSchema': self._schemas[self._schema],
            'maximumRecords': self._max_records,
            'startRecord': start_record,
            'recordPacking': 'XML',
            'availableDBs': self._db
        }

    def _query(self):
        if len(self._search) == 1:
            return '{} {} {}'.format(self._search[0]['name'], self._search[0]['relation'], self._search[0]['value'])

    def process(self):
        """Load all MARCJSON records from SRU with the given query into Kafka"""
        response = requests.get(self._domain + self._db, params=self._params(0))
        if response.ok:
            records = json.loads(response.text)
            self._record_count += len(records['collection'])
            for record in records['collection']:
                self._produce_kafka_message(json.dumps(record))
            while records['numberOfRecords'] > self._record_count:
                response = requests.get(self._domain + self._db, params=self._params(records['startRecord'] + len(records['collection'])))
                if response.ok:
                    records = json.loads(response.text)
                    for record in records['collection']:
                        self._produce_kafka_message(json.dumps(record))










