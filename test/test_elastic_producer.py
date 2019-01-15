import sys
import os
import logging
import pytest

logging.basicConfig(filename='logs/elastic-producer.log', filemode='w', level=logging.ERROR)

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from kafka_event_hub.producers import ElasticProducer
from kafka_event_hub.consumers import SimpleConsumer

from kafka import KafkaAdminClient
from simple_elastic import ElasticIndex


class TestElasticProducer(object):

    def setup_class(self):
        self.admin = KafkaAdminClient(bootstrap_servers='localhost:9092')
        self.index = ElasticIndex('test-elastic-producer', 'doc')
        self.index.index_into({'test': 1}, 0)
        self.index.index_into({'test': 2}, 1)
        self.index.index_into({'test': 3}, 2)
        self.index.index_into({'test': 4}, 3)
        self.index.index_into({'test': 5}, 4)

        self.producer = ElasticProducer('configs/elastic/test_elastic_producer_producer.yml')
        self.consumer = SimpleConsumer('configs/elastic/test_elastic_producer_consumer.yml')

    def teardown_class(self):
        self.consumer.close()
        self.admin.delete_topics(['test-elastic-producer'])
        self.admin.close()
        self.index.delete()

    #@pytest.mark.skip()
    def test_produce(self):
        self.producer.process()
        key, message = self.consumer.consume()
        assert key == '0'
        assert message == '{"test": 1}'
        

