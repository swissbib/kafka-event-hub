import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from kafka_event_hub.producers import ElasticProducer


class TestElasticProducer(object):

    def setup_class(self):
        self.producer = ElasticProducer('test/configs/elastic/elastic_test.yml')

    def test_produce(self):
        self.producer.process()
