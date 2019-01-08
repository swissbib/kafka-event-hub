import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from kafka_event_hub.consumers import ElasticConsumer


class TestElasticConsumer(object):

    def setup_class(self):
        self.consumer = ElasticConsumer('configs/elastic/elastic.yml')

    def test_consume(self):
        self.consumer.consume()
