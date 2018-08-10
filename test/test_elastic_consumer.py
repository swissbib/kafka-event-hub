import logging
import os
import sys

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from kafka_event_hub.consumers import ElasticConsumer


class TestElasticConsumer(object):

    def setup_class(self):
        self.consumer = ElasticConsumer('configs/elastic_consumer/elastic_test.yml')

    def test_consume(self):
        self.consumer.consume()


if __name__ == '__main__':
    consumer = TestElasticConsumer()
    consumer.setup_class()
    consumer.test_consume()