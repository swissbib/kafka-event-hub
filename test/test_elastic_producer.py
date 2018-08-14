import logging
import os
import sys


sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from kafka_event_hub.producers import ElasticProducer


class TestElasticProducer(object):

    def setup_class(self):
        self.producer = ElasticProducer('configs/elastic/elastic_producer.yml')

    def test_produce(self):
        self.producer.process()


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    consumer = TestElasticProducer()
    logging.debug('Setup class')
    consumer.setup_class()
    logging.debug('Produce message')
    consumer.test_produce()