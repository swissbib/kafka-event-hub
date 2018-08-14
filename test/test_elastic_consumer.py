import logging
import os
import sys





class TestElasticConsumer(object):

    def setup_class(self):
        sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        from kafka_event_hub.consumers import ElasticConsumer
        self.consumer = ElasticConsumer('configs/elastic/elastic.yml')

    def test_consume(self):
        self.consumer.consume()


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    consumer = TestElasticConsumer()
    logging.debug('Setup class')
    consumer.setup_class()
    logging.debug('Consume message')
    consumer.test_consume()