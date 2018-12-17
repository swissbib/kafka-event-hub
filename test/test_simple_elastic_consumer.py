import sys
import os
import logging

logging.basicConfig(filename='logs/test-elastic-consumer.log', filemode='w', level=logging.DEBUG)


sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from kafka_event_hub.consumers import SimpleElasticConsumer
from kafka_event_hub.producers import LineProducer

from confluent_kafka.admin import AdminClient

class TestSimpleElasticConsumer(object):

    def setup_class(self):
        self.consumer = SimpleElasticConsumer('configs/elastic/elastic-consumer-simple-test.yml')
        self.producer = LineProducer("configs/elastic/json-lines-producer.yml")

    def test_consume(self):
        self.producer.process()
        while True:
            value = self.consumer.consume()
            if not value:
                break

    def teardown_class(self):
        self.consumer.delete_topic()
