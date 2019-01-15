import sys
import os
import logging
import pytest

logging.basicConfig(filename='logs/test-elastic-consumer.log', filemode='w', level=logging.ERROR)


sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from kafka_event_hub.consumers import SimpleElasticConsumer
from kafka_event_hub.producers import LineProducer

from kafka import KafkaAdminClient

class TestSimpleElasticConsumer(object):

    def setup_class(self):
        self.admin = KafkaAdminClient(bootstrap_servers='localhost:9092')
        self.consumer = SimpleElasticConsumer('configs/elastic/elastic-consumer-simple-test.yml')
        self.producer = LineProducer("configs/elastic/json-lines-producer.yml")

    def teardown_class(self):
        self.admin.delete_topics(['json'])
        self.consumer.close()


    # @pytest.mark.skip()
    def test_consume(self):
        self.producer.process()
        
        count = 0
        while self.consumer.consume():
            count += 1
            if count == 4:
                break
