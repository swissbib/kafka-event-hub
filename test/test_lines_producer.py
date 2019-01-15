import sys
import os
import logging
import pytest

logging.basicConfig(filename='logs/line_producer.log', filemode='w', level=logging.ERROR)

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from kafka_event_hub.producers import LineProducer
from kafka_event_hub.consumers import SimpleConsumer
from kafka import KafkaAdminClient

class TestLineProducer(object):

    def setup_class(self):
        self.admin = KafkaAdminClient(bootstrap_servers='localhost:9092')
        self.producer = LineProducer('configs/lines/producer.yml')
        self.producer_gz = LineProducer('configs/lines/producer_gz.yml')
        self.consumer = SimpleConsumer('configs/lines/consumer.yml')
        self.consumer_gz = SimpleConsumer('configs/lines/consumer_gz.yml')

    def teardown_class(self):
        self.consumer.close()
        self.consumer_gz.close()

        self.admin.delete_topics(['test-lines-gz-v3'])
        self.admin.delete_topics(['test-lines-v3'])
        self.admin.close()

    #@pytest.mark.skip()
    def test_produce(self):
        self.producer.process()
        key = None
        message = None
        while key is None and message is None:
            key, message = self.consumer.consume()
        assert key == '0'
        assert message == "This is a line"
        key = None
        message = None
        while key is None and message is None:
            key, message = self.consumer.consume()
        assert key == '1'
        assert message == "and another line"
        key = None
        message = None
        while key is None and message is None:
            key, message = self.consumer.consume()
        assert key == '2'
        assert message == "a third line"
        key = None
        message = None
        while key is None and message is None:
            key, message = self.consumer.consume()
        assert key == '3'
        assert message == "a forth line"
        key = None
        message = None
        while key is None and message is None:
            key, message = self.consumer.consume()
        assert key == '4'
        assert message == "a lot of lines now"

    #@pytest.mark.skip("Currently way too slow")
    def test_produce_gz(self):                
        self.producer_gz.process()
        key = None
        message = None
        while key is None and message is None:
            key, message = self.consumer_gz.consume()
        assert key == '0'
        assert message == "This is a line"
        key = None
        message = None
        while key is None and message is None:
            key, message = self.consumer_gz.consume()
        assert key == '1'
        assert message == "and another line"
        key = None
        message = None
        while key is None and message is None:
            key, message = self.consumer_gz.consume()
        assert key == '2'
        assert message == "a third line"
        key = None
        message = None
        while key is None and message is None:
            key, message = self.consumer_gz.consume()
        assert key == '3'
        assert message == "a forth line"
        key = None
        message = None
        while key is None and message is None:
            key, message = self.consumer_gz.consume()
        assert key == '4'
        assert message == "a lot of lines now"