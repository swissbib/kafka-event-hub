import sys
import os
import logging

logging.basicConfig(filename='logs/line_producer.log', filemode='w', level=logging.DEBUG)

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from kafka_event_hub.producers import LineProducer
from kafka_event_hub.consumers import SimpleConsumer

class TestLineProducer(object):

    def setup_class(self):
        logging.info("Begin Test")
        self.producer = LineProducer('configs/lines/producer.yml')
        self.producer_gz = LineProducer('configs/lines/producer_gz.yml')
        self.consumer = SimpleConsumer('configs/lines/consumer.yml')
        self.consumer_gz = SimpleConsumer('configs/lines/consumer_gz.yml')

    def teardown_class(self):
        self.consumer.close()
        self.consumer_gz.close()

    def test_produce(self):
        self.producer.process()
        key = None
        message = None
        while key == None and message == None:
            key, message = self.consumer.consume()
        assert key == '0'
        assert message == "This is a line"
        key = None
        message = None
        while key == None and message == None:
            key, message = self.consumer.consume()
        assert key == '1'
        assert message == "and another line"
        key = None
        message = None
        while key == None and message == None:
            key, message = self.consumer.consume()
        assert key == '2'
        assert message == "a third line"
        key = None
        message = None
        while key == None and message == None:
            key, message = self.consumer.consume()
        assert key == '3'
        assert message == "a forth line"
        key = None
        message = None
        while key == None and message == None:
            key, message = self.consumer.consume()
        assert key == '4'
        assert message == "a lot of lines now"

    def test_produce_gz(self):                
        self.producer_gz.process()
        key = None
        message = None
        while key == None and message == None:
            key, message = self.consumer_gz.consume()
        assert key == '0'
        assert message == "This is a line"
        key = None
        message = None
        while key == None and message == None:
            key, message = self.consumer_gz.consume()
        assert key == '1'
        assert message == "and another line"
        key = None
        message = None
        while key == None and message == None:
            key, message = self.consumer_gz.consume()
        assert key == '2'
        assert message == "a third line"
        key = None
        message = None
        while key == None and message == None:
            key, message = self.consumer_gz.consume()
        assert key == '3'
        assert message == "a forth line"
        key = None
        message = None
        while key == None and message == None:
            key, message = self.consumer_gz.consume()
        assert key == '4'
        assert message == "a lot of lines now"