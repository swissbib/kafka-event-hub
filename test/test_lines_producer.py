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
        self.consumer = SimpleConsumer('configs/lines/consumer.yml')

    def test_produce(self):
        self.producer.process()
        logging.info("Finished producing.")
        for key, message in self.consumer.consume():
            assert len(message) > 0
            assert len(key) == 0
            logging.info("consumed message %s with key %s", message, key)
          
