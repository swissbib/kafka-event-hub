import sys
import os
import logging
import pytest

logging.basicConfig(filename='logs/line-producer-tests.log', filemode='w', level=logging.DEBUG)

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from kafka_event_hub.producers import SimpleProducer
from kafka_event_hub.consumers import SimpleConsumer


class TestSimpleProducer(object):

    def setup_class(self):
        self.producer = SimpleProducer('configs/simple/producer.yml')

    def test_simple(self):
        self.producer.send(key='100'.encode('utf8'), message='We are perfect'.encode('utf8'))
