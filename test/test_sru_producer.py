import logging
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from kafka_event_hub.producers import SRUProducer

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


class TestSRUProducer(object):

    def setup_class(self):
        self.producer = SRUProducer('configs/sru/dsv05_dump.yml')

    def test_init(self):
        p = SRUProducer('configs/sru/dsv05_dump.yml')
        assert p.configuration['SRU']['database'] == 'defaultdb'
        assert p.configuration['Producer']['debug'] == 'all'

    def test_producer(self):
        self.producer.set_query_id_equal_with('HAN000214657')
        self.producer.process()

    def test_list_topics(self):
        assert self.producer.list_topics()


if __name__ == '__main__':
    tests = TestSRUProducer()
    tests.setup_class()
    tests.test_producer()
    tests.test_list_topics()




