import sys
import os
import logging
import pytest

logging.basicConfig(filename='logs/line-producer-tests.log', filemode='w', level=logging.DEBUG)

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from kafka_event_hub.producers import LineProducer, SortedNTriplesCollectorProducer
from kafka_event_hub.consumers import SimpleConsumer, BulkElasticConsumer
from kafka import KafkaAdminClient
from kafka.errors import UnknownTopicOrPartitionError


class TestLineProducer(object):

    def setup_class(self):
        self.admin = KafkaAdminClient(bootstrap_servers='localhost:9092')
        self.producer = LineProducer('configs/lines/producer.yml')
        self.producer_gz = LineProducer('configs/lines/producer_gz.yml')
        self.producer_bz2 = LineProducer('configs/lines/producer_bz2.yml')
        self.consumer = SimpleConsumer('configs/lines/consumer.yml')
        self.consumer_gz = SimpleConsumer('configs/lines/consumer_gz.yml')
        self.consumer_bz2 = SimpleConsumer('configs/lines/consumer_bz2.yml')

        self.ntriples_producer = SortedNTriplesCollectorProducer('configs/nt/producer.yml')
        self.ntriples_consumer = BulkElasticConsumer('configs/nt/consumer.yml')

    def teardown_class(self):
        self.consumer.close()
        self.consumer_gz.close()
        self.consumer_bz2.close()
        try:
            self.admin.delete_topics(['test-lines-gz'])
        except UnknownTopicOrPartitionError:
            pass
        try:
            self.admin.delete_topics(['test-lines'])
        except UnknownTopicOrPartitionError:
            pass
        try:
            self.admin.delete_topics(['test-lines-bz2'])
        except UnknownTopicOrPartitionError:
            pass
        try:
            self.admin.delete_topics(['test-sorted-nt-resource'])
        except UnknownTopicOrPartitionError:
            pass
        self.admin.close()

    def test_ntriples_producer(self):
        self.ntriples_producer.process()
        assert self.ntriples_consumer.consume()

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

    # @pytest.mark.skip("Currently way too slow")
    def test_produce_bz2(self):
        self.producer_bz2.process()
        key = None
        message = None
        while key is None and message is None:
            key, message = self.consumer_bz2.consume()
        assert key == '0'
        assert message == "This is a line"
        key = None
        message = None
        while key is None and message is None:
            key, message = self.consumer_bz2.consume()
        assert key == '1'
        assert message == "and another line"
        key = None
        message = None
        while key is None and message is None:
            key, message = self.consumer_bz2.consume()
        assert key == '2'
        assert message == "a third line"
        key = None
        message = None
        while key is None and message is None:
            key, message = self.consumer_bz2.consume()
        assert key == '3'
        assert message == "a forth line"
        key = None
        message = None
        while key is None and message is None:
            key, message = self.consumer_bz2.consume()
        assert key == '4'
        assert message == "a lot of lines now"
