import sys
import os
import logging
import pytest
from kafka import KafkaAdminClient

logging.basicConfig(filename='logs/test-elastic-consumer.log', filemode='w', level=logging.DEBUG)

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from kafka_event_hub.consumers import SimpleElasticConsumer, BulkElasticConsumer
from kafka_event_hub.producers import LineProducer


class TestSimpleElasticConsumer(object):

    def setup_class(self):
        self.admin = KafkaAdminClient(bootstrap_servers='localhost:9092')
        self.consumer = SimpleElasticConsumer('configs/elastic/elastic-consumer-simple-test.yml')
        self.bulk_consumer = BulkElasticConsumer('configs/elastic/elastic-consumer-bulk-test.yml')
        self.producer = LineProducer("configs/elastic/json-lines-producer.yml")
        self.producer.process()

    def teardown_class(self):
        self.admin.delete_topics(['json'])
        self.consumer.close()

    @pytest.mark.skip()
    def test_consume(self):
        result = self.consumer.consume()
        assert result

    # @pytest.mark.skip()
    def test_consume_bulk(self):
        result = self.bulk_consumer.consume()
        assert result
