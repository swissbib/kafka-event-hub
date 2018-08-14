from kafka_event_hub.consumers import ElasticConsumer


class TestElasticConsumer(object):

    def setup_class(self):
        self.consumer = ElasticConsumer('test/configs/elastic/elastic.yml')

    def test_consume(self):
        self.consumer.consume()