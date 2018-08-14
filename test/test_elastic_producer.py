from kafka_event_hub.producers import ElasticProducer


class TestElasticProducer(object):

    def setup_class(self):
        self.producer = ElasticProducer('test/configs/elastic/elastic_test.yml')

    def test_produce(self):
        self.producer.process()
