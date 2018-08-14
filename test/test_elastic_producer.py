from kafka_event_hub.producers import ElasticProducer


class TestElasticProducer(object):

    def setup_class(self):
        self.producer = ElasticProducer('configs/elastic/elastic_producer.yml')

    def test_produce(self):
        self.producer.process()
