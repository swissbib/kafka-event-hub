from kafka_event_hub.producers import SRUProducer


class TestSRUProducer(object):

    def setup_class(self):
        pass

    def test_init(self):
        p = SRUProducer('configs/sru/dsv05_dump.yml')
        assert p.configuration['Kafka']['database'] == 'defaultdb'
