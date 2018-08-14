from kafka_event_hub.producers import SRUProducer


class TestSRUProducer(object):

    def setup_class(self):
        import os
        print(os.getcwd())
        self.producer = SRUProducer('test/configs/sru/dsv05_dump.yml')

    def test_producer(self):
        self.producer.set_query_id_equal_with('HAN000214657')
        self.producer.process()

    def test_list_topics(self):
        assert self.producer.list_topics()




