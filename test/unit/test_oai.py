
from kafka_event_hub.producers import OAIProducerKafka


def test_firstTest():
    #https://www.epochconverter.com/ (todo: make Tests with EpocConverter)
    producer = OAIProducerKafka("../configs/oai/idssg1.oai.yaml")
    producer.process()
