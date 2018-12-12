
from kafka_event_hub.producers.oai.oai_producer import OAIProducer

def test_firstTest():
    #https://www.epochconverter.com/ (todo: make Tests with EpocConverter)
    producer = OAIProducer("../configs/oai/idssg1.oai.yaml")
    producer.initialize()
    producer.lookUpData()
    producer.preProcessData()
    producer.process()
    producer.postProcessData()
