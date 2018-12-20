
from kafka_event_hub.consumers.cbs.cbs_consumer import CBSConsumer

def test_firstTest():
    #https://www.epochconverter.com/ (todo: make Tests with EpocConverter)
    consumer  = CBSConsumer("test/configs/oai/idssg1.oai.yaml")
    consumer.consume()
    print("das wars")