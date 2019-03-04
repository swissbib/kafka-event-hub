from kafka_event_hub.producers.base_producer import AbstractBaseProducer
from kafka_event_hub.config import ProducerConfig


class SimpleProducer(AbstractBaseProducer):
    """
    Connects to Kafka Event Hub and does nothing else. Use send() to send messages.
    """

    def __init__(self, config: str):
        super().__init__(config, config_parser=ProducerConfig)

    def process(self):
        pass
