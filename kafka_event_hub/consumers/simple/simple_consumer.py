from kafka_event_hub.config import ConsumerConfig
from kafka_event_hub.consumers.base_consumer import AbstractBaseConsumer


class SimpleConsumer(AbstractBaseConsumer):
    """
    A very simple consumer, which does nothing but consume one message after the other and return them. Both keys and
    messages are returned as strings. Mostly useful for debug purposes.
    """

    def __init__(self, config_path: str, **kwargs):
        super().__init__(config_path, ConsumerConfig, **kwargs)

    def consume(self) -> (str, str):
        message = next(self._consumer)
        self._time_logger.info('Received message: {} with key {}'
                               .format(message.value.decode('utf-8'), message.key.decode('utf-8')))
        return message.key.decode('utf-8'), message.value.decode('utf-8')
