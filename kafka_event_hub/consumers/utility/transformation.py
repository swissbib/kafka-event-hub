from confluent_kafka import Message

from typing import Tuple

import json
import logging


class DataTransformation(object):

    def __init__(self, logger=logging.getLogger(__name__)):
        self._logger = logger

    def pre_filter(self, value: str) -> bool:
        return False

    def transform(self, value: str) -> dict:
        return json.loads(value)

    def post_filer(self, value: dict) -> bool:
        return False

    def update(self, value: dict) -> dict:
        return value

    def get_identifier(self, value: dict) -> str:
        return value['identifier']

    def run(self, message: Message) -> Tuple[str, dict]:
        value = message.value()
        if value is None:
            self._logger.error('Message had no value. Skipped.')
            return '', dict()

        value = value.decode('utf-8')

        if not self.pre_filter(value):
            value = self.transform(value)

            if not self.post_filer(value):
                value = self.update(value)
                return self.get_identifier(value), value
            else:
                self._logger.info('Message was filtered after the transformation: %s.', value)
                return '', dict()
        else:
            self._logger.info('Message was filtered before the transformation: %s.', value)
            return '', dict()
