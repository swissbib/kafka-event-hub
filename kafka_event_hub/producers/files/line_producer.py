import gzip
import logging

from kafka_event_hub.producers.base_producer import AbstractBaseProducer
from kafka_event_hub.config import BaseConfig


class LineProducer(AbstractBaseProducer):

    def __init__(self, config: str):
        super().__init__(config, config_parser=BaseConfig)
        self.count = 0

    def process(self):
        file_name = self.configuration['path']
        if file_name.endswith('.gz'):
            fp = gzip.open(file_name, mode='r')
        else:
            fp = open(file_name, 'r')
            
        for line in fp:
            self._poll(0)
            if isinstance(line, bytes):
                line = line.decode('utf-8')
            line = line.strip()
            logging.debug("Produced Message %s from Line: %s", self.count, line)
            self._produce_kafka_message("{}".format(self.count), line)   
            self.count += 1
        self._flush()
        fp.close()
