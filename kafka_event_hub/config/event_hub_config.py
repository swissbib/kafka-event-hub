# coding: utf-8


__author__ = 'swissbib - UB Basel, Switzerland, Guenter Hipler'
__copyright__ = "Copyright 2018, swissbib project"
__credits__ = []
__license__ = "GNU General Public License v3.0"
__version__ = "0.2"
__maintainer__ = "Guenter Hipler"
__email__ = "guenter.hipler@unibas.ch"
__status__ = "in development"
__description__ = """

                    """
from kafka_event_hub.utility.producer_utility import current_timestamp, current_utc_timestamp

import logging
import yaml


class BaseConfig:
    """
        Basic wrapper for the configuration files to configure producers and consumers.
    """
    def __init__(self, config_path: str, logger=logging.getLogger(__name__)):
        self._config_path = config_path
        self._yaml = None
        self._logger = logger
        self._load()

    def _load(self):
        try:
            with open(self._config_path, 'r') as fp:
                self._yaml = yaml.load(fp)
        except Exception:
            self._logger.exception('The config file at %s could not be loaded!', self._config_path)
            raise Exception

    @property
    def configuration(self):
        if self._yaml is None:
            self._load()
        return self._yaml

    def store(self):
        with open(self._config_path, 'w') as fp:
            yaml.dump(self._yaml, fp, default_flow_style=False)

    @property
    def logging(self):
        return self.configuration['Logging']['path']

    @property
    def errorlogging(self):
        return self.configuration['Logging']['errpath']

    def __getitem__(self, item):
        return self.configuration[item]


class ConsumerConfig(BaseConfig):

    def __init__(self, config_path: str, logger=logging.getLogger(__name__)):
        super().__init__(config_path, logger=logger)

    @property
    def consumer(self):
        return self.configuration['Consumer']

    @property
    def topic(self):
        return self.configuration['Topics']


class ElasticConsumerConfig(ConsumerConfig):

    def __init__(self, config_path: str, logger=logging.getLogger(__name__)):
        super().__init__(config_path, logger=logger)

    @property
    def elastic_settings(self):
        return self.configuration['ElasticIndex']

    @property
    def id_name(self):
        try:
            return self.configuration['IdentifierKey']
        except KeyError:
            return '_key'


class ProducerConfig(BaseConfig):

    def __init__(self, config_path: str, logger=logging.getLogger(__name__)):
        super().__init__(config_path, logger=logger)

    @property
    def producer(self):
        return self.configuration['Producer']

    @property
    def topic(self):
        return self.configuration['Topic']

    @property
    def admin(self):
        return self.configuration['AdminClient']


class LineProducerConfig(ProducerConfig):

    def __init__(self, config_path, logger=logging.getLogger(__name__)):
        super().__init__(config_path, logger=logger)

    @property
    def path(self):
        return self.configuration['Path']


class ElasticProducerConfig(ProducerConfig):

    def __init__(self, config_path, logger=logging.getLogger(__name__)):
        super().__init__(config_path, logger=logger)

    @property
    def elastic_settings(self):
        return self.configuration['ElasticIndex']
    
    @property
    def scroll(self):
        return self.configuration['Scroll']

    @property
    def identifier_key(self):
        return self.configuration['IdentifierKey']