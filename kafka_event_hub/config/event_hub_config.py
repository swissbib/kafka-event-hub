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
from copy import deepcopy


class BaseConfig:
    """

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
    def processor(self):
        return self._yaml['Processing']['processorType']

    def __getitem__(self, item):
        return self._yaml[item]


class OAIConfig(BaseConfig):

    def __init__(self, config_path):
        super().__init__(config_path=config_path)
        #todo: open - do we need a temporarily copy of configuration?
        #self._nextyaml = deepcopy( self.configuration)
        self._processStarttime = current_utc_timestamp(self._yaml['OAI']['granularity'])

    def update_start_time(self):
        granularity = self._yaml['OAI']['granularity']
        if granularity is not None:
            granularity = str(granularity)
        self._yaml['OAI']['timestampUTC'] = self._processStarttime


    def update_stop_time(self):
        self._yaml['OAI']['stoppageTime'] = current_timestamp()

    @property
    def processStarttime(self):
        return self._processStarttime

    @processStarttime.setter
    def processStarttime(self, starttime):
        #todo: check validaty in relation to granularity pattern
        self._processStarttime = starttime
