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
from kafka_event_hub.producers.base_producer import AbstractBaseProducer
from kafka_event_hub.config import FileReroWebDavConfig
from kafka_event_hub.utility.producer_utility import remove_files_from_dir, move_files, list_files_absolute_sorted
import re


class WebDavKafka(AbstractBaseProducer):

    def __init__(self, configrep: str, configrepshare: str):

        AbstractBaseProducer.__init__(self,configrepshare, FileReroWebDavConfig)
        self.configuration.initialize(configrep)
        self._initialize()

    def process(self):
        pass

    def _initialize(self):
        remove_files_from_dir(self.configuration.working_dir)
        self.p_identifier_key = re.compile(self.configuration.identifier_key,re.UNICODE | re.DOTALL | re.MULTILINE)
