# coding: utf-8


__author__ = 'swissbib - UB Basel, Switzerland, Guenter Hipler'
__copyright__ = "Copyright 2018, swissbib project"
__credits__ = []
__license__ = "GNU General Public License v3.0"
__maintainer__ = "Guenter Hipler"
__email__ = "guenter.hipler@unibas.ch"
__status__ = "in development"
__description__ = """

                    """

from kafka_event_hub.producers.base_producer import AbstractBaseProducer
from kafka_event_hub.config import OAIConfig
from kafka_event_hub.utility.producer_utility import transform_from_until
from kafka_event_hub.producers.data_preparation import DataPreparation

from sickle import Sickle
from sickle.oaiexceptions import OAIError, BadArgument

import re
import sys
import time


class OAIProducer(AbstractBaseProducer):

    def __init__(self, configrep: str, configrepshare: str):

        AbstractBaseProducer.__init__(self,configrepshare, OAIConfig)
        #todo
        #I have to split it up this way because AbstractBaseProducer doesn't work with specialized configuratiomns so far
        #until we have to decided to continue I will call an initialized method after initialization so I can do
        #the processing for the configurations only valid for a single data repository
        self.configuration.initialize(configrep)

    def initialize(self):
        pass


    def process(self):
        try:

            #todo
            #what do we do with until and other OAI parameters
            sickle = Sickle(self.configuration['OAI']['url'])
            dic = {}
            if not self.configuration['OAI']['metadataPrefix'] is None:
                dic['metadataPrefix'] = self.configuration['OAI']['metadataPrefix']
            if not self.configuration['OAI']['set'] is None:
                dic['set'] = self.configuration['OAI']['set']
            if not self.configuration['OAI']['timestampUTC'] is None:
                dic['from'] = transform_from_until(self.configuration['OAI']['timestampUTC'],
                                                   self.configuration['OAI']['granularity'])
            if not self.configuration['OAI']['until'] is None:
                dic['until'] = transform_from_until(self.configuration['OAI']['until'],
                                                    self.configuration['OAI']['granularity'])

            records_iter = sickle.ListRecords(
                **dic
            )

            messages = 0
            for record in records_iter:
                messages += 1
                self.send(key=record.header.identifier.encode('utf8'),
                          message=record.raw.encode('utf8'))
            self.flush()

        except BadArgument as ba:
            self._logger.exception(ba)
        except OAIError as oaiError:
            self._logger.exception(oaiError)
        except Exception as baseException:
            self._logger.exception(baseException)
        else:
            self.update_configuration()

    def update_configuration(self):
        self.configuration.update_stop_time()
        self.configuration.update_start_time()
        self._configuration.store()

