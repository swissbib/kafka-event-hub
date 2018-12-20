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


class OAIProducer(AbstractBaseProducer, DataPreparation):

    def __init__(self, configuration: str):
        """

        :param configuration: The path to the configuration file.
        """
        super().__init__(configuration, OAIConfig)
        self._record_body_regex = re.compile(self.configuration['Processing']['Default']['recordBodyRegEx'], re.UNICODE | re.DOTALL | re.IGNORECASE)

    @property
    def record_body_regex(self):
        return self._record_body_regex

    def process(self):
        try:

            sickle = Sickle(self.configuration['OAI']['url'])
            dic = {}
            if not self.configuration['OAI']['metadataPrefix'] is None:
                dic['metadataPrefix'] = self.configuration['OAI']['metadataPrefix']
            if not self.configuration['OAI']['setSpec'] is None:
                dic['set'] = self.configuration['OAI']['setSpec']
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
                if messages % 1000 == 0:
                    self._poll()
                    self._flush()
                #org = record.header.datestamp
                #test = int(time.mktime(time.strptime(record.header.datestamp, '%Y-%m-%dT%H:%M:%SZ'))) - time.timezone
                millisecondsSinceEpocUTC = int(time.mktime(time.strptime(record.header.datestamp, '%Y-%m-%dT%H:%M:%SZ')))
                #zurueck = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.localtime(millisecondsSinceEpocUTC)  )
                self._produce_kafka_message(value=record.raw,
                                            key=record.header.identifier,
                                            timestamp=millisecondsSinceEpocUTC
                                            )

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
        super().update_configuration()

