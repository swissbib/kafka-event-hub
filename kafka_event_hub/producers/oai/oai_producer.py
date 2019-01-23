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

from sickle import Sickle
from sickle.oaiexceptions import OAIError, BadArgument

import re
import sys


class OAIProducer(AbstractBaseProducer):

    def __init__(self, configuration: str):
        """

        :param configuration: The path to the configuration file.
        """
        super().__init__(configuration, OAIConfig)
        self._record_body_regex = re.compile(self.configuration['Processing']['recordBodyRegEx'], re.UNICODE | re.DOTALL | re.IGNORECASE)
        self.configuration.update_start_time()

    @property
    def record_body_regex(self):
        return self._record_body_regex

    def process(self):
        try:

            sickle = Sickle(self.configuration['oai']['base']['url'])
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
                **self.configuration['oai']
            )

            for record in records_iter:
                self._produce_kafka_message(value=record.raw,
                                            key=record.header.identifier,
                                            eventTime=record.header.datestamp)

        except BadArgument as ba:
            self._error_logger.exception(ba)
        except OAIError as oaiError:
            self._error_logger.exception(oaiError)
        except Exception as baseException:
            self._error_logger.exception(baseException)
        else:
            self.update_configuration()

    def update_configuration(self):
        self.configuration.update_stop_time()
        super().update_configuration()

