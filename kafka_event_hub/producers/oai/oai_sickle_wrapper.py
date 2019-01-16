# coding: utf-8


__author__ = 'swissbib - UB Basel, Switzerland, Guenter Hipler'
__copyright__ = "Copyright 2019, swissbib project"
__credits__ = []
__license__ = "GNU General Public License v3.0"
__maintainer__ = "Guenter Hipler"
__email__ = "guenter.hipler@unibas.ch"
__status__ = "in development"
__description__ = """

                    """

from kafka_event_hub.config.content_collector_config import OAIConfig
from kafka_event_hub.utility.producer_utility import transform_from_until
from sickle import Sickle
from sickle.oaiexceptions import OAIError, BadArgument


class OaiSickleWrapper(object):

    def __init__(self, configuration: type(OAIConfig)):

        self._oaiconfig = configuration
        self._initialize()

    def _initialize(self):
        self.dic = {}

        if not self._oaiconfig.metadata_prefix is None:
            self.dic['metadataPrefix'] = self._oaiconfig.metadata_prefix
        if not self._oaiconfig.oai_set is None:
            self.dic['set'] = self._oaiconfig.oai_set
        if not self._oaiconfig.timestamp_utc is None:
            self.dic['from'] = transform_from_until(self._oaiconfig.timestamp_utc,
                                               self._oaiconfig.granularity)
        if not self._oaiconfig.oai_until is None:
            self.dic['until'] = transform_from_until(self._oaiconfig.oai_until,
                                                self._oaiconfig.granularity)

    def fetch_iter(self):

        try:

            sickle = Sickle(self._oaiconfig['OAI']['url'])

            records_iter = sickle.ListRecords(
                **self.dic
            )

            for record in records_iter:
                yield record


        except BadArgument as ba:
            #todo: implement logging
            print(str(ba))
        except OAIError as oaiError:
            #self._logger.exception(oaiError)
            #todo: implement logging
            print(str(oaiError))
        except Exception as baseException:
            #todo: implement logging
            print(str(baseException))
        else:
            print("oai fetching finished successfully")
            #todo: make better logging

