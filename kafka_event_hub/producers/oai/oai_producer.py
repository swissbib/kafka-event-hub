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
        self.recordBodyRegEx = re.compile(self.configuration['Processing']['recordBodyRegEx'], re.UNICODE | re.DOTALL | re.IGNORECASE)
        self.configuration.update_start_time()

    def process(self):
        try:

            sickle = Sickle(self.configuration['OAI']['url'])
            #print(sickle.endpoint)
            dic = {}
            if not self.configuration['OAI']['metadataPrefix'] is None:
                dic['metadataPrefix'] =  self.configuration['OAI']['metadataPrefix']
            if not self.configuration['OAI']['setSpec'] is None:
                dic['set'] = self.configuration['OAI']['setSpec']
            if not self.configuration['OAI']['timestampUTC'] is None:
                dic['from'] = transform_from_until(self.configuration['OAI']['timestampUTC'],
                                                             self.configuration['OAI']['granularity'])
            if not self.configuration['OAI']['until'] is None:
                dic['until'] = transform_from_until(self.configuration['OAI']['until'],
                                                              self.configuration['OAI']['granularity'])

            #for k, v in dic.items():
            #    print(k, v)

            recordsIt = sickle.ListRecords(
                **dic
            )


            for record in recordsIt:
                #print(record.header.identifier)
                #print(record.header.datestamp)
                #no at the moment we store all the data
                #if record.header.deleted:
                    #at the moment in time I don't want to use deleted items in Kafka
                #    continue
                #we want the raw data coming directly from the source
                #this is going to be stored in Kafka and later used for the persistent storage
                #sBody = self.recordBodyRegEx.search(record.raw)
                #if sBody:
                #    body = sBody.group(1)
                    #todo
                    #key should not contain the ID of the OAI identifier (sysID of library system
                    #otherwise partitions for single networks won't be stable
                self._produce_kafka_message(value=record.raw,
                                            key=record.header.identifier,
                                            eventTime=record.header.datestamp)
                #else:
                #    raise Exception("we havent't found the body which should not be the case")

        except BadArgument as ba:
            print(ba)
        except OAIError as oaiError:
            print(oaiError)
        except Exception as baseException:
            print(baseException)
        except:
            print("Unexpected error:", sys.exc_info()[0])

    def update_configuration(self):
        self.configuration.update_stop_time()
        super().update_configuration()

