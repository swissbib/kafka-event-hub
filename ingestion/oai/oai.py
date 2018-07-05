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

from sickle import Sickle
from ingestion.processor import BaseProcessor
from sickle.oaiexceptions import OAIError
from config.appConfig import AppConfig
import re
from utils.ingestUtils import IngestUtils
from sickle.oaiexceptions import BadArgument
import sys



class OAI(BaseProcessor):

    def __init__(self, appConfig : AppConfig = None):
        BaseProcessor.__init__(self,appConfig)
        self.recordBodyRegEx = re.compile(self.appConfig.getConfig()['Processing']['recordBodyRegEx'],
                                                    re.UNICODE | re.DOTALL | re.IGNORECASE)
        self.appConfig.setStartTimeInNextConfig()



    def process(self):

        try:

            sickle = Sickle(self.appConfig.getConfig()['OAI']['url'])
            #print(sickle.endpoint)
            dic = {}
            if not self.appConfig.getConfig()['OAI']['metadataPrefix'] is None:
                dic['metadataPrefix'] =  self.appConfig.getConfig()['OAI']['metadataPrefix']
            if not self.appConfig.getConfig()['OAI']['setSpec'] is None:
                dic['set'] = self.appConfig.getConfig()['OAI']['setSpec']
            if not self.appConfig.getConfig()['OAI']['timestampUTC'] is None:
                dic['from'] = IngestUtils.transformFromUntil(self.appConfig.getConfig()['OAI']['timestampUTC'],
                                                             self.appConfig.getConfig()['OAI']['granularity'])
            if not self.appConfig.getConfig()['OAI']['until'] is None:
                dic['until'] = IngestUtils.transformFromUntil(self.appConfig.getConfig()['OAI']['until'],
                                                              self.appConfig.getConfig()['OAI']['granularity'])

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
                self.produceKafkaMessage(record.raw,
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

    def postProcessData(self):
        self.appConfig.setStopTimeInNextConfig()
        super().postProcessData()

