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

import yaml
import re
from utils.ingestUtils import IngestUtils
import copy

class AppConfig:
    def __init__(self,configpath):
        self.configPath = configpath
        self.loadConfig()
        self.detailedGranularityPattern = re.compile('Thh:mm:ssZ', re.UNICODE | re.DOTALL | re.IGNORECASE)



    def loadConfig(self):

        try:
            configHandle = open(self.configPath, 'r')
            self.config = yaml.load (configHandle)
            configHandle.close()
        except Exception:
            print("error trying to read config File")
            raise Exception

    def getConfig(self):
        return self.orgConfig




    def writeConfig(self):
        configHandle = open(self.configPath, 'w')
        yaml.dump(self.config, configHandle,default_flow_style=False)
        configHandle.close()


    def getProcessor(self):
        return self.orgConfig['Processing']['processorType']



class OAIConfig(AppConfig):

    def __init__(self,configpath):
        super().__init__(configpath=configpath)
        self.detailedGranularityPattern = re.compile('Thh:mm:ssZ', re.UNICODE | re.DOTALL | re.IGNORECASE)

    def loadConfig(self):

        try:
            configHandle = open(self.configPath, 'r')
            self.orgConfig = yaml.load (configHandle)
            self.nextConfig = copy.deepcopy(self.orgConfig)
            configHandle.close()
        except Exception:
            print("error trying to read config File")
            raise Exception



    def setStartTimeInNextConfig(self):
        granularity = self.nextConfig['OAI']['granularity']
        if (not granularity is None and not type(granularity) is str ):
            granularity = str(granularity)

        self.nextConfig['OAI']['timestampUTC'] = IngestUtils.getCurrentUTCTimestamp(granularity)


    def setStopTimeInNextConfig(self):

        self.nextConfig['OAI']['stoppageTime'] = IngestUtils.getCurrentTimestamp()


    def writeConfig(self):
        configHandle = open(self.configPath, 'w')
        yaml.dump(self.nextConfig, configHandle,default_flow_style=False)
        configHandle.close()


class MongoConfig(AppConfig):
    def __init__(self,configpath):
        super().__init__(configpath=configpath)

