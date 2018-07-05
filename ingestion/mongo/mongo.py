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

from ingestion.processor import BaseProcessor
from config.appConfig import AppConfig
from pymongo import MongoClient
from ingestion.mongo.cleanup.sourceCleanUp import DefaultCleanUp,NebisCleanUp, OAIDC, Jats, ReroCleanUp



class MongoSource(BaseProcessor):

    def __init__(self, appConfig : AppConfig = None):
        BaseProcessor.__init__(self,appConfig)


    def initialize(self):
        self.mongoClient = MongoClientWrapper(self.appConfig)

    def process(self):
        if not self.mongoClient is None:

            cleaner = globals()[self.appConfig.getConfig()['Processing']['cleanUpType']](self.appConfig.getConfig())

            for doc in  self.mongoClient.getAllDocsInCollection():

                try:
                    cleanResource = cleaner.cleanUp(doc)

                    if not cleanResource is None:
                        self.produceKafkaMessage(messageValue=cleanResource['doc'],
                                                 key=cleanResource['key'],
                                                 eventTime=cleanResource['eventTime'])

                except Exception as pythonBaseException:
                    print(pythonBaseException)


    def postProcessData(self):
        if not self.mongoClient is None:
            self.mongoClient.closeConnection()


class MongoClientWrapper:
    def __init__(self, appConfig : AppConfig = None):
        self.config = appConfig.getConfig()

        if not self.config['HOST']['user'] is None:
            uri = 'mongodb://{USER}:{PASSWORD}@{SERVER}:{PORT}/{DB}'.format(
                USER=self.config['HOST']['user'],
                PASSWORD=self.config['HOST']['password'],
                SERVER=self.config['HOST']['server'],
                PORT=self.config['HOST']['port'],
                DB=self.config['HOST']['authDB']
            )
        else:

            uri = 'mongodb://{SERVER}:{PORT}'.format(
                SERVER=self.config['HOST']['server'],
                PORT=self.config['HOST']['port'],
            )

        self.client = MongoClient( uri)
        self.database = self.client[self.config['DB']['dbname']]
        self.collection = self.database[self.config['DB']['collection']['name']]

    def getAllDocsInCollection(self):

        if not self.collection is None:
            for doc in  self.collection.find():
                yield doc

    def closeConnection(self):
        if not self.client is None:
            self.client.close()


