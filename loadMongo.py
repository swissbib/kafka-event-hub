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

"Kappa" procedure for Mongo sources
bulk load of mongo data into Kafka cluster as basis for stream processing

                    """

from datetime import datetime
from argparse import ArgumentParser
from config.appConfig import MongoConfig
# client could be one of these types
from ingestion.oai.oai import OAI
from ingestion.webdav.webdav import WebDav
from ingestion.filePush.filepush import FilePush
from ingestion.mongo.mongo import MongoSource

if __name__ == '__main__':



    oParser = ArgumentParser()
    oParser.add_argument("-c", "--config", dest="confFile")
    args = oParser.parse_args()

    appConfig = MongoConfig(args.confFile)

    print("".join(["job ",args.confFile, " started: ",'{:%Y-%m-%dT%H:%M:%SZ}'.format(datetime.now(), '%Y-%m-%dT%H:%M:%SZ')]))

    client = MongoSource(appConfig)
    client.initialize()
    client.lookUpData()
    client.preProcessData()
    client.process()
    client.postProcessData()
    print("".join(["job ",args.confFile, " finished: ",'{:%Y-%m-%dT%H:%M:%SZ}'.format(datetime.now(), '%Y-%m-%dT%H:%M:%SZ')]))
