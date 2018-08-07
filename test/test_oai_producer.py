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
bootstrap mechanism to start the various producers types


                    """
from kafka_event_hub.config import OAIConfig
from kafka_event_hub.producers import OAI


if __name__ == '__main__':

    __author__ = 'swissbib - UB Basel, Switzerland, Guenter Hipler'
    __copyright__ = "Copyright 2016, swissbib project"
    __credits__ = []
    __license__ = "??"
    __version__ = "0.1"
    __maintainer__ = "Guenter Hipler"
    __email__ = "guenter.hipler@unibas.ch"
    __status__ = "in development"
    __description__ = """

                        """
    appConfig = OAIConfig('configs/oai/')

    client = OA()[appConfig.processor](appConfig)
    client.initialize()
    client.lookUpData()
    client.preProcessData()
    client.process()
    client.update_configuration()


