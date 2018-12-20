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

from kafka_event_hub.producers.base_producer import AbstractBaseProducer

class WebDav(AbstractBaseProducer):

    def __init__(self, appConfig=None):
        #todo: or do we have to use
        #super(self,appConfig)
        AbstractBaseProducer.__init__(self,appConfig)


    def collectItems(self):
        pass
        #BaseProcessor.collectItems(self)
