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
from kafka_event_hub.producers.oai.oai_sickle_wrapper import OaiSickleWrapper

from  logging import config
import logging
import yaml


class OAIProducerKafka(AbstractBaseProducer):

    def __init__(self, configrep: str, configrepshare: str):

        AbstractBaseProducer.__init__(self,configrepshare, OAIConfig, configrep)


    def process(self):

        #self.mylogger = logging.getLogger("bla")

        with open('logs/producer/oai/oai.config.yaml', 'r') as f:
            log_cfg = yaml.safe_load(f.read())
            logging.config.dictConfig(log_cfg)
            self.my_logger = logging.getLogger('simpleExample')
            self.my_console_logger = logging.getLogger("consoleLogger")
        #config.dictConfig().fileConfig("logs/producer/oai/oai.config.yaml")


        self.my_logger.error("jetzt geht es los")

        self.my_console_logger.info("jetzt auf die Konsole")

        self._error_logger.error("my first logging message with python")

        oai_sickle = OaiSickleWrapper(self.configuration)
        messages = 0
        for record in oai_sickle.fetch_iter():
            messages += 1
            self.send(key=record.header.identifier.encode('utf8'),
                      message=record.raw.encode('utf8'))
        self.flush()
        self.update_configuration()


    def update_configuration(self):
        self.configuration.update_stop_time()
        self.configuration.update_start_time()
        self._configuration.store()

