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
from os.path import basename
import re
from kafka_event_hub.utility.producer_utility import current_timestamp


class OAIProducerKafka(AbstractBaseProducer):

    def __init__(self, configrep: str, configrepshare: str):

        AbstractBaseProducer.__init__(self,configrepshare, OAIConfig, configrep)
        self._init_logging(configrep)

    def process(self):


        try:

            self.source_logger_summary.info('\n\n\n\nStart Harvesting datasource {SOURCE} {STARTTIME}'.format(
                SOURCE=self._shortcut_source_name,
                STARTTIME=current_timestamp()
            ))

            oai_sickle = OaiSickleWrapper(self.configuration,
                                          self.source_logger_summary,
                                          self.source_logger)
            messages = 0
            for record in oai_sickle.fetch_iter():
                messages += 1
                self.send(key=record.header.identifier.encode('utf8'),
                          message=record.raw.encode('utf8'))
            self.flush()

            self.source_logger_summary.info('Anzahl der nach Kafka gesendeten messages: {ANZAHL}'.format(
                ANZAHL=messages
            ))

            self.source_logger_summary.info('STOP Harvesting datasource {SOURCE} {STOPTIME}'.format(
                SOURCE=self._shortcut_source_name,
                STOPTIME=current_timestamp()
            ))

        except Exception as baseException:
            self.source_logger.error('Exception während des Harvestingprozesses:  {MESSAGE}'.format(
                MESSAGE=str(baseException)))
        else:
            self.source_logger_summary.error('Keine Exception im Basisworkflow Harvesting der source {SOURCE}'.format(
                SOURCE=self._shortcut_source_name))

        self.update_configuration()


    def update_configuration(self):
        self.configuration.update_stop_time()
        self.configuration.update_start_time()
        self._configuration.store()

    def _init_logging(self, configreppath: str):
        shortcut = re.compile('(^.*?)\..*',re.DOTALL).search(basename(configreppath))
        self._shortcut_source_name = shortcut.group(1) if shortcut else "default"

        with open(self.configuration.logs_config, 'r') as f:
            log_cfg = yaml.safe_load(f.read())
            logging.config.dictConfig(log_cfg)
            self.source_logger = logging.getLogger(self._shortcut_source_name)
            self.source_logger_summary = logging.getLogger(self._shortcut_source_name + '_summary')

