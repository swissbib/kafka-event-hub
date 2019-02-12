# coding: utf-8


__author__ = 'swissbib - UB Basel, Switzerland, Guenter Hipler'
__copyright__ = "Copyright 2019, swissbib project"
__credits__ = []
__license__ = "GNU General Public License v3.0"
__version__ = "0.2"
__maintainer__ = "Guenter Hipler"
__email__ = "guenter.hipler@unibas.ch"
__status__ = "in development"
__description__ = """

                    """
from kafka_event_hub.producers.base_producer import AbstractBaseProducer
from kafka_event_hub.config import FileReroWebDavConfig
from kafka_event_hub.utility.producer_utility import remove_files_from_dir, move_files
from kafka_event_hub.producers.webdav.rero_content_provider import ReroContentProvider
import re
from kafka_event_hub.config.config_utility import init_logging
from kafka_event_hub.utility.producer_utility import current_timestamp



class WebDavReroKafka(AbstractBaseProducer):

    def __init__(self, configrep: str, configrepshare: str):

        AbstractBaseProducer.__init__(self,configrepshare, FileReroWebDavConfig, configrep)

        logComponenents = init_logging(configrep, self.configuration)
        self._shortcut_source_name = logComponenents['shortcut_source_name']
        self.source_logger_summary = logComponenents['source_logger_summary']
        self.source_logger = logComponenents['source_logger']

        self.contentprovider = ReroContentProvider(self.configuration,
                                                   source_logger=self.source_logger)
        self.contentprovider.initialize_dirs()
        self._initialize()


    def process(self):
        if self.contentprovider.is_content_available():
            try:
                number_messages = 0
                for delete in self.contentprovider.provide_deletes():
                    identifier_key = self.p_identifier_key.search(delete).group(1)
                    self.send(key=identifier_key.encode('utf8'),
                              message=delete.encode('utf8'))
                    number_messages +=1
                    if number_messages % 1000 == 0:
                        self.flush()

                number_messages = 0
                for update in self.contentprovider.provide_updates():
                    identifier_key = self.p_identifier_key.search(update).group(1)
                    self.send(key=identifier_key.encode('utf8'),
                              message=update.encode('utf8'))
                    number_messages +=1
                    if number_messages % 1000 == 0:
                        self.flush()

                self.flush()

                self.source_logger_summary.info('Anzahl der nach Kafka gesendeten messages: {ANZAHL}'.format(
                    ANZAHL=number_messages
                ))

                self.source_logger_summary.info('number of deleted records: {deleted} / number of updated records {updated}'.format(
                    deleted=self.contentprovider._number_delete_records,
                    updated=self.contentprovider._number_update_records
                ))


                self.source_logger_summary.info('STOP Harvesting datasource {SOURCE} {STOPTIME}'.format(
                    SOURCE=self._shortcut_source_name,
                    STOPTIME=current_timestamp()
                ))

            except Exception as baseException:
                self.source_logger.error('Exception während des Rzerorozesses:  {MESSAGE}'.format(
                    MESSAGE=str(baseException)))
            else:
                self.source_logger_summary.info('Keine Exception im webdav-process source {SOURCE}'.format(
                    SOURCE=self._shortcut_source_name))


            move_files(self.configuration.rero_working_dir,self.configuration.rero_src_dir)

        self.update_configuration()


    def _initialize(self):
        remove_files_from_dir(self.configuration.rero_working_dir)
        self.p_identifier_key = re.compile(self.configuration.identifier_key,re.UNICODE | re.DOTALL | re.MULTILINE)

    def update_configuration(self):

        self.configuration.update_stop_time()
        self._configuration.store()
