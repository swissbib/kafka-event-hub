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


class WebDavReroKafka(AbstractBaseProducer):

    def __init__(self, configrep: str, configrepshare: str):

        AbstractBaseProducer.__init__(self,configrepshare, FileReroWebDavConfig)
        self.configuration.initialize(configrep)
        self.contentprovider = ReroContentProvider(self.configuration)
        self.contentprovider.initialize_dirs()
        self._initialize()

    def process(self):
        if self.contentprovider.is_content_available():
            number_messages = 0
            for delete in self.contentprovider.provide_deletes():
                identifier_key = self.p_identifier_key.search(delete).group(1)
                self.send(key=identifier_key.encode('utf8'),
                          message=delete.encode('utf8'))
                number_messages +=1
                if number_messages % 100 == 0:
                    self.flush()

            number_messages = 0
            for update in self.contentprovider.provide_updates():
                identifier_key = self.p_identifier_key.search(update).group(1)
                self.send(key=identifier_key.encode('utf8'),
                          message=update.encode('utf8'))
                number_messages +=1
                if number_messages % 100 == 0:
                    self.flush()

        print("number of deleted records: {deleted} / number of updated records {updated}".format(
            deleted=self.contentprovider._number_delete_records,
            updated=self.contentprovider._number_update_records
        ))
        #todo
        # summarize result of processing
        move_files(self.configuration.rero_working_dir,self.configuration.rero_src_dir)


    def _initialize(self):
        remove_files_from_dir(self.configuration.rero_working_dir)
        self.p_identifier_key = re.compile(self.configuration.identifier_key,re.UNICODE | re.DOTALL | re.MULTILINE)

