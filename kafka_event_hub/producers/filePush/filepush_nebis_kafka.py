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
from kafka_event_hub.config import FileNebisScpConfig
import tarfile
from kafka_event_hub.utility.producer_utility import remove_files_from_dir, move_files, list_files_absolute_sorted
import re
from kafka_event_hub.config.config_utility import init_logging
from kafka_event_hub.producers.filePush.cleanup_nebis import CleanupNebis




class FilePushNebisKafka(AbstractBaseProducer):

    def __init__(self, configrep: str, configrepshare: str):

        AbstractBaseProducer.__init__(self,configrepshare, FileNebisScpConfig, configrep)

        logComponenents = init_logging(configrep, self.configuration)
        self._shortcut_source_name = logComponenents['shortcut_source_name']
        self.source_logger_summary = logComponenents['source_logger_summary']
        self.source_logger = logComponenents['source_logger']

        self._initialize()



    def process(self):

        #todo in general
        # make logging for what is going on
        # for example summary of the process
        self.pre_process()
        nebis_incoming_files = list_files_absolute_sorted(self.configuration.working_dir,".*\.gz")

        cleanUpNebis = CleanupNebis(self.configuration)
        number_messages = 0
        for incoming_file in nebis_incoming_files:
            tar = tarfile.open(incoming_file,'r:gz')
            for single_file in tar.getmembers():
                buffered_reader = tar.extractfile(single_file)
                #todo
                # buffered_reader provides bytes and not string
                # do we have to transform it to bytes (as it is done with most of the other sources
                # or is the current implementation sufficient??
                # content = buffered_reader.read().decode('utf-8')

                content = buffered_reader.read().decode('utf-8')

                cleanContent = cleanUpNebis.cleanup(content)
                if len(cleanContent) > 0:
                    self.send(key=cleanContent['key'].encode('utf8'),
                          message=cleanContent['cleanDoc'].encode('utf8'))
                    number_messages +=1
                    if number_messages % 1000 == 0:
                        self.flush()

                #todo:
                # do we need always a key or would it be possible
                # to send a message without key to kafka and handover the task of extracting
                # this information to the KafkaStreams reader and transformer specialized for nebis?
                #self.send(key=record.header.identifier.encode('utf8'),
                #      message=record.raw.encode('utf8'))
        self.flush()
        self.post_process()

    def _initialize(self):
        remove_files_from_dir(self.configuration.working_dir)


    def pre_process(self):
        move_files(self.configuration.incoming_dir,
                   self.configuration.working_dir,
                   "^.*\.gz$")


    def post_process(self):
        move_files(self.configuration.working_dir,
                   self.configuration.nebis_src_dir,
                   "^.*\.gz$")


