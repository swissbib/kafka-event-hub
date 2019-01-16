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



class FilePushNebisKafka(AbstractBaseProducer):

    def __init__(self, configrep: str, configrepshare: str):

        AbstractBaseProducer.__init__(self,configrepshare, FileNebisScpConfig)
        self.configuration.initialize(configrep)
        self._initialize()

    def process(self):

        #todo in general
        # make logging for what is going on
        # for example summary of the process
        self.pre_process()
        nebis_incoming_files = list_files_absolute_sorted(self.configuration.working_dir,".*\.gz")

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

                m_key = self.p_identifier_key.search(content)
                if m_key:
                    documentkey= m_key.group(1)
                    self.send(key=documentkey.encode('utf8'),
                          message=content.encode('utf8'))
                else:
                    #todo:
                    # this shouldn't happen throw an exception or at least log the incident
                    pass

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
        self.p_identifier_key = re.compile(self.configuration.identifier_key,re.UNICODE | re.DOTALL | re.MULTILINE)

    def pre_process(self):
        move_files(self.configuration.incoming_dir,
                   self.configuration.working_dir,
                   "^.*\.gz$")


    def post_process(self):
        move_files(self.configuration.working_dir,
                   self.configuration.nebis_src_dir,
                   "^.*\.gz$")


