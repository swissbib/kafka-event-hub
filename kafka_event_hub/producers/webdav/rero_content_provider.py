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


from kafka_event_hub.config.content_collector_config import FileReroWebDavConfig
from kafka_event_hub.utility.producer_utility import list_files_absolute_sorted, mkdir_if_absent, \
                                                add_end_dir_separator, cp_file
import re
from datetime import datetime, timedelta
import gzip

class ReroContentProvider(object):

    def __init__(self, config : type (FileReroWebDavConfig)):
        assert isinstance(config, FileReroWebDavConfig)
        self._config = config
        self.p_processONLY_NEW_DELIVERD = re.compile("^ONLY_NEW_DELIVERD$", re.UNICODE | re.DOTALL | re.IGNORECASE)
        #next two modes are not used so far
        self.p_processSPECIFIC = re.compile("(.*?)##(.*?)##$")
        self.p_processFROM_UNTIL = re.compile("^(.*?)UNTIL(.*?)$")
        self.pIterSingleRecord = re.compile('<record><header.*?>.*?</metadata></record>',re.UNICODE | re.DOTALL | re.IGNORECASE)
        self._number_delete_records = 0
        self._number_update_records = 0



    def is_content_available(self):
        if self.p_processONLY_NEW_DELIVERD.search(self._config.process_mode):
            sDateLastProc = self._config.latest_proc_date
            dateProc = datetime.strptime(sDateLastProc,"%Y_%m_%d")
            dateProc = dateProc + timedelta(days=1)
            stringCurrentDate = datetime.now().strftime("%Y_%m_%d")
            stringDateProc = dateProc.strftime("%Y_%m_%d")

            content_available = False

            while (stringDateProc <= stringCurrentDate):

                try:

                    files_sorted = list_files_absolute_sorted( "".join([add_end_dir_separator(self._config.basedir_webdav),stringDateProc]))
                    content_available = True if len(files_sorted) else False
                    for file in files_sorted:
                        cp_file(file, self._config.rero_working_dir)
                except Exception as baseException:
                    #todo make better logging
                    print(str(baseException))

                dateProc = datetime.strptime(stringDateProc,"%Y_%m_%d")
                dateProc = dateProc + timedelta(days=1)

                stringDateProc = dateProc.strftime("%Y_%m_%d")

            return content_available


        else:
            raise Exception(str)

    def provide_deletes(self):
        all_deletes = list_files_absolute_sorted(self._config.rero_working_dir,".*delete.*\.xml.gz")
        numberOfRecords = 0
        for record in all_deletes:
            with gzip.open(record, 'rb') as f:
                file_content = f.read().decode("utf-8")
                iterator = self.pIterSingleRecord.finditer(file_content)
                for matchRecord in iterator:
                    contentSingleRecord = matchRecord.group()
                    numberOfRecords += 1
                    yield contentSingleRecord
        self._number_delete_records = numberOfRecords


    def provide_updates(self):
        all_updates = list_files_absolute_sorted(self._config.rero_working_dir,".*update.*\.xml.gz")
        numberOfRecords = 0
        for record in all_updates:
            with gzip.open(record, 'rb') as f:
                file_content = f.read().decode("utf-8")
                iterator = self.pIterSingleRecord.finditer(file_content)
                for matchRecord in iterator:
                    contentSingleRecord = matchRecord.group()
                    numberOfRecords += 1
                    yield contentSingleRecord
        self._number_update_records = numberOfRecords

    def _yield_zipped_records(self, list_of_files, record_iterator):
        numberOfRecords = 0
        for record in list_of_files:
            with gzip.open(record, 'rb') as f:
                file_content = f.read().decode("utf-8")
                iterator = record_iterator.finditer(file_content)
                for matchRecord in iterator:
                    contentSingleRecord = matchRecord.group()
                    numberOfRecords += 1

                    yield contentSingleRecord

        self._number_update_records = numberOfRecords

    def initialize_dirs(self):
        mkdir_if_absent(self._config.rero_working_dir)
        mkdir_if_absent(self._config.rero_src_dir)

