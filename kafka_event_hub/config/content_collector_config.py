# coding: utf-8

__author__ = 'swissbib - UB Basel, Switzerland, Guenter Hipler'
__copyright__ = "Copyright 2019, swissbib project"
__credits__ = []
__license__ = "GNU General Public License v3.0"
__version__ = "0.1"
__maintainer__ = "Guenter Hipler"
__email__ = "guenter.hipler@unibas.ch"
__status__ = "in development"
__description__ = """
                    """

from kafka_event_hub.utility.producer_utility import current_timestamp, current_utc_timestamp
from kafka_event_hub.config import ProducerConfig
import logging
import yaml
import re

"""
actually I'm not sure how to differentiate configurations for different channels in the area of content collector
for a starter I use the base class for all common affairs the area of conmtent collector and even more
specialised types for the various pipes   
"""
class ContentCollectorConfig(ProducerConfig):

    def __init__(self, config_path: str, config_path_special: str=None,logger=logging.getLogger(__name__)):
        super().__init__(config_path, config_path_special,logger=logger)
        self._processStarttime = current_utc_timestamp(self.configuration['Processing']['Default']['granularity'])
        self.p_basedir_pattern = re.compile('{basedir}', re.IGNORECASE)

    def initialize(self, configpathrep):
        pass


    def update_start_time(self):
        granularity = self.granularity
        if granularity is not None:
            #todo: ist es mal nicht typ str??
            granularity = str(granularity)
        self.specializedConfiguration['OAI']['timestampUTC'] = self._processStarttime


    def update_stop_time(self):
        self.specializedConfiguration['Processing']["Default"]['stoppageTime'] = current_timestamp()



    @property
    def processStarttime(self):
        return self._processStarttime

    @property
    def identifier_key(self):
        return self.configuration['Processing']['Default']['identifierkey']

    @property
    def granularity(self):
        return self.configuration['Processing']['Default']['granularity']


    @processStarttime.setter
    def processStarttime(self, starttime):
        #todo: check validaty in relation to granularity pattern
        self._processStarttime = starttime

    @property
    def configuration(self):
        if not hasattr(self,"_yamlmerged") or self._yamlmerged is None:
            return super().configuration
        else:
            return self._yamlmerged

    @property
    def specializedConfiguration(self):
        return self._yamlspecial

    @property
    def logs_config(self):
        return self.configuration['Logging']['config']

    @property
    def basedir(self):
        return self.configuration['Processing']['Default']['baseDir']


    @property
    def deleted_pattern(self):
        return self.configuration['Processing']['Default']['deletedPattern']

    @property
    def header_pattern(self):
        return self.configuration['Processing']['Default']['headerPattern']


    def replace_base_dir(self, defined_dir: str):
        return  re.sub(self.p_basedir_pattern,self.basedir,defined_dir)



    def store(self):
        with open(self._config_path_rep, 'w') as fp:
            yaml.dump(self.specializedConfiguration, fp, default_flow_style=False)



class OAIConfig(ContentCollectorConfig):

    def __init__(self, config_path: str, config_path_special: str= None, logger=logging.getLogger(__name__)):
        super().__init__(config_path, config_path_special, logger=logger)

    @property
    def metadata_prefix(self):
        return None if self.configuration['OAI'].get('metadataPrefix') is None \
            else self.configuration['OAI']['metadataPrefix']

    @property
    def oai_set(self):
        return None if self.configuration['OAI'].get('set') is None else self.configuration['OAI']['set']

    @property
    def timestamp_utc(self):
        return None if self.configuration['OAI'].get('timestampUTC') is None \
            else self.configuration['OAI']['timestampUTC']

    @property
    def oai_until(self):
        return None if self.configuration['OAI'].get('until') is None \
            or self.configuration['OAI'].get('until') == 'None' else self.configuration['OAI']['until']

    @property
    def oai_verb(self):
        return 'ListRecords' if self.configuration['OAI'].get('verb') is None \
            or self.configuration['OAI'].get('verb') == 'None' else self.configuration['OAI']['verb']


class EduConfig(ContentCollectorConfig):
    def __init__(self, config_path: str, config_path_special: str= None, logger=logging.getLogger(__name__)):
        super().__init__(config_path, config_path_special, logger=logger)

    @property
    def base_url(self):
        return self.configuration['EDU']['url']

    @property
    def auth_token(self):
        return self.configuration['EDU']['token']

    @property
    def last_project_id(self):
        return None if self.configuration['EDU'].get('lastproject') is None \
            else self.configuration['EDU'].get('lastproject')

    @property
    def config_path_special_rep(self):
        return self._config_path_rep

    @property
    def auth_file(self):
        return self.configuration['EDU']['auth_file']

    @property
    def refresh_token_time(self):
        return self.configuration['EDU']['time_to_refresh_token']


    @property
    def persons_to_enrich(self):
        return self.configuration['Transformations']['enrich_persons'] \
            if 'Transformations' in self.configuration \
               and 'enrich_persons' in self.configuration['Transformations'] else None

    @property
    def keywords_to_enrich(self):
        return self.configuration['Transformations']['keywords'] \
            if 'Transformations' in self.configuration \
               and 'keywords' in self.configuration['Transformations'] else None

    @property
    def code_data_provider(self):
        return self.configuration['EDU']['code_data_provider'] \
            if 'EDU' in self.configuration \
               and 'code_data_provider' in self.configuration['EDU'] else 'DefaultProvider'

    @property
    def field_type_description(self):
        return self.configuration['ES']['field_type_description'] \
            if 'ES' in self.configuration \
               and 'field_type_description' in self.configuration['ES'] else \
            '/basedir/configs/eduplatform/indexfieldtypes.json'



class FileNebisScpConfig(ContentCollectorConfig):

    def __init__(self, config_path: str, config_path_special: str = None, logger=logging.getLogger(__name__)):
        super().__init__(config_path, config_path_special, logger=logger)

    @property
    def incoming_dir(self):
        return self.replace_base_dir(self.configuration['Filepush']['incomingDir'])

    @property
    def working_dir(self):
        return self.replace_base_dir(self.configuration['Filepush']['nebisWorking'])

    @property
    def nebis_src_dir(self):
        return self.replace_base_dir(self.configuration['Filepush']['nebisSrcDir'])

    @property
    def nebis_prepare_deleted(self):
        return self.replace_base_dir(self.configuration['Datacleaner']['prepareDeleted'])

    @property
    def nebis_marc_record(self):
        return self.replace_base_dir(self.configuration['Datacleaner']['marcRecord'])

    @property
    def nebis_record_body(self):
        return self.replace_base_dir(self.configuration['Datacleaner']['recordBody'])


class FileReroWebDavConfig(ContentCollectorConfig):

    def __init__(self, config_path: str, config_path_special: str = None, logger=logging.getLogger(__name__)):
        super().__init__(config_path, config_path_special, logger=logger)


    @property
    def basedir_webdav(self):
        return self.replace_base_dir( self.configuration['WebDav']['basedirwebdav'])

    @property
    def process_mode(self):
        return self.configuration['WebDav']['processMode']

    @property
    def single_record_iterator(self):
        #todo: kann ich diese property mit recordBodyRegEx, die bisher in test verwendet wird
        #vereinheitlichen? dort fuer cleansing. jezt ein enig ein durcheinander!
        return self.configuration['WebDav']['singleRecordIterator']



    @property
    def rero_src_dir(self):
        return self.replace_base_dir(self.configuration['WebDav']['srcDir'])

    @property
    def rero_working_dir(self):
        return self.replace_base_dir( self.configuration['WebDav']['workingDir'])

    @property
    def latest_proc_date(self):
        return str(self.configuration['WebDav']['latestProcDate'])


    def update_latest_proc_date(self, value):
        self.specializedConfiguration['WebDav']['latestProcDate'] = value

