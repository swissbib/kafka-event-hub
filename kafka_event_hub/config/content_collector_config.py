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

"""
actually I'm not sure how to differentiate configurations for different channels in the area of content collector
for a starter I use the base class for all common affairs the area of conmtent collector and even more
specialised types for the various pipes   
"""
class ContentCollectorConfig(ProducerConfig):

    def __init__(self, config_path: str, config_path_special: str=None,logger=logging.getLogger(__name__)):
        super().__init__(config_path, config_path_special,logger=logger)
        self._processStarttime = current_utc_timestamp(self.configuration['Processing']['Default']['granularity'])

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


class FileNebisScpConfig(ContentCollectorConfig):

    def __init__(self, config_path, logger=logging.getLogger(__name__)):
        super().__init__(config_path=config_path, logger=logger)

    @property
    def incoming_dir(self):
        return self.configuration['Processing']['nebis']['incomingDir']

    @property
    def working_dir(self):
        return self.configuration['Processing']['nebis']['nebisWorking']

    @property
    def nebis_src_dir(self):
        return self.configuration['Processing']['nebis']['nebisSrcDir']


class FileReroWebDavConfig(ContentCollectorConfig):

    def __init__(self, config_path, logger=logging.getLogger(__name__)):
        super().__init__(config_path=config_path, logger=logger)

    @property
    def basedir_webdav(self):
        return self.configuration['Processing']['rero']['basedirwebdav']

    @property
    def process_mode(self):
        return self.configuration['Processing']['rero']['processMode']

    @property
    def rero_src_dir(self):
        return self.configuration['Processing']['rero']['reroSrcDir']

    @property
    def rero_working_dir(self):
        return self.configuration['Processing']['rero']['reroWorkingDir']

    @property
    def latest_proc_date(self):
        return self.configuration['Processing']['rero']['latestProcDate']
