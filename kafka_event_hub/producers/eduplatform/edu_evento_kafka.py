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

import requests

import json
from kafka_event_hub.producers.base_producer import AbstractBaseProducer
from kafka_event_hub.config import EduConfig
from kafka_event_hub.utility.producer_utility import current_timestamp
from kafka_event_hub.config.config_utility import init_logging
import itertools
import os
import yaml
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry




class EduEventoKafka(AbstractBaseProducer):

    def __init__(self, configrep: str, configrepshare: str):
        AbstractBaseProducer.__init__(self,configrepshare, EduConfig, configrep)

        #self.refresh_access_token()
        self.read_oauth2_credentials()

        self.base_url = self._auth_file_dic['oauth2']['BasisURI'] + os.sep + self._auth_file_dic['oauth2']['EndpunktApi'] + os.sep
        self.url_all_events = self.base_url + "Events/"
        self.url_all_event_texts = self.base_url + "EventTexts/"
        self.url_all_event_locations = self.base_url +  "EventLocations/"
        self.lessons_of_event = self.base_url +  "Events/" + str(self._auth_file_dic['oauth2']['AnlassId']) + "/Lessons"


        self.headers = {'CLX-Authorization': "token_type=urn:ietf:params:oauth:token-type:jwt-bearer, access_token={}".format(self._auth_file_dic['oauth2']['access_token']),
                        'Content-Type': 'application/json'}

        #self.headers = {'CLX-Authorization': 'token_type=urn:ietf:params:oauth:token-type:jwt-bearer',
        #                'access_token': self._auth_file_dic['oauth2']['access_token'], 'Content-Type': 'application/json'}
        self.active = True

        logComponenents = init_logging(configrep, self.configuration)
        self._shortcut_source_name = logComponenents['shortcut_source_name']
        self.source_logger_summary = logComponenents['source_logger_summary']
        self.source_logger = logComponenents['source_logger']

        self.last_project_id = self.configuration.last_project_id
        self.active = True if self.last_project_id is None else False



    def checkProcessingProject(self, project):
        return True


    def checkStopProcessing(self, project):
        return False


    def getProjectId(self, url: str):

        # example for the URL: '/v1/projects/700037' and we are looking for the project number
        projectid = url[url.rfind("/") + 1:]
        self.source_logger_summary.info('\nFetching project id:{PROJECTID} {CURRENTTIME}'.format(
            PROJECTID=projectid,
            CURRENTTIME=current_timestamp()
        ))


        return projectid

    def process(self):


        self.source_logger_summary.info('\n\n\n\nStart Edu zem {SOURCE} {STARTTIME}'.format(
            SOURCE=self._shortcut_source_name,
            STARTTIME=current_timestamp()
        ))

        # Beschreibung Vorgehen pro Evento Schule:

        # Public Token lösen
        # alle Events abholen
        # alle Zusatz Texte zu Events erfassen (individuell pro Schule). Lable -> Memo
        # alle EventLocations abholen
        # Filtern nach EventLevelId (1005 -> LuL Forbildung, Id wird sich für die Produktion noch ändern)
        # Pro gefilterter Event noch die Lektionen abholen (Perfomance)
        # Frage:
        # Wollen sie auch gleich Anmeldungen vornehmen? Wenn ja, müssten ich Ihnen noch 2 Request und ein wenig Businesslogik Kenntnis mehr abgeben.
        # Nein!


        all_events = self.make_repository_request(self.url_all_events)
        all_events_texts = self.make_repository_request(self.url_all_event_texts)
        all_events_locations = self.make_repository_request(self.url_all_event_locations)
        all_lessons_of_events = self.make_repository_request(self.lessons_of_event)

        all = {}
        all["all_events"] = all_events
        all["all_events_texts"] = all_events_texts
        all["all_events_locations"] = all_events_locations
        all["all_lessons_of_events"] = all_lessons_of_events


        evento_out = open("evento_content.json","w")
        evento_out.write(json.dumps(all))

        evento_out.close()


        #self.send(key="4711".encode('utf8'),
        #          message=json.dumps(all_events).encode('utf8'))

        self.source_logger_summary.info('\n\n\n\nFinished Edu zem {SOURCE} {STARTTIME}'.format(
            SOURCE=self._shortcut_source_name,
            STARTTIME=current_timestamp()
        ))


    def read_oauth2_credentials(self):

        special_rep_path = os.path.dirname(self.configuration.config_path_special_rep)
        self._auth_file = special_rep_path + os.sep + self.configuration.auth_file
        try:
            with open(self._auth_file, 'r') as fp:
                self._auth_file_dic = yaml.load(fp)
        except Exception:
            self.source_logger.exception('The config file at %s could not be loaded!', self._auth_file)
            raise Exception

    def refresh_access_token(self):

        self.refresh_timestamp = datetime.now()

        self.read_oauth2_credentials()
        response = requests.post(self._auth_file_dic['oauth2']['base_token_url'],
                                 data={'client_secret': self._auth_file_dic['oauth2']['client_secret'],
                                       'grant_type': 'refresh_token',
                                       'refresh_token': self._auth_file_dic['oauth2']['refresh_token'],
                                       'client_id': self._auth_file_dic['oauth2']['client_id'],
                                       })

        if response is None or not response.ok:
            self.source_logger.exception('error refreshing access_token')
            raise Exception("error refreshing access_token")

        json_data = json.loads(response.text)
        self.headers = {'Authorization': "Bearer  " + json_data["access_token"]}
        self._auth_file_dic['oauth2']['refresh_token'] = json_data["refresh_token"]

        with open(self._auth_file, 'w') as fp:
            yaml.dump(self._auth_file_dic, fp, default_flow_style=False)

    def check_valid_access_token(self):

        time_to_refresh = datetime.now()
        delta = time_to_refresh - self.refresh_timestamp
        if delta.total_seconds() > int(self.configuration.refresh_token_time):
            self.source_logger_summary.info("refreshing access token")
            self.refresh_access_token()

    def make_repository_request(self, url,
                                retries=3,
                                backoff_factor=0.3,
                                status_forcelist=(500, 502, 504),
                                session=None,):
        #https://www.peterbe.com/plog/best-practice-with-retries-with-requests

        self.source_logger_summary.info("fetch: " + url + " " + str(datetime.now()))
        session = session or requests.Session()
        retry = Retry(
            total=retries,
            read=retries,
            connect=retries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,

        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)

        #json_response = None
        #i = 0
        #while (i < 3):
        #    try:
        #        response = session.get(
        #            self.base_url + query_parameter, headers=self.headers
        #        )
        #        if not response is None and response.ok:
        #            break
        #    except Exception as exc:
        #        self.source_logger.exception("error trying to fetch content" + str(exc))
        #        i += 1
        #
        #if not response is None and response.ok:
        #    return json.loads(response.text)
        #else:
        #    self.source_logger.exception("definitely not able to fetch content")
        #    raise Exception("definitely not able to fetch content")


        response = session.get(
            url, headers=self.headers
        )

        self.source_logger_summary.info("ready fetch: " + url + " " + str(datetime.now()))

        return json.loads(response.text)




if __name__ == '__main__':
    #zem = EduZemKafka()
    #zem.process()
    pass

