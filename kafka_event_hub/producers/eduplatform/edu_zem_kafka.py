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




class EduZemKafka(AbstractBaseProducer):

    def __init__(self, configrep: str, configrepshare: str):
        AbstractBaseProducer.__init__(self,configrepshare, EduConfig, configrep)
        self.refresh_access_token()

        self.base_url = self.configuration.base_url
        self.active = True

        logComponenents = init_logging(configrep, self.configuration)
        self._shortcut_source_name = logComponenents['shortcut_source_name']
        self.source_logger_summary = logComponenents['source_logger_summary']
        self.source_logger = logComponenents['source_logger']

        self.last_project_id = self.configuration.last_project_id
        self.active = True if self.last_project_id is None else False


    def processCompany(self, company):

        jsonCompany = self.make_repository_request(company["company"])
        #jsonCompany = json.loads(requests.get(self.base_url + company["company"], headers=self.headers).text)
        privacyCompany = {}

        privacyCompany["addresses"] = jsonCompany["addresses"] if "addresses" in jsonCompany else []
        privacyCompany["urls"] = jsonCompany["urls"] if "urls" in jsonCompany else []
        privacyCompany["name"] = jsonCompany["name"] if "name" in jsonCompany else "na"

        return privacyCompany

    def checkProcessingProject(self, project):
        return True


    def checkStopProcessing(self, project):
        return False

    def process_methods_pricenote_in_form(self,form):

        formprops =  self.make_repository_request(form["form"])
        methods = []
        priceNote = None
        if "values" in formprops:
            for value in formprops["values"]:
                if "name" in value and "values" in value and value["name"] == "Methoden (max. 3):":
                    methods = value["values"]
                if "name" in value and "value" in value and value["name"] == "Bemerkungen zur Teilnahmegeb\u00fchr:\n(max. 200 Zeichen)":
                    priceNote = value["value"]


        return (methods,priceNote)

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

        response = requests.get(self.base_url + "/v1/projects",headers=self.headers)
        #todo post is actually not possible
        # change the type of registered application so post and put methods are allowed
        #status = {"state": {"equal": "open"}}
        #response = requests.post(self.base_url + "/v1/projects",data=status,headers=self.headers)


        if response.ok:
            text = response.text
            projects = json.loads(text)

            for project in projects:

                projectId = self.getProjectId(project["self"])

                if not self.active:
                    if projectId == str(self.last_project_id):
                        #next id should be used
                        self.active = True
                        continue
                    else:
                        self.source_logger_summary.info('\nFetched projectid  {ID} passed'.format(
                            ID=projectId,
                            STARTTIME=current_timestamp()
                        ))
                        continue

                self.check_valid_access_token()

                #fullproject = requests.get(self.base_url + project["self"],headers=self.headers)
                #Beispielprojekt mit pricenote
                #fullproject = requests.get(self.base_url + "/v1/projects/997065",headers=self.headers)

                fp = self.make_repository_request(project["self"])

                #Silvia
                # Ich würde nur die Kurse mit Status in_progress nehmen.
                # Im Moment ist noch deferred und abandoned vorhanden, was ja sicher keine
                # aktuellen Kurse sind.
                # Dann gibt es noch Status new, der aber auch wenig Sinn macht da noch keine
                # vernünftige Kursbeschreibung vorhanden ist. Also besser weg damit.

                #if not "status" in fp or fp["status"] is None or fp["status"] == "done" or fp["status"] == "cancelled":
                if not "status" in fp or fp["status"] is None or fp["status"] != 'in_progress':
                  continue


                #t = open("onlytest.json","w")
                #json.dump(fp,t,indent=20)
                #t.flush()
                #t.close()

                if "forms" in fp:
                    methods = []
                    price_note = None
                    for form in fp["forms"]:
                        method_pricenote_tuple = self.process_methods_pricenote_in_form(form)
                        methods.append(method_pricenote_tuple[0])
                        price_note = method_pricenote_tuple[1]
                    fp["course_methods"] =  list(itertools.chain(*methods))
                    if not price_note is None:
                        fp["price_note"] = price_note


                if "companies" in fp:
                    for company in fp["companies"]:
                        company["details"] = self.processCompany(company)





                if "contacts" in fp:
                    for contact in fp["contacts"]:
                        jsonContactDetails = self.make_repository_request(contact["contact"])
                        #jsonContactDetails = json.loads(requests.get(self.base_url + contact["contact"], headers=self.headers).text)
                        privacyContact = {}
                        privacyContact["prefix"] = jsonContactDetails["prefix"] if "prefix" in jsonContactDetails else "na"
                        privacyContact["first_name"] = jsonContactDetails["first_name"] if "first_name" in jsonContactDetails else "na"
                        privacyContact["last_name"] = jsonContactDetails["last_name"] if "last_name" in jsonContactDetails else "na"
                        privacyContact["birthday"] = jsonContactDetails["birthday"] if "birthday" in jsonContactDetails else "na"
                        privacyContact["keywords"] = jsonContactDetails["keywords"] if "keywords" in jsonContactDetails else []

                        privacyContact["emails"] = jsonContactDetails["emails"] if "emails" in jsonContactDetails else []

                        privacyContact["phone_numbers"] = jsonContactDetails["phone_numbers"] if "phone_numbers" in jsonContactDetails else []

                        if "companies" in jsonContactDetails:
                            privacyContact["companies"] = jsonContactDetails["companies"]
                            for company in privacyContact["companies"]:
                                company["details"] = self.processCompany(company)
                        contact["details"] = privacyContact

                if "tasks" in fp:
                    del fp["tasks"]
                if "notes" in fp:
                    del fp["notes"]
                if "forms" in fp:
                    del fp["forms"]
                if "appointments" in fp:
                    del fp["appointments"]



                self.send(key=projectId.encode('utf8'),
                          message=json.dumps(fp).encode('utf8'))

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

    def make_repository_request(self, query_parameter,
                                retries=3,
                                backoff_factor=0.3,
                                status_forcelist=(500, 502, 504),
                                session=None,):
        #https://www.peterbe.com/plog/best-practice-with-retries-with-requests

        self.source_logger_summary.info("fetch: " + query_parameter + " " + str(datetime.now()))
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
            self.base_url + query_parameter, headers=self.headers
        )

        self.source_logger_summary.info("ready fetch: " + query_parameter + " " + str(datetime.now()))

        return json.loads(response.text)








if __name__ == '__main__':
    #zem = EduZemKafka()
    #zem.process()
    pass

