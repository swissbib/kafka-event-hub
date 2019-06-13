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




class EduZemKafka(AbstractBaseProducer):

    def __init__(self, configrep: str, configrepshare: str):
        AbstractBaseProducer.__init__(self,configrepshare, EduConfig, configrep)

        self.headers = {'Authorization': "Bearer  " + self.configuration.auth_token}
        self.base_url = self.configuration.base_url
        self.active = True

        logComponenents = init_logging(configrep, self.configuration)
        self._shortcut_source_name = logComponenents['shortcut_source_name']
        self.source_logger_summary = logComponenents['source_logger_summary']
        self.source_logger = logComponenents['source_logger']

        self.last_project_id = self.configuration.last_project_id
        self.active = True if self.last_project_id is None else False


    def processCompany(self, company):

        jsonCompany = json.loads(requests.get(self.base_url + company["company"], headers=self.headers).text)
        privacyCompany = {}

        privacyCompany["addresses"] = jsonCompany["addresses"] if "addresses" in jsonCompany else []
        privacyCompany["urls"] = jsonCompany["urls"] if "urls" in jsonCompany else []
        privacyCompany["name"] = jsonCompany["name"] if "name" in jsonCompany else "na"

        return privacyCompany

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


        response = requests.get(self.base_url + "/v1/projects",headers=self.headers)


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


                fullproject = requests.get(self.base_url + project["self"],headers=self.headers)
                #fullproject = requests.get(self.base_url + "/v1/projects/997065",headers=self.headers)
                fp = json.loads(fullproject.text)


                if "companies" in fp:
                    for company in fp["companies"]:
                        company["details"] = self.processCompany(company)


                if "contacts" in fp:
                    for contact in fp["contacts"]:
                        jsonContactDetails = json.loads(requests.get(self.base_url + contact["contact"], headers=self.headers).text)
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



if __name__ == '__main__':
    #zem = EduZemKafka()
    #zem.process()
    pass

