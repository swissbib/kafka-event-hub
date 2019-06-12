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
#geht das??
from kafka_event_hub.config import OAIConfig

from kafka_event_hub.config.config_utility import init_logging




class EduZem(AbstractBaseProducer):

    def __init__(self, configrep: str, configrepshare: str):
        AbstractBaseProducer.__init__(self,configrepshare, OAIConfig, configrep)

        self.headers = {'Authorization': 'Bearer   Qku1tQ6fhUDADQSu4k5fZxA1KBHWPS'}
        self.base_url = "https://api.marketcircle.net"
        self.active = True

        logComponenents = init_logging(configrep, self.configuration)
        self._shortcut_source_name = logComponenents['shortcut_source_name']
        self.source_logger_summary = logComponenents['source_logger_summary']
        self.source_logger = logComponenents['source_logger']



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


    def getProjectId(self, url):
        #todo: fetch project id
        pass

    def process(self):

        response = requests.get(self.base_url + "/v1/projects",headers=self.headers)

        kurse_serialized = open("zem1.json","w")
        kurse_serialized.write("[\n")

        if response.ok:
            text = response.text
            projects = json.loads(text)

            for project in projects:

                projectId = self.getProjectId(project["self"])

                fullproject = requests.get(self.base_url + project["self"],headers=self.headers)
                #fullproject = requests.get(self.base_url + "/v1/projects/997065",headers=self.headers)
                fp = json.loads(fullproject.text)

                if self.active is False:
                    continue


                #set = fp

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
                          message=fp.encode('utf8'))


            #kurse_serialized.write("\n]")
            #kurse_serialized.close()

if __name__ == '__main__':
    zem = EduZem()
    zem.process()
