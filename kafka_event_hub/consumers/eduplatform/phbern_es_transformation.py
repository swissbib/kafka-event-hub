import re
import hashlib
import json
from kafka_event_hub.consumers.eduplatform.edu_utilities import EduplatformUtilities


from datetime import datetime

class EventoESTransformation():

    def __init__(self, course : dict, utilities: EduplatformUtilities):

        self._configuration = None
        self.edu_utilities = utilities
        self.course = course
        self.es = {}

    def set_configuration(self, value):
        self._configuration = value

    def make_structure(self):


        self._create_id()
        self._courseName()
        self._beginDate()
        self._key_coursetypes()
        self._keywords()
        self._dates()

        #ab hier weiter
        self._description()
        self._endDate()
        self._goals()
        self._instructorsNote()
        self._localID()
        self._methods()
        self._note()
        self._place()
        self._priceNote()
        self._provider()
        self._registrationDate()
        self._requirements()
        self._status()
        self._targetAudience()
        self._subtitle()



        #now create full document out of single elements
        self._create_full_document()




    @property
    def es_structure(self):
        return self.es



    def _check_event_text_single(self, event_text, label_element):
        if 'Number' in event_text and 'Number' in label_element and 'Type' in event_text \
            and 'Type' in label_element and event_text['Number'] == label_element['Number'] and \
                event_text['Type'] == 'Memo':
            return event_text['Value']


    def _check_event_text_sequence(self, searched_element):

        type_memo = []
        if "event_texts" in self.course:
            all_certificates_label =  list(filter(lambda event_text_dict:
                                               event_text_dict['Type'] == 'Label'
                                               and event_text_dict['Value'] == searched_element, self.course["event_texts"]))

            if len(all_certificates_label) > 0:

                for label_element in all_certificates_label:
                    type_memo = list(filter (lambda elem: elem is not None,
                                           map(lambda event_text: self._check_event_text_single(event_text,
                                                            label_element),self.course["event_texts"])))

        return type_memo



    def _priceNote(self):
        # @Günter: siehe Beispiele

        searched_element = self._check_event_text_sequence(searched_element='Kosten')
        searched_element.extend(self._check_event_text_sequence(searched_element='Kosten '))

        #if len(searched_element) > 0:
        #  self.es['priceNote'] = searched_element
        self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                   searched_element,
                                                                   "priceNote")



    def _courseName(self):
        if "titel" in self.course:
            self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                       self.course["titel"],
                                                                       "courseName")

    def _beginDate(self):
        # @Günter: aus termine, das erste/tiefste Datum aus start (vlg. Beispiele)
        if "DateFrom" in self.course and self.course["DateFrom"] is not None:
            #self.es["beginDate"] = self.course["DateFrom"]
            self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                       self.course["DateFrom"],
                                                                       "beginDate")

    def _key_coursetypes(self):
        # noch offen
        coursetypes = []
        if "AreaOfEducation" in self.course:
            coursetypes.append(self.course["AreaOfEducation"])

        if "EventLevel" in self.course:
            coursetypes.append(self.course["EventLevel"])

        if "EventType" in self.course:
            coursetypes.append(self.course["EventType"])

        #self.es["courseType"] = coursetypes

        self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                   coursetypes,
                                                                   'courseType')

    def _keywords(self):

        searched_element = self._check_event_text_sequence(searched_element='primaereAngebotsgruppe')
        searched_element.extend(self._check_event_text_sequence(searched_element='suchbegriffe'))

        self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                   searched_element,
                                                                   "keywords")


    def _dates(self):
        # @Günter: siehe Beispiele

        dates = []

        dates.extend(self._check_event_text_sequence(searched_element='Kursdaten/Zeiten'))
        dates.extend(self._check_event_text_sequence(searched_element='Unterrichtszeiten'))
        dates.extend(self._check_event_text_sequence(searched_element='Unterrichtszeiten/Zeitaufwand'))
        dates.extend(self._check_event_text_sequence(searched_element='Unterrichtzeiten'))
        dates.extend(self._check_event_text_sequence(searched_element='Unterrichtzeiten/Zeitaufwand'))
        dates.extend(self._check_event_text_sequence(searched_element='Zeiten'))

        if len(dates) == 0:
            if 'DateFrom' in self.course and 'DateTo' in self.course \
                and self.course['DateFrom'] is not None and self.course['DateTo'] is not None \
                and self.course['DateFrom'] != '' and self.course['DateTo'] != '' \
                and self.course['DateFrom'] == self.course['DateTo']:
                dates.append(datetime.strftime(datetime.strptime(self.course['DateFrom'], '%Y-%m-%dT%H:%M:%S'),
                                               '%d.%m.%Y'))
            if 'TimeFrom' in self.course and 'DateTo' in self.course \
                and self.course['TimeFrom'] is not None and self.course['DateTo'] is not None \
                and self.course['TimeFrom'] != '' and self.course['DateTo'] != '':
                dates.append(self.course['TimeFrom'] + " - " + self.course['TimeTo'])

        #self.es["dates"] = dates
        self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                   dates,
                                                                   "dates")



    def _description(self):

        searched_element = self._check_event_text_sequence(searched_element='einleitungstext')
        searched_element.extend(self._check_event_text_sequence(searched_element='inhalte'))

        #if len(searched_element) > 0:
        #    self.es['description'] = searched_element
        self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                   searched_element,
                                                                   "description")




    def _endDate(self):
        # @Günter: aus termine, letztes/höchstes ende-Datum (vgl. Beispiele)
        if "DateTo" in self.course and self.course["DateTo"] is not None:
            #self.es["endDate"] = self.course["DateTo"]
            self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                       self.course["DateTo"],
                                                                       "endDate")

    def _goals(self):

        searched_element = self._check_event_text_sequence(searched_element='ziele')
        searched_element.extend(self._check_event_text_sequence(searched_element='kompetenzen'))

        #if len(searched_element) > 0:
        #    self.es['description'] = searched_element
        self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                   searched_element,
                                                                   "goals")


    def _instructorsNote(self):
        #aus all_events.Leadership
        if "Leadership" in self.course and self.course["Leadership"] is not None:
            #self.es["instructorsNote"] = self.course["Leadership"]
            self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                       self.course["Leadership"],
                                                                       "instructorsNote")


    def _localID(self):
        if "angebotNr" in self.course:
            self.es["localID"] = self.course["angebotNr"]
            self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                       self.course["angebotNr"],
                                                                       "localID")

    def _methods(self):

        searched_element = self._check_event_text_sequence(searched_element='arbeitsweiseWerkzeuge')
        self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                   searched_element,
                                                                   "methods")

    def _note(self):

        searched_element = self._check_event_text_sequence(searched_element='bemerkungen')
        self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                   searched_element,
                                                                   "note")

    def _place(self):
        # @Günter: Hier sollte noch der Standardtext "PH Bern, Standort" vor den Inhalt von Standort gesetzt werden

        searched_element = self._check_event_text_sequence(searched_element='standort')
        self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                   searched_element,
                                                                   "place")


    def _provider(self):
        # @Günter: code_data_provider sollte PHBern sein
        self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                   self._provider_Code,
                                                                   "provider")

    def _registrationDate(self):
        if "anmeldeschluss" in self.course and self.course["anmeldeschluss"] is not None:
            self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                       self.course["anmeldeschluss"],
                                                                       "registrationDate")

    def _requirements(self):

        searched_element = self._check_event_text_sequence(searched_element='voraussetzungen')
        if len(searched_element) > 0:
            #self.es['requirements'] = searched_element
            self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                       searched_element,
                                                                       "requirements")

    def _status(self):
        if "angebotStatus" in self.course:
            #self.es["status"] = self.course["Status"]
            self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                       self.course["angebotStatus"],
                                                                       "status")


    def _targetAudience(self):

        searched_element = self._check_event_text_sequence(searched_element='zielgruppen')
        #if len(searched_element) > 0:
        #    self.es['targetAudience'] = searched_element
        self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                   searched_element,
                                                                   "targetAudience")

    def _subtitle(self):

        searched_element = self._check_event_text_sequence(searched_element='lead')
        self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                   searched_element,
                                                                   "subtitle")


    def _create_full_document(self):

        fullrecord = {}

        simplefields = ['beginDate', 'courseName', 'courseType', 'dates', 'description', 'endDate', 'goals',
                        'localID', 'methods', 'note',
                        'organiser', 'place', 'priceNote', 'registrationDate', 'status', 'subtitle',
                        'targetAudience', 'provider', "instructorsNote", "registrationInfo",
                        "requirements"]
        fullrecord = {}

        for fieldname in simplefields:
            if fieldname in self.es:
                fullrecord[fieldname] = self.es[fieldname]

        #todo - or only remark: there is a specialized prepration for keywords in the fulldocument of daylite
        # (have forgotten why) Therefor we can't put it into the simplefields list
        if "keywords" in self.es:
            fullrecord["keywords"] = self.es["keywords"]

        #remark: no persons in the evento source because they don't provide structured informations related to persons

        self.es["fulldocument"] = json.dumps(fullrecord)

    def _create_id(self):
        if "angebotId" in self.course:
            self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                       self._provider_Code + str(self.course["angebotId"]),
                                                                       "id")

    @property
    def _provider_Code(self):
        return self._configuration['EDU']['code_data_provider'] \
            if 'EDU' in self._configuration \
               and 'code_data_provider' in self._configuration['EDU'] else 'DefaultProvider'