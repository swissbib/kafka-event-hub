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
        self._key_language()
        self._localID()
        self._maxParticipants()
        self._minParticipants()
        self._methods()
        self._note()
        self._place()
        self._price()
        self._provider()
        self._registrationDate()
        self._registrationInfo()
        self._requirements()
        self._status()
        self._targetAudience()

        #new
        self._certificate()
        self._priceNote()


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



    def _certificate(self):

        searched_element = self._check_event_text_sequence(searched_element='Abschluss')

        #if len(searched_element) > 0:
        #    self.es['certificate'] = searched_element
        self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                   searched_element,
                                                                   "certificate")


    def _priceNote(self):

        searched_element = self._check_event_text_sequence(searched_element='Kosten')
        searched_element.extend(self._check_event_text_sequence(searched_element='Kosten '))

        #if len(searched_element) > 0:
        #  self.es['priceNote'] = searched_element
        self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                   searched_element,
                                                                   "priceNote")



    def _courseName(self):
        #Silvia: aus all-events.Designation
        if "Designation" in self.course:
            #self.es["courseName"] = self.course["Designation"]
            self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                       self.course["Designation"],
                                                                       "courseName")

    def _beginDate(self):
        #Silvia: aus all-events.DateFrom
        if "DateFrom" in self.course and self.course["DateFrom"] is not None:
            #self.es["beginDate"] = self.course["DateFrom"]
            self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                       self.course["DateFrom"],
                                                                       "beginDate")

    def _key_coursetypes(self):
        #Silvia: aus all-events.AreaOfEducation und all-events.EventCategory und all-events.EventLevel und aus all-events.EventType
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

        if "EventCategory" in self.course and self.course["EventCategory"] is not None:
            #self.es["keywords"] = self.course["EventCategory"]
            self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                       self.course["EventCategory"],
                                                                       "keywords")


    def _dates(self):
        ####if text mit Label für dates: diesen Text in self.es["dates"]

        #else if DateFrom = DateTo: Weekday DateFrom (in Datum umgewandelt), TimeFrom - TimeTo
        #else: DateString, jeweils Weekday TimeFrom-TimeTo

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

        searched_element = self._check_event_text_sequence(searched_element='Ausbildungsinhalt')
        searched_element.extend(self._check_event_text_sequence(searched_element='Inhalt'))
        searched_element.extend(self._check_event_text_sequence(searched_element='Kursinhalt'))
        searched_element.extend(self._check_event_text_sequence(searched_element='Kursinhalt '))
        searched_element.extend(self._check_event_text_sequence(searched_element='Kursinhalt, -ziel, Arbeitsweise'))
        searched_element.extend(self._check_event_text_sequence(searched_element='Programm/Kursinhalt'))

        #if len(searched_element) > 0:
        #    self.es['description'] = searched_element
        self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                   searched_element,
                                                                   "description")




    def _endDate(self):
        #Silvia: aus all_events.DateTo
        if "DateTo" in self.course and self.course["DateTo"] is not None:
            #self.es["endDate"] = self.course["DateTo"]
            self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                       self.course["DateTo"],
                                                                       "endDate")

    def _goals(self):

        searched_element = self._check_event_text_sequence(searched_element='Kursziel')
        #if len(searched_element) > 0:
        #self.es['goals'] = searched_element
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

    def _key_language(self):
        #Silvia: aus all_events.LanguageOfInstruction (ist in den beiden Testsätzen null)
        if "LanguageOfInstruction" in self.course and self.course["LanguageOfInstruction"] is not None:
            #self.es["language"] = self.course["LanguageOfInstruction"]

            #Todo: Hinweis Silvia - Nutzung der daylite Relationen

            self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                            self.edu_utilities._map_language(self.course["LanguageOfInstruction"]),
                                                                       "language")
        else:
            #self.es["language"] = "ger" #just a default value
            self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                       "ger",
                                                                       "language")

    def _localID(self):
        #aus all_events.Number
        if "Number" in self.course:
            self.es["localID"] = self.course["Number"]
            self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                       self.course["Number"],
                                                                       "localID")

    def _maxParticipants(self):
        #aus all_events.MaxParticipants
        if "MaxParticipants" in self.course:
            #self.es["maxParticipants"] = self.course["MaxParticipants"]
            self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                       self.course["MaxParticipants"],
                                                                       "maxParticipants")

    def _minParticipants(self):
        #aus all_events.MinParticipants
        if "MinParticipants" in self.course:
            #self.es["minParticipants"] = self.course["MinParticipants"]
            self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                       self.course["MinParticipants"],
                                                                       "minParticipants")

    def _methods(self):

        searched_element = self._check_event_text_sequence(searched_element='Arbeitsweise')
        searched_element.extend(self._check_event_text_sequence(searched_element='Methodik'))
        #if len(searched_element) > 0:
        #    self.es['methods'] = searched_element
        self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                   searched_element,
                                                                   "methods")

    def _note(self):

        searched_element = self._check_event_text_sequence(searched_element='Bemerkung')
        searched_element.extend(self._check_event_text_sequence(searched_element='Bemerkungen'))
        searched_element.extend(self._check_event_text_sequence(searched_element='Durchf\u00fchrung'))
        searched_element.extend(self._check_event_text_sequence(searched_element='Ferien'))
        searched_element.extend(self._check_event_text_sequence(searched_element='Hinweis'))
        searched_element.extend(self._check_event_text_sequence(searched_element='Information'))
        searched_element.extend(self._check_event_text_sequence(searched_element='Informationen'))
        searched_element.extend(self._check_event_text_sequence(searched_element='Kleidung'))
        searched_element.extend(self._check_event_text_sequence(searched_element='Lehrmittel'))
        searched_element.extend(self._check_event_text_sequence(searched_element='Link'))
        searched_element.extend(self._check_event_text_sequence(searched_element='Selbststudium'))
        searched_element.extend(self._check_event_text_sequence(searched_element='Tr\u00e4gerschaft'))
        searched_element.extend(self._check_event_text_sequence(searched_element='Video'))
        searched_element.extend(self._check_event_text_sequence(searched_element='Weitere Informationen'))
        searched_element.extend(self._check_event_text_sequence(searched_element='Weitere Termine'))
        searched_element.extend(self._check_event_text_sequence(searched_element='WICHTIG'))
        searched_element.extend(self._check_event_text_sequence(searched_element='Zeitaufwand'))
        searched_element.extend(self._check_event_text_sequence(searched_element='Zeitungsartikel'))



        #if len(searched_element) > 0:
        #    self.es['note'] = searched_element
        self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                   searched_element,
                                                                   "note")

    def _place(self):

        #I use a list because various locations might be possible
        locations = []

        if 'event_locations' in self.course and self.course['event_locations'] is not None \
                and  isinstance(self.course['event_locations'], list):
            for elem in self.course['event_locations']:
                location = []
                if 'BuildingAddress' in elem.keys():
                    location.append(elem['BuildingAddress'])

                if 'BuildingZip' in elem.keys():
                    location.append(" " + elem['BuildingZip'] + " ")

                if 'BuildingLocation' in elem.keys():
                    location.append(" " + elem['BuildingLocation'] + " ")

                #@Silvia: I haven't seen BuildingName (ResocurceDesignation)
                if 'BuildingName' in elem.keys():
                    location.append(" " + elem['BuildingName'] + " ")

                locations.append("".join(location))

        #@Sivia: don't know if it's possible that we have locations at two different places. I take it as given for
        #the moment
        if "Location" in self.course and self.course["Location"] is not None:
            locations.append(self.course["Location"])

        #if len(locations) > 0:
        #    self.es["place"] = "".join(locations)
        self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                   locations,
                                                                   "place")



    def _price(self):
        #aus all_events.Price
        if "Price" in self.course and self.course["Price"] is not None:
            #self.es["price"] = self.course["Price"]
            self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                       self.course["Price"],
                                                                       "price")

    def _provider(self):
        #self.es["provider"] = "EVENTOTest" #only test and fix value to indicate this
        #self.es["provider"] = self._provider_Code
        self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                   self._provider_Code,
                                                                   "provider")

    def _registrationDate(self):
        #aus all_events.SubscriptionDateTo
        if "SubscriptionDateTo" in self.course and self.course["SubscriptionDateTo"] is not None:
            #self.es["registrationDate"] = self.course["SubscriptionDateTo"]
            self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                       self.course["SubscriptionDateTo"],
                                                                       "registrationDate")

    def _registrationInfo(self):

        searched_element = self._check_event_text_sequence(searched_element='Anmeldung ')
        searched_element.extend(self._check_event_text_sequence(searched_element='Anmeldung'))
        if len(searched_element) > 0:
            #self.es['registrationInfo'] = searched_element
            self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                       searched_element,
                                                                       "registrationInfo")

    def _requirements(self):

        searched_element = self._check_event_text_sequence(searched_element='Voraussetzung')
        searched_element.extend(self._check_event_text_sequence(searched_element='Voraussetzungen'))
        if len(searched_element) > 0:
            #self.es['requirements'] = searched_element
            self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                       searched_element,
                                                                       "requirements")

    def _status(self):
        #aus all_events.Status
        if "Status" in self.course:
            #self.es["status"] = self.course["Status"]
            self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                       self.course["Status"],
                                                                       "status")


    def _targetAudience(self):

        searched_element = self._check_event_text_sequence(searched_element='Zielgruppe')
        searched_element.extend(self._check_event_text_sequence(searched_element='Zielgruppe '))
        searched_element.extend(self._check_event_text_sequence(searched_element='Zielpublikum'))
        #if len(searched_element) > 0:
        #    self.es['targetAudience'] = searched_element
        self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                   searched_element,
                                                                   "targetAudience")


    def _create_full_document(self):

        fullrecord = {}

        simplefields = ['beginDate', 'courseName', 'courseType', 'dates', 'description', 'endDate', 'goals',
                        'language', 'localID', 'methods', 'maxParticipants', 'minParticipants', 'note',
                        'organiser', 'place', 'price', 'priceNote', 'registrationDate', 'status', 'subtitle',
                        'targetAudience', 'provider', "certificate", "instructorsNote", "registrationInfo",
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
        if "Id" in self.course:
            self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                       self._provider_Code + str(self.course["Id"]),
                                                                       "id")

    @property
    def _provider_Code(self):
        return self._configuration['EDU']['code_data_provider'] \
            if 'EDU' in self._configuration \
               and 'code_data_provider' in self._configuration['EDU'] else 'DefaultProvider'