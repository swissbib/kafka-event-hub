import re
import hashlib
import json

class EventoESTransformation():

    def __init__(self, course : dict):

        self._configuration = None
        self.course = course
        self.es = {}

    def set_configuration(self, value):
        self._configuration = value

    def make_structure(self):


        self._create_id()
        self._course_name()
        self._beginDate()
        self._key_coursetypes()
        self._dates()
        self._description()
        self._endDate()

        self._key_language()
        self._localID()
        self._maxParticipants()
        self._minParticipants()
        self._note()
        self._place()
        self._price()
        self._provider()
        self._registrationDate()
        self._status()
        self._targetAudience()
        self._contacts()

        self._create_full_document()










    @property
    def es_structure(self):
        return self.es

    def _course_name(self):
        #Silvia: aus all-events.Designation
        if "Designation" in self.course:
            self.es["name"] = self.course["Designation"]

    def _beginDate(self):
        #Silvia: aus all-events.DateFrom
        if "DateFrom" in self.course:
            self.es["beginDate"] = self.course["DateFrom"]

    def _key_coursetypes(self):
        #Silvia: aus all-events.AreaOfEducation und all-events.EventCategory und all-events.EventLevel und aus all-events.EventType
        coursetypes = []
        if "AreaOfEducation" in self.course:
            coursetypes.append(self.course["AreaOfEducation"])

        if "EventCategory" in self.course:
            coursetypes.append(self.course["EventCategory"])

        if "EventLevel" in self.course:
            coursetypes.append(self.course["EventLevel"])

        if "EventType" in self.course:
            coursetypes.append(self.course["EventType"])

        self.es["courseType"] = coursetypes

    def _dates(self):
        #Silvia: aus all-events.DateString
        if "DateString" in self.course:
            self.es["dates"] = self.course["DateString"]

    def _description(self):
        #Silvia: aus all_events_texts.Value, wenn Type = Memo und Number = 1

        descriptions = []
        if "event_texts" in self.course:

            #todo
            #description in zem is simple string. So here I'm going to use a concatination of several strings
            #we have to check what to do in the future

            #all_descriptions = " / ".join(list(filter(lambda event_text: event_text['Type'] == 'Memo' and event_text['Number'] == 1, self.course["event_texts"])))
            all_descriptions =  "  / ".join(  list(map(lambda etd: etd['Value'],
                                        filter(lambda event_text_dict:
                                               event_text_dict['Type'] == 'Memo'
                                               and event_text_dict['Number'] == 1, self.course["event_texts"]))))
            #remove carriage return
            all_descriptions = " ".join(all_descriptions.split()) #very simple done
            #self.es["description"] = self.course["details"] if "details" in self.course else "NA"
            self.es["description"] = all_descriptions

    def _endDate(self):
        #Silvia: aus all_events.DateTo
        if "DateTo" in self.course:
            self.es["endDate"] = self.course["DateTo"]

    def _key_language(self):
        #Silvia: aus all_events.LanguageOfInstruction (ist in den beiden Testsätzen null)
        if "LanguageOfInstruction" in self.course and self.course["LanguageOfInstruction"] is not None:
            self.es["language"] = self.course["LanguageOfInstruction"]
        else:
            self.es["language"] = "de" #just a default value

    def _localID(self):
        #aus all_events.Number
        if "Number" in self.course:
            self.es["localID"] = self.course["Number"]

    def _maxParticipants(self):
        #aus all_events.MaxParticipants
        if "MaxParticipants" in self.course:
            self.es["maxParticipants"] = self.course["MaxParticipants"]

    def _minParticipants(self):
        #aus all_events.MinParticipants
        if "MinParticipants" in self.course:
            self.es["minParticipants"] = self.course["MinParticipants"]

    def _note(self):
        #aus all_events_texts.Value, wenn Type = Memo und Number = 3
        if "event_texts" in self.course:

            #todo
            #same question as for _description. By now note is only used for evento not zem
            #we have to check what to do in the future

            all_notes =  "  / ".join(  list(map(lambda etd: etd['Value'],
                                        filter(lambda event_text_dict:
                                               event_text_dict['Type'] == 'Memo'
                                               and event_text_dict['Number'] == 3, self.course["event_texts"]))))

            self.es["note"] = all_notes

    def _place(self):
        #aus all_events.Location
        if "Location" in self.course:
            self.es["place"] = self.course["Location"]


    def _price(self):
        #aus all_events.Price
        if "Price" in self.course:
            self.es["price"] = self.course["Price"]

    def _provider(self):
        self.es["provider"] = "EVENTOTest" #only test and fix value to indicate this

    def _registrationDate(self):
        #aus all_events.SubscriptionDateTo
        if "SubscriptionDateTo" in self.course:
            self.es["registrationDate"] = self.course["SubscriptionDateTo"]

    def _status(self):
        #aus all_events.Status
        if "Status" in self.course:
            self.es["status"] = self.course["Status"]


    def _targetAudience(self):
        #aus all_events_texts.Value,  wenn Type = Memo und Number = 2
        if "event_texts" in self.course:

            #todo
            #same question as for _description. By now note is only used for evento not zem
            #we have to check what to do in the future

            all_targetAudience =  "  / ".join(  list(map(lambda etd: etd['Value'],
                                        filter(lambda event_text_dict:
                                               event_text_dict['Type'] == 'Memo'
                                               and event_text_dict['Number'] == 2, self.course["event_texts"]))))

            self.es["targetAudience"] = all_targetAudience



    def _contacts(self):

        # Definition Silvia for fulldocument
        #      "instructors": {
        #          "properties": {
        #              "companies": {
        #                  "properties": {
        #                      "name"
        #                      "street"
        #                      "city"
        #                      "postalcode"
        #                      "url"
        #                      "title" // aus all_events.Leadership, alles was nach Komma kommt
        #
        #                  }
        #              },
        #              "firstName"
        #              "lastName"  // aus all_events.Leadership, alles was vor Komma kommt
        #              "phone"
        #              "email"
        #              "birthdate"
        #          }
        #      },

        if "Leadership" in self.course:
            contact = {}
            leadership = self.course["Leadership"].split(",")
            if (len(leadership) == 2):
                contact["lastName"] = leadership[0]
                contact["companies"] = [{'title': leadership[1]}]
                self.es["persons"] = [contact]

    def _create_full_document(self):
        fullrecord = {}
        if "beginDate" in self.es:
            fullrecord["beginDate"] = self.es["beginDate"]
        #fullrecord["category"]
        fullrecord["speakers"] = self.es["persons"] if "persons" in self.es else []
        if "name" in self.es:
            fullrecord["coursename"] = self.es["name"]
        #fullrecord["coursename"] = self.es["name"] if "name" in self.es else "NA"
        if "courseType" in self.es:
            fullrecord["courseType"] = self.es["courseType"]

        if "dates" in self.es:
            fullrecord["dates"] = self.es["dates"]
        #fullrecord["dates"] = self.es["dates"] if "dates" in self.es else "NA"
        if "description" in self.es:
            fullrecord["description"] = self.es["description"]
        #fullrecord["description"] = self.es["description"] if "description" in self.es else "NA"
        if "endDate" in self.es:
            fullrecord["endDate"] = self.es["endDate"]

        if "language" in self.es:
            fullrecord["language"] = self.es["language"]

        if "localID" in self.es:
            fullrecord["localID"] = self.es["localID"]

        if "maxParticipants" in self.es:
            fullrecord["maxParticipants"] = self.es["maxParticipants"]

        if "minParticipants" in self.es:
            fullrecord["minParticipants"] = self.es["minParticipants"]

        if "note" in self.es:
            fullrecord["note"] = self.es["note"]

        if "place" in self.es:
            fullrecord["place"] = self.es["place"]

        if "price" in self.es:
            fullrecord["price"] = self.es["price"]

        if "provider" in self.es:
            fullrecord["provider"] = self.es["provider"]

        if "registrationDate" in self.es:
            fullrecord["registrationDate"] = self.es["registrationDate"]

        if "status" in self.es:
            fullrecord["status"] = self.es["status"]

        if "targetAudience" in self.es:
            fullrecord["targetAudience"] = self.es["targetAudience"]

        if "persons" in self.es:
            fullrecord["instructors"] = self.es["persons"]


        self.es["fulldocument"] = json.dumps(fullrecord)

    def _create_id(self):
        #Todo: zem kafka producer should create id consisting of prefix (ZEM) + numeric id coming from zem

        self.es["id"] = "EVENTO" + str(self.course["Id"])
