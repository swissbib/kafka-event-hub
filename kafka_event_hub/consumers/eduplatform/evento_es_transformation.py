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

            #self.es["description"] = self.course["details"] if "details" in self.course else "NA"
            self.es["description"] = all_descriptions

    def _endDate(self):
        #Silvia: aus all_events.DateTo
        pass

    def _key_language(self):
        #Silvia: aus all_events.LanguageOfInstruction (ist in den beiden Testsätzen null)
        pass

    def _localID(self):
        #aus all_events.Number
        pass

    def _maxParticipants(self):
        #aus all_events.MaxParticipants
        pass

    def _minParticipants(self):
        #aus all_events.MinParticipants
        pass
