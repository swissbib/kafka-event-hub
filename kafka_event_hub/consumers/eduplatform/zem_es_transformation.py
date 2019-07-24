
import re

class ZemESTransformation():

    def __init__(self, courses : dict):
        self.courses = courses
        self.es = {}
        self.first_2_digits_keywords = re.compile('^\d\d', re.UNICODE | re.DOTALL | re.IGNORECASE)
        self.first_digit_coursetype = re.compile('^A', re.UNICODE | re.DOTALL | re.IGNORECASE)
        self.first_2_digits_language = re.compile('^Sp', re.UNICODE | re.DOTALL | re.IGNORECASE)
        self.holangebot = re.compile('Hol - Angebot', re.UNICODE | re.DOTALL | re.IGNORECASE)


    @property
    def es_structure(self):
        return self.es


    def make_structure(self):
        self._course_name()
        self._key_words()
        self._key_coursetypes()
        self._key_language()
        self._description()
        self._status()
        self._localID()
        self._maxParticipants()
        self._minParticipants()
        self._price()
        self._place()
        self._dates()
        self._subtitle()
        self._goals()
        self._targetAudience()
        self._beginDate()
        self._endDate()
        self._registrationDate()
        self._organiser()
        self._provider()


    def _provider(self):
        self.es["provider"] = "ZEM" #always ZEM

    def _course_name(self):
        self.es["name"] = self.courses["name"] if "name" in self.courses else "na"

    def _key_words(self):
        self.es["keywords"] = self._filteredKeyWords(self.courses["keywords"]) if "keywords" in self.courses else []

    def _key_coursetypes(self):
        self.es["courseType"] = self._filteredCourseType(self.courses["keywords"]) if "keywords" in self.courses else []

    def _key_language(self):
        self.es["language"] = self._filteredLanguageType(self.courses["keywords"]) if "keywords" in self.courses else []


    def _description(self):
        self.es["description"] = self.courses["details"] if "details" in self.courses else "na"

    def _status(self):
        self.es["status"] = self.courses["status"] if "status" in self.courses else "na"

    def _localID(self):
        self.es["localID"] = self.courses["extra_fields"]["com.marketcircle.daylite/extra1"]["value"] if \
            "extra_fields" in self.courses and "com.marketcircle.daylite/extra1"  in self.courses["extra_fields"] \
            and "value" in self.courses["extra_fields"]["com.marketcircle.daylite/extra1"] \
            else "na"

    def _maxParticipants(self):
        self.es["maxParticipants"] = self.courses["extra_fields"]["com.marketcircle.daylite/extra2"]["value"] if \
            "extra_fields" in self.courses and "com.marketcircle.daylite/extra2"  in self.courses["extra_fields"] \
            and "value" in self.courses["extra_fields"]["com.marketcircle.daylite/extra2"] \
            else "na"


    def _minParticipants(self):
        self.es["minParticipants"] = self.courses["extra_fields"]["com.marketcircle.daylite/extra3"]["value"] if \
            "extra_fields" in self.courses and "com.marketcircle.daylite/extra3"  in self.courses["extra_fields"] \
            and "value" in self.courses["extra_fields"]["com.marketcircle.daylite/extra3"] \
            else "na"

    def _price(self):
        self.es["price"] = self.courses["extra_fields"]["com.marketcircle.daylite/extra5"]["value"] if \
            "extra_fields" in self.courses and "com.marketcircle.daylite/extra5"  in self.courses["extra_fields"] \
            and "value" in self.courses["extra_fields"]["com.marketcircle.daylite/extra5"] \
            else "na"


    def _place(self):
        self.es["place"] = self.courses["extra_fields"]["com.marketcircle.daylite/extra6"]["value"] if \
            "extra_fields" in self.courses and "com.marketcircle.daylite/extra6"  in self.courses["extra_fields"] \
            and "value" in self.courses["extra_fields"]["com.marketcircle.daylite/extra6"] \
            else "na"

    def _dates(self):
        #todo: hier Liste als default value??
        self.es["dates"] = self.courses["extra_fields"]["com.marketcircle.daylite/extra7"]["value"] if \
            "extra_fields" in self.courses and "com.marketcircle.daylite/extra7"  in self.courses["extra_fields"] \
            and "value" in self.courses["extra_fields"]["com.marketcircle.daylite/extra7"] \
            else "na"

    def _subtitle(self):
        #todo: hier Liste als default value??
        self.es["subtitle"] = self.courses["extra_fields"]["com.marketcircle.daylite/extra9"]["value"] if \
            "extra_fields" in self.courses and "com.marketcircle.daylite/extra9"  in self.courses["extra_fields"] \
            and "value" in self.courses["extra_fields"]["com.marketcircle.daylite/extra9"] \
            else "na"

    def _goals(self):
        self.es["goals"] = self.courses["extra_fields"]["com.marketcircle.daylite/extra11"]["value"] if \
            "extra_fields" in self.courses and "com.marketcircle.daylite/extra11"  in self.courses["extra_fields"] \
            and "value" in self.courses["extra_fields"]["com.marketcircle.daylite/extra11"] \
            else "na"

    def _targetAudience(self):
        self.es["targetAudience"] = self.courses["extra_fields"]["com.marketcircle.daylite/extra12"]["value"] if \
            "extra_fields" in self.courses and "com.marketcircle.daylite/extra12"  in self.courses["extra_fields"] \
            and "value" in self.courses["extra_fields"]["com.marketcircle.daylite/extra12"] \
            else "na"

    def _beginDate(self):

        if self._check_holangebot() is False:
            self.es["beginDate"] = self.courses["extra_fields"]["com.marketcircle.daylite/extra_date_1"]["value"] if \
                "extra_fields" in self.courses and "com.marketcircle.daylite/extra_date_1"  in self.courses["extra_fields"] \
                and "value" in self.courses["extra_fields"]["com.marketcircle.daylite/extra_date_1"] \
                else "na"

    def _endDate(self):

        if self._check_holangebot() is False:
            self.es["endDate"] = self.courses["extra_fields"]["com.marketcircle.daylite/extra_date_2"]["value"] if \
                "extra_fields" in self.courses and "com.marketcircle.daylite/extra_date_2"  in self.courses["extra_fields"] \
                and "value" in self.courses["extra_fields"]["com.marketcircle.daylite/extra_date_2"] \
                else "na"

    def _registrationDate(self):

        if self._check_holangebot() is False:
            self.es["registrationDate"] = self.courses["extra_fields"]["com.marketcircle.daylite/extra_date_3"]["value"] if \
                "extra_fields" in self.courses and "com.marketcircle.daylite/extra_date_3"  in self.courses["extra_fields"] \
                and "value" in self.courses["extra_fields"]["com.marketcircle.daylite/extra_date_3"] \
                else "na"


    def _organiser(self):
        self.es["organiser"] = {'name': self.courses["companies"]["details"]["name"]} if \
            "companies" in self.courses and "details"  in self.courses["companies"] \
            and "name" in self.courses["cmpanies"]["details"] \
            else {}

    def _filteredKeyWords(self, rawKeywordList):
        return list(map(lambda fw: fw[3:],  filter(lambda v : self.first_2_digits_keywords.search(v),rawKeywordList)))

    def _filteredCourseType(self, rawKeywordList):
        return list(map(lambda fw: fw[2:],  filter(lambda v : self.first_digit_coursetype.search(v),rawKeywordList)))

    def _filteredLanguageType(self, rawKeywordList):
        return list(map(lambda lang:self._map_language(lang),
                        map(lambda fw: fw[3:],  filter(lambda v : self.first_2_digits_language.search(v),rawKeywordList))))


    def _map_language(self, language_value):
        language_codes = {
            'Deutsch': 'ger',
            'Deutsch-English': 'gereng',
            'Deutsch-Espa\u00f1ol': 'gerspa',
            'Deutsch-Fran\u00e7ais': 'gerfre',
            'Deutsch-Fran\u00e7ais-English': 'gerfreeng',
            'English': 'eng',
            'Espa\u00f1ol': 'spa',
            'Fran\u00e7ais': 'fre',
            'Italiano': 'Italiano'
        }

        test = language_codes[language_value] if language_value in language_codes else language_value
        return test

    def _not_hol_angebote(self, rawKeywordList):
        return list(map(lambda lang:self._map_language(lang),
                        map(lambda fw: fw[3:],  filter(lambda v : self.first_2_digits_language.search(v),rawKeywordList))))

    def _get_keywords(self):
        return self.courses["keywords"] if "keywords" in self.courses else []

    def _check_holangebot(self):
        #rule: no dates if holangebot
        keywords = self._get_keywords()
        holangebot = False
        if isinstance(keywords, list) and len(keywords) > 0:
            for elem in keywords:
                if self.holangebot.search(elem):
                    holangebot = True
                    break

        return holangebot