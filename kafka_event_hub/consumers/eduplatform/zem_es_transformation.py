
import re
import hashlib
import json
from kafka_event_hub.consumers.eduplatform.edu_utilities import EduplatformUtilities

class ZemESTransformation():

    def __init__(self, course : dict, utilities: EduplatformUtilities):

        self._configuration = None

        self.edu_utilities = utilities

        self.course = course
        self.es = {}
        self.first_2_digits_keywords = re.compile('^\d\d', re.UNICODE | re.DOTALL | re.IGNORECASE)
        self.first_digit_coursetype = re.compile('^A', re.UNICODE | re.DOTALL | re.IGNORECASE)
        self.first_2_digits_language = re.compile('^Sp', re.UNICODE | re.DOTALL | re.IGNORECASE)
        self.holangebot = re.compile('Hol - Angebot', re.UNICODE | re.DOTALL | re.IGNORECASE)
        self.relevant_contact_role = re.compile('Referent|Kursleiter', re.UNICODE | re.DOTALL | re.IGNORECASE)
        self.leiter_contact_role = re.compile('Kursleiter', re.UNICODE | re.DOTALL | re.IGNORECASE)
        self.referent_contact_role = re.compile('Referent', re.UNICODE | re.DOTALL | re.IGNORECASE)
        self.work_phone_number = re.compile('Arbeit', re.UNICODE | re.DOTALL | re.IGNORECASE)

        #self.reg_number = re.compile("^\d+?\.\d+?$", re.UNICODE | re.DOTALL | re.IGNORECASE)
        self.reg_number = re.compile("^[0-9]*\.?[0-9]+$", re.UNICODE | re.DOTALL | re.IGNORECASE)

        self.keywords_codes = {
            "Comp\u00e9tences transversales des enseignant-e-s": "Überfachliche Kompetenzen Lehrpersonen",
            "Erstsprache - Weitere Angebote finden Sie auf unserer franz\u00f6sischen Seite.": "Erstsprache",
            "Langue premi\u00e8re - offres suppl\u00e9mentaires sur notre page Internet en fran\u00e7ais": "Erstsprache",
            "Langues secondes - Vous trouverez d'autres offres sur notre page al\u00e9manique.": "Zweitsprachen",
            "Zweitsprachen - Weitere Angebote finden Sie auf unserer franz\u00f6sischen Seite.": "Zweitsprachen",
            "Math\u00e9matiques": "Mathematik",
            "Physique": "Physik",
            "G\u00e9ographie": "Geographie",
            "Histoire": "Geschichte",
            "Informatique": "Informatik",
            "Technologies de l'information et des m\u00e9dias": "Informationstechnologie und Medien",
            "Zusatzausbildungen (Ausbildungen, die auf eine zus\u00e4tzliche Funktion vorbereiten und mit einem Zertifikat abschliessen)" :"Zusatzausbildungen",
            "Interdisziplin\u00e4re Projekte und Studienreisen": "Interdisziplinäre Projekte und Studienreisen",
            "Allg. Didaktik und p\u00e4d. Psychologie": "Allg. Didaktik und päd. Psychologie",
            "Lern- und Arbeitsverhalten Sch\u00fcler/innen": "Lern- und Arbeitsverhalten Schüler/innen",
            "Techniques de l'apprentissage et du travail des \u00e9tudiant-e-s": "Lern- und Arbeitsverhalten Schüler/innen",
            "\u00c9coles de culture g\u00e9n\u00e9rale": "Fachmittelschulen",
            "Cours pour cadres": "Kaderkurse",
            "Congr\u00e8s, Journ\u00e9es, Forums": "Kongresse, Tagungen, Foren",
            "Course \u00e0 la carte": "Hol-Angebot"
        }

    def set_configuration(self, value):
        self._configuration = value

    @property
    def es_structure(self):
        return self.es


    def make_structure(self):
        self._courseName()
        self._key_words()
        self._key_coursetypes()
        self._key_language()
        self._description()
        self._status()
        self._localID()
        self._maxParticipants()
        self._minParticipants()
        self._course_methods()
        self._price()
        self._price_note()
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
        self._contacts()
        self._create_id()
        self._note()

        self._create_full_document()



    def _create_full_document(self):
        fullrecord = {}

        simplefields = ['beginDate', 'courseName', 'courseType', 'dates', 'description', 'endDate', 'goals'
                        'language', 'localID', 'methods', 'maxParticipants', 'minParticipants', 'note',
                        'organiser', 'place', 'price', 'priceNote', 'registrationDate', 'status', 'subtitle',
                        'targetAudience', 'provider']

        for fieldname in simplefields:
            if fieldname in self.es:
                fullrecord[fieldname] = self.es[fieldname]

        if "persons" in self.es:
            fullrecord["speakers"] = self.es["persons"]

        if "keywords" in self.es:
            fullrecord["keywords"] = self.es["keywords"]
            enriched_keywords = self._check_keywords_url(self.es["keywords"])
            if len(enriched_keywords) > 0:
                fullrecord["keywordsurl"] = enriched_keywords


        if "contacts" in self.course:
            contacts = self.course["contacts"]
            #contacts = self.course["contacts"] if "contacts" in self.course else []
            instructors = list(map(lambda rc: self._prepare_relevant_contact_fullrecord(rc),
                     filter(lambda contact: self._filter_leiter_contacts(contact), contacts)))
            if len(instructors) > 0:
                fullrecord["instructors"] =  instructors
            speakers = list(map(lambda rc: self._prepare_relevant_contact_fullrecord(rc),
                                                  filter(lambda contact: self._filter_referent_contacts(contact), contacts)))
            if len(speakers) > 0:
                fullrecord["speakers"] = speakers

        self.es["fulldocument"] = json.dumps(fullrecord)


    def _course_methods(self):
        if "course_methods" in self.course:

            #self.es["methods"] = self.course["course_methods"]
            self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                       self.course["course_methods"],
                                                                       "methods")
        #self.es["methods"] = self.course["methods"] if "methods" in self.course else []

    def _price_note(self):
        if "price_note" in self.course:
            #self.es["priceNote"] = self.course["price_note"]
            self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                       self.course["price_note"],
                                                                       "priceNote")
        #self.es["priceNote"] = self.course["priceNote"] if "priceNote" in self.course else "NA"

    def _create_id(self):
        #Todo: zem kafka producer should create id consisting of prefix (ZEM) + numeric id coming from zem

        #self.es["id"] = self._provider_Code + self.course["self"][self.course["self"].rfind("/") + 1:]
        self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                   self._provider_Code + self.course["self"]\
                                                                       [self.course["self"].rfind("/") + 1:],
                                                                   "id")

    def _contacts(self):
        if "contacts" in self.course:
            contacts = self.course["contacts"]
            #contacts = self.course["contacts"] if "contacts" in self.course else []
            zem_prepared_contacts = []
            if len(contacts) > 0:
                zem_prepared_contacts =  list(map(lambda rc: self._prepare_relevant_contact(rc),
                                                  filter(lambda contact: self._filterrelevantContacts(contact), contacts)))

            #self.es["persons"] = zem_prepared_contacts
            self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                       zem_prepared_contacts,
                                                                       "persons")


    def _prepare_relevant_contact(self,rc):
        contact = {}
        if "details" in rc:
            last_name = rc["details"]["last_name"] if "last_name" in rc["details"] else ""
            first_name = rc["details"]["first_name"] if "first_name" in rc["details"] else ""
            contact["name"] = last_name + ", " + first_name
            if "contact" in rc:
                contact["id"] = self._get_id_from_resource(rc["contact"])
            else:
                contact["id"] = hashlib.sha1(contact["name"].encode('utf-8')).hexdigest()

            if contact["id"] in self._get_ids_persons_to_enrich():
                contact["swissbiburl"] = list(map(lambda kw: kw[contact["id"]]["url"], filter(lambda dict: list(dict.keys())[0] == contact["id"],
                                                           self._configuration["Transformations"]["enrich_persons"])))[0]


            contact["role"] = rc["role"]
            if "companies" in rc["details"] and type(rc["details"]["companies"] is list):
                contact["companies"] =  list(map(lambda rel_company: self._prepare_company_of_relevant_contact(rel_company) ,
                                              filter(lambda company: self._filter_relevant_company(company),
                                                 rc["details"]["companies"])))
        return contact

    def _get_id_from_resource(self,resourcelink):
        return resourcelink[resourcelink.rfind("/") + 1:]

    def _get_ids_persons_to_enrich(self):

        return list(map(lambda ep: str(list( ep.keys())[0]), self._configuration["Transformations"]["enrich_persons"])) \
            if self._configuration is not None \
            and "Transformations" in self._configuration \
            and "enrich_persons" in self._configuration["Transformations"] else []



    def _prepare_relevant_contact_fullrecord(self, rc):
        contact = {}
        if "details" in rc:
            contact["firstName"] = rc["details"]["first_name"] if "first_name" in rc["details"] else "NA"
            contact["lastName"] = rc["details"]["last_name"] if "last_name" in rc["details"] else "NA"

            contact["phone"] = list(map(lambda number_object:number_object["number"],
                                        filter(lambda phone_number: self._filter_work_phone_number_email(phone_number),rc["details"]["phone_numbers"] ))) \
                if "details" in rc and "phone_numbers" in rc["details"] else []

            contact["email"] = list(map(lambda email_object:email_object["address"],
                                        filter(lambda email: self._filter_work_phone_number_email(email),rc["details"]["emails"] ))) \
                if "details" in rc and "emails" in rc["details"] else []

            #contact["birthdate"] = rc["details"]["birthdate"] if "birthdate" in rc["details"] else "NA"
            if "birthdate" in rc["details"]:
                contact["birthdate"] = rc["details"]["birthdate"]

            contactid = self._get_id_from_resource(rc["contact"])
            if  contactid in self._get_ids_persons_to_enrich():
                contact["swissbiburl"] = list(map(lambda kw: kw[contactid]["url"], filter(lambda dict: list(dict.keys())[0] == contactid,
                                                           self._configuration["Transformations"]["enrich_persons"])))[0]



            #projectid = rc["contact"] if "contact" in rc else hashlib.sha1(
            #    contact["name"].encode('utf-8')).hexdigest()
            #contact["id"] = projectid[projectid.rfind("/") + 1:]
            if "companies" in rc["details"] and type(rc["details"]["companies"] is list):
                contact["companies"] = list(
                    map(lambda rel_company: self._prepare_company_of_relevant_contact_fullrecord(rel_company),
                        filter(lambda company: self._filter_relevant_company(company),
                               rc["details"]["companies"])))

        return contact

    def _filter_work_phone_number_email(self,phone_number):
        return self.work_phone_number.search(phone_number["label"]) if "label" in phone_number \
                                                                       and "number" in phone_number  else False


    def _filterrelevantContacts(self, contact):
        return self.relevant_contact_role.search(contact["role"]) if "role" in contact else False

    def _filter_leiter_contacts(self, contact):
        return self.leiter_contact_role.search(contact["role"]) if "role" in contact else False

    def _filter_referent_contacts(self, contact):
        return self.referent_contact_role.search(contact["role"]) if "role" in contact else False


    def _filter_relevant_company(self, company):
        return  True if "default" in company and str(company["default"]).lower() == "true" else False



    def _prepare_company_of_relevant_contact(self, contact_zem):
        return {"name": contact_zem["details"]["name"]} if "details" in contact_zem and "name" in contact_zem["details"] else {"name": "company name NA"}

    def _prepare_company_of_relevant_contact_fullrecord(self, contact_zem):

        #"url" : list(map(lambda contact: contact["details"]["urls"], filter(lambda url: self._filter_companies_with_url(contact_zem["details"]["urls"]))))  if "details" in contact_zem and "urls" in contact_zem["details"] else []
        #if "details" in contact_zem and "urls" in contact_zem["details"]:
        #urls = list(map(lambda urlObject: urlObject["url"], filter(lambda o : "url" in o, contact_zem["details"]["urls"]) ))
        return  {

            "name": contact_zem["details"]["name"] if "details" in contact_zem and "name" in contact_zem["details"] else  None,
            "title" : contact_zem["details"]["title"] if "details" in contact_zem and "title" in contact_zem["details"] else None,
            "url" : list(map(lambda urlObject: urlObject["url"], filter(lambda o : "url" in o, contact_zem["details"]["urls"]) ))
        }


    def _provider(self):
        #self.es["provider"] = "ZEM" #always ZEM
        #self.es["provider"] = self._provider_Code
        self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                   self._provider_Code,
                                                                   "provider")

    def _courseName(self):
        if "name" in self.course:
            self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                       self.course['name'],
                                                                                   "courseName")


    def _key_words(self):

        if "keywords" in self.course:
            #self.es["keywords"] = self._filteredKeyWords(self.course["keywords"]) if "keywords" in self.course else []
            filtered_keywords = self._filteredKeyWords(self.course["keywords"])

            self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                       filtered_keywords,
                                                                       "keywords")
            if "keywords" in self.es:
                enriched_keywords = self._check_keywords_url(self.es["keywords"])
                self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                           enriched_keywords,
                                                                           "keywordsurl")


            #if len(enriched_keywords) > 0:
            #    self.es["keywordsurl"] = enriched_keywords

    def _check_keywords_url(self, keywords):
        enriched_keywords = []
        if self._configuration is not None and "Transformations" in self._configuration  \
            and "keywords" in self._configuration["Transformations"]:
            for dict in self._configuration["Transformations"]["keywords"]:
                key = list(dict.keys())[0]
                if key in keywords:
                    enriched_keywords.append(
                        {
                            "keyword" : key,
                            "url": list(map(lambda kw: kw[key]["url"],  filter(lambda dict: list(dict.keys())[0] == key,
                                          self._configuration["Transformations"]["keywords"])))[0]
                        }
                    )
        return enriched_keywords


    def _key_coursetypes(self):
        if "keywords" in self.course:
            #self.es["courseType"] = self._filteredCourseType(self.course["keywords"]) if "keywords" in self.course else []
            #self.es["courseType"] = self._filteredCourseType(self.course["keywords"])
            filtered_course_type = self._filteredCourseType(self.course["keywords"])
            self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                       filtered_course_type,
                                                                       "courseType")


    def _key_language(self):
        if "keywords" in self.course:
            #self.es["language"] = self._filteredLanguageType(self.course["keywords"]) if "keywords" in self.course else []
            #self.es["language"] = self._filteredLanguageType(self.course["keywords"])
            filtered_language_type = self._filteredLanguageType(self.course["keywords"])
            self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                       filtered_language_type,
                                                                       "language")



    def _description(self):
        if "details" in self.course:

            self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                       self.course['details'],
                                                                       'description')


    def _status(self):
        if "status" in self.course:

            self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                       self.course["status"],
                                                                       "status")

    def _localID(self):
        if "extra_fields" in self.course and "com.marketcircle.daylite/extra1" in self.course["extra_fields"] \
                        and "value" in self.course["extra_fields"]["com.marketcircle.daylite/extra1"]:
            #self.es["localID"] = self.course["extra_fields"]["com.marketcircle.daylite/extra1"]["value"] if \
            #    "extra_fields" in self.course and "com.marketcircle.daylite/extra1"  in self.course["extra_fields"] \
            #    and "value" in self.course["extra_fields"]["com.marketcircle.daylite/extra1"] \
            #    else "NA"
            #self.es["localID"] = self.course["extra_fields"]["com.marketcircle.daylite/extra1"]["value"]

            self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                       self.course["extra_fields"] \
                                                                           ["com.marketcircle.daylite/extra1"]["value"],
                                                                       "localID")


    def _maxParticipants(self):
        if "extra_fields" in self.course and "com.marketcircle.daylite/extra2" in self.course["extra_fields"] \
                        and "value" in self.course["extra_fields"]["com.marketcircle.daylite/extra2"]:
            #self.es["maxParticipants"] = self.course["extra_fields"]["com.marketcircle.daylite/extra2"]["value"]

            self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                       self.course["extra_fields"] \
                                                                           ["com.marketcircle.daylite/extra2"]["value"],
                                                                       "maxParticipants")

            #self.es["maxParticipants"] = self.course["extra_fields"]["com.marketcircle.daylite/extra2"]["value"] if \
            #    "extra_fields" in self.course and "com.marketcircle.daylite/extra2"  in self.course["extra_fields"] \
            #    and "value" in self.course["extra_fields"]["com.marketcircle.daylite/extra2"] \
            #    else "NA"


    def _minParticipants(self):

        if "extra_fields" in self.course and "com.marketcircle.daylite/extra3" in self.course["extra_fields"] \
                        and "value" in self.course["extra_fields"]["com.marketcircle.daylite/extra3"]:
            #self.es["minParticipants"] = self.course["extra_fields"]["com.marketcircle.daylite/extra3"]["value"]

            self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                       self.course["extra_fields"]\
                                                                           ["com.marketcircle.daylite/extra3"]["value"],
                                                                       "minParticipants")

            #self.es["minParticipants"] = self.course["extra_fields"]["com.marketcircle.daylite/extra3"]["value"] if \
            #    "extra_fields" in self.course and "com.marketcircle.daylite/extra3"  in self.course["extra_fields"] \
            #    and "value" in self.course["extra_fields"]["com.marketcircle.daylite/extra3"] \
            #    else "NA"

    def _price(self):
        if "extra_fields" in self.course and "com.marketcircle.daylite/extra5" in self.course["extra_fields"] \
                        and "value" in self.course["extra_fields"]["com.marketcircle.daylite/extra5"] and \
            not self.reg_number.match(self.course["extra_fields"]["com.marketcircle.daylite/extra5"]["value"]) is None:
            #self.es["price"] = self.course["extra_fields"]["com.marketcircle.daylite/extra5"]["value"]

            self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                       self.course["extra_fields"]\
                                                                           ["com.marketcircle.daylite/extra5"]["value"],
                                                                       "price")

            # self.es["price"] = self.course["extra_fields"]["com.marketcircle.daylite/extra5"]["value"] if \
            #     "extra_fields" in self.course and "com.marketcircle.daylite/extra5"  in self.course["extra_fields"] \
            #     and "value" in self.course["extra_fields"]["com.marketcircle.daylite/extra5"] \
            #     else "NA"


    def _place(self):
        if "extra_fields" in self.course and "com.marketcircle.daylite/extra6" in self.course["extra_fields"] \
                        and "value" in self.course["extra_fields"]["com.marketcircle.daylite/extra6"]:
            #self.es["place"] = self.course["extra_fields"]["com.marketcircle.daylite/extra6"]["value"]

            self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                       self.course["extra_fields"]\
                                                                           ["com.marketcircle.daylite/extra6"]["value"],
                                                                       "place")

            # self.es["place"] = self.course["extra_fields"]["com.marketcircle.daylite/extra6"]["value"] if \
            #     "extra_fields" in self.course and "com.marketcircle.daylite/extra6"  in self.course["extra_fields"] \
            #     and "value" in self.course["extra_fields"]["com.marketcircle.daylite/extra6"] \
            #     else "NA"

    def _note(self):
        #by now not used for zem only evento
        pass


    def _dates(self):
        #todo: hier Liste als default value??
        if "extra_fields" in self.course and "com.marketcircle.daylite/extra7" in self.course["extra_fields"] \
                        and "value" in self.course["extra_fields"]["com.marketcircle.daylite/extra7"]:
            #self.es["dates"] = self.course["extra_fields"]["com.marketcircle.daylite/extra7"]["value"]

            self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                       self.course["extra_fields"]\
                                                                           ["com.marketcircle.daylite/extra7"]["value"],
                                                                       "dates")

            # self.es["dates"] = self.course["extra_fields"]["com.marketcircle.daylite/extra7"]["value"] if \
            #     "extra_fields" in self.course and "com.marketcircle.daylite/extra7"  in self.course["extra_fields"] \
            #     and "value" in self.course["extra_fields"]["com.marketcircle.daylite/extra7"] \
            #     else "NA"

    def _subtitle(self):
        #todo: hier Liste als default value??
        if "extra_fields" in self.course and "com.marketcircle.daylite/extra9" in self.course["extra_fields"] \
                        and "value" in self.course["extra_fields"]["com.marketcircle.daylite/extra9"]:
            #self.es["subtitle"] = self.course["extra_fields"]["com.marketcircle.daylite/extra9"]["value"]

            self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                       self.course["extra_fields"]\
                                                                           ["com.marketcircle.daylite/extra9"]["value"],
                                                                       "subtitle")
            # self.es["subtitle"] = self.course["extra_fields"]["com.marketcircle.daylite/extra9"]["value"] if \
            #     "extra_fields" in self.course and "com.marketcircle.daylite/extra9"  in self.course["extra_fields"] \
            #     and "value" in self.course["extra_fields"]["com.marketcircle.daylite/extra9"] \
            #     else "NA"

    def _goals(self):
        if "extra_fields" in self.course and "com.marketcircle.daylite/extra11" in self.course["extra_fields"] \
                        and "value" in self.course["extra_fields"]["com.marketcircle.daylite/extra11"]:
            #self.es["goals"] = self.course["extra_fields"]["com.marketcircle.daylite/extra11"]["value"]

            self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                       self.course["extra_fields"]\
                                                                           ["com.marketcircle.daylite/extra11"]["value"],
                                                                       "goals")
            # self.es["goals"] = self.course["extra_fields"]["com.marketcircle.daylite/extra11"]["value"] if \
            #     "extra_fields" in self.course and "com.marketcircle.daylite/extra11"  in self.course["extra_fields"] \
            #     and "value" in self.course["extra_fields"]["com.marketcircle.daylite/extra11"] \
            #     else "NA"

    def _targetAudience(self):
        if "extra_fields" in self.course and "com.marketcircle.daylite/extra12" in self.course["extra_fields"] \
                        and "value" in self.course["extra_fields"]["com.marketcircle.daylite/extra12"]:
            #self.es["targetAudience"] = self.course["extra_fields"]["com.marketcircle.daylite/extra12"]["value"]

            self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                       self.course["extra_fields"]\
                                                                           ["com.marketcircle.daylite/extra12"]["value"],
                                                                       "targetAudience")
            # self.es["targetAudience"] = self.course["extra_fields"]["com.marketcircle.daylite/extra12"]["value"] if \
            #     "extra_fields" in self.course and "com.marketcircle.daylite/extra12"  in self.course["extra_fields"] \
            #     and "value" in self.course["extra_fields"]["com.marketcircle.daylite/extra12"] \
            #     else "NA"

    def _beginDate(self):

        if self._check_holangebot() is False and "extra_fields" in self.course and \
                "com.marketcircle.daylite/extra_date_1"  in self.course["extra_fields"] \
                and "value" in self.course["extra_fields"]["com.marketcircle.daylite/extra_date_1"]:
            #self.es["beginDate"] = self.course["extra_fields"]["com.marketcircle.daylite/extra_date_1"]["value"]

            self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                       self.course["extra_fields"]\
                                                                           ["com.marketcircle.daylite/extra_date_1"]["value"],
                                                                       "beginDate")
            # self.es["beginDate"] = self.course["extra_fields"]["com.marketcircle.daylite/extra_date_1"]["value"] if \
            #     "extra_fields" in self.course and "com.marketcircle.daylite/extra_date_1"  in self.course["extra_fields"] \
            #     and "value" in self.course["extra_fields"]["com.marketcircle.daylite/extra_date_1"] \
            #     else "NA"

    def _endDate(self):

        if self._check_holangebot() is False and "extra_fields" in self.course and \
                "com.marketcircle.daylite/extra_date_2"  in self.course["extra_fields"] \
                and "value" in self.course["extra_fields"]["com.marketcircle.daylite/extra_date_2"]:
            #self.es["endDate"] = self.course["extra_fields"]["com.marketcircle.daylite/extra_date_2"]["value"]
            self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                       self.course["extra_fields"]\
                                                                           ["com.marketcircle.daylite/extra_date_2"]\
                                                                           ["value"],
                                                                       "endDate")
            # self.es["endDate"] = self.course["extra_fields"]["com.marketcircle.daylite/extra_date_2"]["value"] if \
            #     "extra_fields" in self.course and "com.marketcircle.daylite/extra_date_2"  in self.course["extra_fields"] \
            #     and "value" in self.course["extra_fields"]["com.marketcircle.daylite/extra_date_2"] \
            #     else "NA"

    def _registrationDate(self):

        if self._check_holangebot() is False and "extra_fields" in self.course and "com.marketcircle.daylite/extra_date_3"  in self.course["extra_fields"] \
                and "value" in self.course["extra_fields"]["com.marketcircle.daylite/extra_date_3"]:
            #self.es["registrationDate"] = self.course["extra_fields"]["com.marketcircle.daylite/extra_date_3"]["value"]

            self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                       self.course["extra_fields"]\
                                                                           ["com.marketcircle.daylite/extra_date_3"]["value"],
                                                                       "registrationDate")
            # self.es["registrationDate"] = self.course["extra_fields"]["com.marketcircle.daylite/extra_date_3"]["value"] if \
            #     "extra_fields" in self.course and "com.marketcircle.daylite/extra_date_3"  in self.course["extra_fields"] \
            #     and "value" in self.course["extra_fields"]["com.marketcircle.daylite/extra_date_3"] \
            #     else "NA"


    def _organiser(self):
        if "companies" in self.course and "details" in self.course["companies"] \
                        and "name" in self.course["companies"]["details"]:
            #self.es["organiser"] = {'name': self.course["companies"]["details"]["name"]}

            self.edu_utilities.add_data_to_search_doc_prepared_content(self.es,
                                                                       {'name': self.course["companies"]["details"]["name"]},
                                                                       "organiser")

            # self.es["organiser"] = {'name': self.course["companies"]["details"]["name"]} if \
            #     "companies" in self.course and "details"  in self.course["companies"] \
            #     and "name" in self.course["cmpanies"]["details"] \
            #     else {}

    def _filteredKeyWords(self, rawKeywordList):
        return list(map(lambda short_word : self._map_keywords_to_norm(short_word),
                        map(lambda fw: fw[3:],  filter(lambda v : self.first_2_digits_keywords.search(v),rawKeywordList))))

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
            'Italiano': 'ita'
        }

        test = language_codes[language_value] if language_value in language_codes else language_value
        return test

    def _map_keywords_to_norm(self,keyword):

        return self.keywords_codes[keyword] if keyword in self.keywords_codes else keyword

    def _not_hol_angebote(self, rawKeywordList):
        return list(map(lambda lang:self._map_language(lang),
                        map(lambda fw: fw[3:],  filter(lambda v : self.first_2_digits_language.search(v),rawKeywordList))))

    def _get_keywords(self):
        return self.course["keywords"] if "keywords" in self.course else []

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

    @property
    def _provider_Code(self):
        return self._configuration['EDU']['code_data_provider'] \
            if 'EDU' in self._configuration \
               and 'code_data_provider' in self._configuration['EDU'] else 'DefaultProvider'