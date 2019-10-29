
from kafka_event_hub.config import EduConfig
import json

class EduplatformUtilities:

    def __init__(self, configuration: type(EduConfig)):
        self.configuration = configuration
        self._initialize()

    def _initialize(self):
        path_field_types = self.configuration["ES"]["field_type_description"] if 'ES' in self.configuration \
                and 'field_type_description' in self.configuration['ES'] else \
                '/basedir/configs/eduplatform/indexfieldtypes.json'

        with open(path_field_types, 'r') as content_file:
            self.field_type_description = json.loads(content_file.read())


    @property
    def field_types_desc(self):
        return self.field_type_description



    def add_data_to_search_doc_prepared_content(self,
                                           search_doc,
                                           content,
                                           search_doc_fieldname: str):


        def content_type():
            lf = search_doc_fieldname.lower()
            return self.field_type_description[lf] if lf in self.field_type_description.keys() else "string"


        def check_content(source_content):
            ct = content_type()
            if ct == "list" and isinstance(source_content, list) and len(source_content) > 0:
                search_doc[search_doc_fieldname] = source_content
            elif ct == "string" and isinstance(source_content, list) and len(source_content) > 0:
                search_doc[search_doc_fieldname] = " ".join(source_content)
            elif ct == "string" and isinstance(source_content, str) and source_content != "":
                search_doc[search_doc_fieldname] = source_content
            elif ct == "dict" and isinstance(source_content, dict):
                search_doc[search_doc_fieldname] = source_content
            elif ct == "list" and isinstance(source_content, str):
                search_doc[search_doc_fieldname] =  [source_content]
            else:
                #no empty lists
                if not isinstance(source_content, list):
                    #print("make logging - field isn't created because data is not sufficient - correct?")
                    search_doc[search_doc_fieldname] = str(source_content)



        if content is not None:
            check_content(content)

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
            'Italiano': 'ita',
            '1': 'ger'
        }

        return language_codes[language_value] if language_value in language_codes else language_value




