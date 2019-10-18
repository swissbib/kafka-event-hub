from kafka_event_hub.config import ConsumerConfig
from kafka_event_hub.consumers.base_consumer import AbstractBaseConsumer
import argparse
import json
from elasticsearch import Elasticsearch, exceptions
from kafka_event_hub.consumers.eduplatform.zem_es_transformation import ZemESTransformation

from kafka_event_hub.consumers.eduplatform.evento_es_transformation import EventoESTransformation




class EventoConsumer(AbstractBaseConsumer):

    """
        consumes the messages in cc-zem (json structure based on data of dalite repository)
        and should create on the fly the elasticsearch doc for the zem index
        aim: make it rather simple and usable for prototyping the zem frontend
    """

    def createDoc(self, message):
        course = json.loads(message)

        transformations = EventoESTransformation(course)
        transformations.set_configuration(self.configuration.configuration)
        transformations.make_structure()
        result = transformations.es_structure

        return result


    def __init__(self, config_path: str, configrepshare: str = None, **kwargs):
        super().__init__(config_path, ConsumerConfig, **kwargs)
        self._initialize()


    def _initialize(self):
        if self.configuration["ES"]["active"]:
            self.es = Elasticsearch((self.configuration["ES"]["hosts"]).split("#"),
                                    index=self.configuration["ES"]["index"])
            self.indexClient = self.es.indices
            self.dI = index = self.configuration["ES"]["index"]

        self.all_docs = []

    def _index_doc(self,key, message):
        if self.configuration["ES"]["active"]:
            doc = self.createDoc(message)
            #bug im update https://github.com/elastic/elasticsearch/issues/41625
            #response = self.es.update(index="zem",id=key,body=doc) if self.es.exists(index="zem",id=key) else self.es.create(index="zem",id=key,body=doc)
            if not self.es.exists(index=self.dI, id=doc["id"]):
                response = self.es.create(index=self.dI, id=doc["id"], body=doc)

    def _append_doc_for_dump(self,key, message):
        self.all_docs.append(json.loads(message))



    def process(self):

        #test = self.indexClient.get_mapping(index=self.dI)
        message = next(self._consumer,None)

        while (message is not None):
            value = message.value.decode('utf-8')
            key = message.key.decode('utf-8')
            self._index_doc(key, value)
            #self._append_doc_for_dump(key,value)
            message = next(self._consumer,None)

        #ad = open("all_docs.json","w")
        #json.dump(self.all_docs,ad,indent=20)
        #ad.flush()
        #ad.close()

        #self._time_logger.info('Received message: {} with key {}'
        #                       .format(message.value.decode('utf-8'), message.key.decode('utf-8')))
        #return message.key.decode('utf-8'), message.value.decode('utf-8')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Path to configuration file', type=str, default='configs/eduplatform/evento.yaml')
    parser.parse_args()
    args = parser.parse_args()
    config = config_path = args.config

    eventoConsumer = EventoConsumer(config_path=config)
    eventoConsumer.process()
