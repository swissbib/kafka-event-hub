
import argparse
from kafka_event_hub.producers.oai.oai_producer import OAIProducer



parser = argparse.ArgumentParser(description='runner for content collector producer')
parser.add_argument('--type', action='store')
parser.add_argument('config', action='store')


args = parser.parse_args()

producer = globals()[args.type](args.config)

producer.initialize()
producer.lookUpData()
producer.preProcessData()
producer.process()
producer.postProcessData()
