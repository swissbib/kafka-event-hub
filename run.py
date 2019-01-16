
import argparse
from kafka_event_hub.producers.oai.oai_producer import OAIProducer



parser = argparse.ArgumentParser(description='runner for content collector producer')
parser.add_argument('-t','--type', action='store')
parser.add_argument('-s', '--sharedconfig', action='store', required=False, default=None)
parser.add_argument('configrep', action='store')

args = parser.parse_args()
producer = globals()[args.type](args.configrep, args.sharedconfig)

#producer.initialize()
#producer.lookUpData()
#producer.preProcessData()
producer.process()
#producer.postProcessData()
