
import argparse
from kafka_event_hub.consumers.cbs.cbs_consumer import CBSConsumer



parser = argparse.ArgumentParser(description='runner for content collector consumer')
parser.add_argument('-t','--type',type=str, action='store')
parser.add_argument('-s','--sharedconfig',type=str, action='store', required=False, default=None)

parser.add_argument('config', action='store')


args = parser.parse_args()

consumer = globals()[args.type](args.config)

consumer.initialize()
consumer.lookUpData()
consumer.preProcessData()
consumer.process()
consumer.postProcessData()
