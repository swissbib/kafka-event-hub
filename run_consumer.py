
import argparse
from kafka_event_hub.consumers.cbs.cbs_consumer import CBSConsumer



parser = argparse.ArgumentParser(description='runner for content collector consumer')
parser.add_argument('--type', action='store')
parser.add_argument('config', action='store')


args = parser.parse_args()

consumer = globals()[args.type](args.config)

consumer.initialize()
consumer.lookUpData()
consumer.preProcessData()
consumer.process()
consumer.postProcessData()
