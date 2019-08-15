
import argparse
from kafka_event_hub.producers import OAIProducerKafka, FilePushNebisKafka, WebDavReroKafka, EduZemKafka, EduEventoKafka
from kafka_event_hub.consumers import ZemConsumer, EventoConsumer



parser = argparse.ArgumentParser(description='runner for content collector producer')
parser.add_argument('-t','--type', action='store')
parser.add_argument('-s', '--sharedconfig', action='store', required=False, default=None)
parser.add_argument('configrep', action='store')

args = parser.parse_args()
producer = globals()[args.type](args.configrep, args.sharedconfig)

producer.process()
