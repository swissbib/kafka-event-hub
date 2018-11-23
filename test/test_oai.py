import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sickle import Sickle
from sickle.iterator import OAIItemIterator, OAIResponseIterator
from sickle.models import Identify


class TestOAI(object):

    def setup_class(self):
        self.oai = Sickle('http://aleph.ag.ch/OAI', iterator=OAIResponseIterator)
        self.id: Identify = self.oai.Identify()
        self.timestamp = self.id.earliestDatestamp
        print(self.id.granularity)

    def test_other_stuff(self):
        self.oai.iterator = OAIItemIterator
        records = self.oai.ListRecords(**{
            'metadataPrefix': 'marc21',
            'set': 'SWISSBIB-FULL-OAI',
            'from': self.timestamp
        })
        for r in records:
            with open('output.txt', 'a') as file:
                file.write(r.raw + '\n')

