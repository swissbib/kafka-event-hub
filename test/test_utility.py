import re

from kafka_event_hub.utility.producer_utility import current_timestamp


class TestUtility(object):

    def setup_class(self):
        self.regex = re.compile('\d{4}-\d{2}-\d{2}T[0-2][0-9]:[0-6][0-9]:[0-6][0-9]')

    def test_current_time(self):
        assert self.regex.match(current_timestamp())
