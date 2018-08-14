

class TestUtility(object):

    def setup_class(self):
        import re
        self.regex = re.compile('\d{4}-\d{2}-\d{2}T[0-2][0-9]:[0-6][0-9]:[0-6][0-9]')

    def test_current_time(self):
        import os
        import sys
        sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        from kafka_event_hub.utility.producer_utility import current_timestamp

        assert self.regex.match(current_timestamp())
