from kafka_event_hub.utility import current_timestamp, \
    current_utc_timestamp, \
    transform_from_until, \
    detailed_granularity_pattern

import re


class TestUtility(object):

    def setup_class(self):
        pass

    def test_current_time(self):
        assert re.match('\d{4}-\d{2}-\d{2}T[0-2][0-9]:[0-6][0-9]:[0-6][0-9]', current_timestamp())
