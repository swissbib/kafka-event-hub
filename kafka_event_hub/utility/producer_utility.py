import re
from datetime import datetime


detailed_granularity_pattern = re.compile('Thh:mm:ssZ', re.UNICODE | re.DOTALL | re.IGNORECASE)


def current_utc_timestamp(granularity: str = None):
    if granularity is not None and detailed_granularity_pattern.search(granularity):
        current_time_utc = datetime.utcnow()
        return '{}T{:0>2}:{:0>2}:{:0>2}Z'.format(current_time_utc.date(), current_time_utc.hour, current_time_utc.minute, current_time_utc.second)
    else:
        # granularity without time => next from should be the current day (think about it again especially with UTC)
        return datetime.utcnow().strftime("%Y-%m-%d")


def current_timestamp():
    current_time = datetime.now()
    return '{}T{:0>2}:{:0>2}:{:0>2}Z'.format(current_time.date(), current_time.hour, current_time.minute, current_time.second)


def transform_from_until(value, granularity ='YYYY-MM-DDThh:mm:ssZ'):
    if detailed_granularity_pattern.search(granularity):
        if isinstance(value, str):
            return '{:%Y-%m-%dT%H:%M:%SZ}'.format(
                datetime.strptime(value, '%Y-%m-%dT%H:%M:%SZ'))
        else:
            return '{:%Y-%m-%dT%H:%M:%SZ}'.format(
                value, '%Y-%m-%dT%H:%M:%SZ')
    else:
        if isinstance(value, str):
            return '{:%Y-%m-%d}'.format(
                datetime.strptime(value, '%Y-%m-%d'))
        else:
            return '{:%Y-%m-%d}'.format(
                value, '%Y-%m-%d')
