from datetime import datetime

from dateutil.parser import parse as date_parse


def to_datetime(d):
    if isinstance(d, str):
        return date_parse(d)
    elif isinstance(d, datetime):
        return d
    else:
        raise ValueError("{} should be a datetime".format(d))
