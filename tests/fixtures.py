from datetime import datetime, timedelta

import pytest


@pytest.fixture
def data_list():
    return list(range(15))


@pytest.fixture
def data_timed():
    data = []
    start = datetime(2018, 5, 4, 15, 45)
    for i in range(15):
        d = start + timedelta(seconds=i)
        data.append({'time': d, 'value': i})
    return data
