from datetime import datetime, timedelta
import random
from copy import deepcopy

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


@pytest.fixture
def data_timed_holes():
    random.seed(42)
    data = []
    start = datetime(2018, 5, 4, 15, 45)
    i = 0
    while True:
        if i > 30:
            break
        d = start + timedelta(seconds=i)
        data.append({'time': d, 'value': i})
        if random.random() > 0.5:
            i += random.randint(1, 5)
        else:
            i += 1
    return data


@pytest.fixture
def data_timed_holes_grouped(data_timed_holes):
    data = deepcopy(data_timed_holes)
    random.seed(42)
    for row in data:
        row['group'] = random.randint(0, 2)

    return data
