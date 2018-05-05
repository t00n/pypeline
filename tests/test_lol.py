from datetime import datetime, timedelta
import os

from pypeline import (
    Pipeline,
    FileSource,
    FileSink,
    IterableSource,
    ListSink,
    DummySource,
    DummySink,
    Window,
    GroupBy,
    Flatten,
)

from .fixtures import *


def test_files():
    try:
        os.unlink("test_lol.csv")
    except FileNotFoundError:
        pass
    content = ""
    with open("test_lol.csv", "w") as f:
        d = datetime(2018, 5, 4, 2, 51)
        for i in range(15):
            to_write = "{}:{}\n".format(d, i)
            f.write(to_write)
            d += timedelta(seconds=1)
            content += to_write

    with Pipeline() as p:
        p | FileSource('test_lol.csv') | FileSink('test_res.csv')

    with open("test_res.csv") as f:
        assert(f.read() == content)

    os.unlink("test_lol.csv")
    os.unlink("test_res.csv")


def test_iterable_list(data_list):
    result = []

    with Pipeline() as p:
        p | IterableSource(data_list) | ListSink(result)

    assert(data_list == result)


def test_matmul():
    with Pipeline() as p:
        source = p | DummySource() @ "dummy_source"
        sink = source | DummySink() @ "dummy_sink"

    assert(source.name == "dummy_source")
    assert(sink.name == "dummy_sink")


def test_matmul2():
    with Pipeline() as p:
        sink = p | DummySource() @ "dummy_source" \
                 | DummySink() @ "dummy_sink"

    assert(isinstance(sink, DummySink))
    assert(sink.name == "dummy_sink")


def test_fixed_window_int(data_list):
    result = []
    with Pipeline() as p:
        p | IterableSource(data_list) | Window(4) | ListSink(result)

    assert(result == [data_list[:4], data_list[4:8], data_list[8:12]])


def test_fixed_window_timedelta(data_timed_holes):
    result = []
    with Pipeline() as p:
        p | IterableSource(data_timed_holes) | Window(timedelta(seconds=6), key='time') | ListSink(result)

    result_int = [[x['value'] for x in ar] for ar in result]

    assert(result_int == [
        [0, 1, 3, 4],
        [9, 10, 11],
        [12, 13, 14],
        [19, 20, 21, 22],
        [24, 27, 28, 29],
    ])


def test_sliding_window_int(data_list):
    result = []
    with Pipeline() as p:
        p | IterableSource(data_list) | Window(4, skip=1) | ListSink(result)

    should_be = [data_list[i:i+4] for i in range(12)]

    assert(result == should_be)


def test_sliding_window_timedelta(data_timed_holes):
    result = []
    with Pipeline() as p:
        p | IterableSource(data_timed_holes) | Window(timedelta(seconds=6), skip=1, key='time') | ListSink(result)

    result_int = [[x['value'] for x in ar] for ar in result]

    assert(result_int == [
        [0, 1, 3, 4],
        [1, 3, 4],
        [3, 4],
        [3, 4],
        [4, 9],
        [9, 10],
        [9, 10, 11],
        [9, 10, 11, 12],
        [9, 10, 11, 12, 13],
        [9, 10, 11, 12, 13, 14],
        [10, 11, 12, 13, 14],
        [11, 12, 13, 14],
        [12, 13, 14],
        [13, 14],
        [14, 19],
        [19, 20],
        [19, 20, 21],
        [19, 20, 21, 22],
        [19, 20, 21, 22],
        [19, 20, 21, 22, 24],
        [20, 21, 22, 24],
        [21, 22, 24],
        [22, 24, 27],
        [24, 27, 28],
        [24, 27, 28, 29]
    ])


def test_sliding_window_int_skip(data_list):
    result = []
    with Pipeline() as p:
        p | IterableSource(data_list) | Window(4, skip=3) | ListSink(result)

    should_be = [data_list[i:i+4] for i in range(0, 12, 3)]

    assert(result == should_be)


def test_sliding_window_timedelta_skip(data_timed_holes):
    result = []
    with Pipeline() as p:
        p | IterableSource(data_timed_holes) | Window(timedelta(seconds=6), skip=3, key='time') | ListSink(result)

    result_int = [[x['value'] for x in ar] for ar in result]

    assert(result_int == [
        [0, 1, 3, 4],
        [3, 4],
        [9, 10, 11],
        [9, 10, 11, 12, 13, 14],
        [12, 13, 14],
        [19, 20],
        [19, 20, 21, 22],
        [21, 22, 24],
        [24, 27, 28, 29],
    ])


def test_window_list(data_list):
    result = []

    with Pipeline() as p:
        p | IterableSource(data_list) \
          | Window(5) \
          | Window(2) \
          | ListSink(result)

    assert(result == [[0, 1], [2, 3], [4, 5], [6, 7], [8, 9], [10, 11], [12, 13]])


def test_flatten(data_list):
    result = []

    with Pipeline() as p:
        p | IterableSource(data_list) \
          | Window(5) \
          | Flatten() \
          | ListSink(result)

    assert(result == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14])


def test_groupby(data_timed_holes_grouped):
    result = []

    with Pipeline() as p:
        p | IterableSource(data_timed_holes_grouped) \
          | Window(8) | GroupBy('group') \
          | ListSink(result)

    result_int = [[(x['group'], x['value']) for x in ar] for ar in result]

    assert(result_int == [
        [(2, 0), (2, 4)],
        [(0, 1), (0, 3), (0, 10), (0, 11), (0, 12)],
        [(1, 9)],
        [(2, 13), (2, 19), (2, 20), (2, 21), (2, 24)],
        [(0, 14), (0, 22)],
        [(1, 27)]
    ])


def test_groupby_multiple(data_timed_holes_grouped_multiple):
    result = []

    with Pipeline() as p:
        p | IterableSource(data_timed_holes_grouped_multiple) \
          | Window(8) | GroupBy('group') | GroupBy('group2') \
          | ListSink(result)

    result_int = [[(x['group'], x['group2'], x['value']) for x in ar1] for ar1 in result]

    assert(result_int == [
        [(2, 0, 0), (2, 0, 4)],
        [(0, 0, 1), (0, 0, 10), (0, 0, 11), (0, 0, 12)],
        [(0, 1, 3)],
        [(1, 0, 9)],
        [(2, 1, 13)],
        [(2, 0, 19), (2, 0, 20), (2, 0, 21), (2, 0, 24)],
        [(0, 0, 14), (0, 0, 22)],
        [(1, 0, 27)]
    ])
