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


def test_fixed_window_timedelta(data_timed):
    result = []
    with Pipeline() as p:
        p | IterableSource(data_timed) | Window(timedelta(seconds=6), key='time') | ListSink(result)

    assert(result == [data_timed[:6], data_timed[6:12]])


def test_sliding_window_int(data_list):
    result = []
    with Pipeline() as p:
        p | IterableSource(data_list) | Window(4, skip=1) | ListSink(result)

    should_be = [data_list[i:i+4] for i in range(12)]

    assert(result == should_be)


def test_sliding_window_timedelta(data_timed):
    result = []
    with Pipeline() as p:
        p | IterableSource(data_timed) | Window(timedelta(seconds=6), skip=1, key='time') | ListSink(result)

    should_be = [data_timed[i:i+6] for i in range(10)]

    assert(result == should_be)


def test_sliding_window_int_skip(data_list):
    result = []
    with Pipeline() as p:
        p | IterableSource(data_list) | Window(4, skip=3) | ListSink(result)

    should_be = [data_list[i:i+4] for i in range(0, 12, 3)]

    print(result)

    assert(result == should_be)