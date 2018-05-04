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
)


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


def test_iterable_list():
    data = [
        "first line",
        "second line",
        "third line",
    ]

    result = []

    with Pipeline() as p:
        p | IterableSource(data) | ListSink(result)

    assert(data == result)


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