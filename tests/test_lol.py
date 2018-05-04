from datetime import datetime, timedelta
import os

from bitoduc import (
    Pipeline,
    FileSource,
    FileSink,
)


def test_lol():
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
