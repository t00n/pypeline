from datetime import timedelta

from bitoduc import (
    Pipeline,
    Source,
    Sink,
    Function,
    # Map,
    Window,
    # GroupBy,
    # Resample,
)


class FileSource(Source):
    def __init__(self, filename):
        super().__init__()
        self.filename = filename

    def read(self):
        with open(self.filename) as f:
            yield from f.readlines()


class CSVSource(FileSource):
    def read(self):
        for line in super().read():
            if line[-1] == '\n':
                line = line[:-1]
            yield line.split(';')


class PrintSink(Sink):
    def write(self, row):
        print("PRINT", row)


class PrintLolSink(Sink):
    def write(self, row):
        print("LOL", row)


class PrintYoloSink(Sink):
    def write(self, row):
        print("YOLO", row)


def split(l):
    for e in l:
        yield e


with Pipeline() as p:
    csv = p | CSVSource('test.csv')
    # csv | Window(5) | PrintSink()
    # csv | Window(5, fixed=False) | PrintSink()
    # csv | Function(split) | PrintLolSink()
    # csv | Window(timedelta(seconds=4), key=lambda x: x[0]) | PrintYoloSink()
    csv | Window(timedelta(seconds=4,), fixed=False, key=lambda x: x[0]) | PrintYoloSink()
