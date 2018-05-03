from bitoduc import (
    Pipeline,
    Source,
    Sink,
    Function,
    Map,
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
        print(row)


class PrintLolSink(Sink):
    def write(self, row):
        print("LOL", row)


def split(l):
    for e in l:
        yield e


with Pipeline() as p:
    csv = p | CSVSource('test.csv')
    csv | Window(5) | PrintSink()
    csv | Function(split) | PrintLolSink()
