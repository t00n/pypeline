from bitoduc import Source


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