import os

from pypeline import Sink


class FileSink(Sink):
    def __init__(self, filename):
        self.filename = filename
        try:
            os.unlink(self.filename)
        except:
            pass

    def write(self, row):
        with open(self.filename, 'a') as f:
            f.write(str(row))


class ListSink(Sink):
    def __init__(self, l):
        self.l = l

    def write(self, row):
        self.l.append(row)


class DummySink(Sink):
    def write(self, row):
        pass
