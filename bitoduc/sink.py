import os

from bitoduc import Sink


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
