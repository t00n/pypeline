from pypeline import (
    Pipeline,
    CSVSource,
    FileSink,
)


with Pipeline() as p:
    csv = p | CSVSource('test.csv') | FileSink('test_res.txt')
    # csv | Window(5) | PrintSink()
    # csv | Window(5, fixed=False) | PrintSink()
    # csv | Function(split) | PrintLolSink()
    # csv | Window(timedelta(seconds=4), key=lambda x: x[0]) | PrintYoloSink()
    # csv | Window(timedelta(seconds=4,), fixed=False, key=lambda x: x[0]) | PrintYoloSink()
