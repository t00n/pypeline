from abc import ABC, abstractmethod
from datetime import timedelta
from collections import defaultdict
from multiprocessing import Process
import uuid

from .pipe import Pipe
from .utils import to_datetime


class Component:
    def __init__(self):
        self.outbound_pipes = []
        self.name = uuid.uuid4()

    def __or__(self, other):
        """ Add a child to this component """
        if not isinstance(other, Component):
            raise ValueError("{} should be a Component".format(other))
        pipe = Pipe(self.name)
        other.set_inbound_pipe(pipe)
        self.outbound_pipes.append(pipe)
        self.pipeline.add_component(other)
        return other

    def propagate(self, row):
        """ Apply operation on row and propagate results to children """
        res = self.apply(row)
        if res is not None:
            for r in res:
                for pipe in self.outbound_pipes:
                    pipe.send(r)

    def apply(self, row):
        """ Operation of this component """
        yield row

    def __matmul__(self, name):
        """ Set the name of this component """
        self.name = name
        return self

    def set_pipeline(self, pipeline):
        self.pipeline = pipeline

    def set_inbound_pipe(self, pipe):
        self.inbound_pipe = pipe

    def should_halt(self):
        return self.inbound_pipe.is_closed()

    def halt_children(self):
        """ Publish number of propagated rows so that children know when to stop """
        for pipe in self.outbound_pipes:
            pipe.close()

    def run(self):
        """ Retrieve next row and propagate it """
        while not self.should_halt():
            row = self.inbound_pipe.recv()
            if row is not None:
                self.propagate(row)
        self.halt_children()


class Function(Component):
    def __init__(self, f):
        super().__init__()
        if not callable(f):
            raise TypeError("{} should be callable".format(f))
        self.f = f

    def apply(self, row):
        yield from self.f(row)


class Map(Function):
    def apply(self, row):
        yield self.f(row)


class Filter(Function):
    def apply(self, row):
        if self.f(row):
            yield row


class Source(Component, ABC):
    @abstractmethod
    def read(self):
        pass

    def run(self):
        for row in self.read():
            self.propagate(row)
        self.halt_children()


class Sink(Component, ABC):
    def __or__(self, other):
        raise ValueError("A Sink is always the last element")

    def propagate(self, row):
        self.write(row)

    @abstractmethod
    def write(self, row):
        pass


class Key:
    def __init__(self, key):
        self.key = key
        if not (isinstance(key, str) or callable(key)):
            raise ValueError("Key should be a str or a callable")

    def get_value(self, row):
        if isinstance(self.key, str):
            try:
                return row[self.key]
            except:
                return getattr(row, self.key)
        elif callable(self.key):
            return self.key(row)


class Window(Component):
    def __init__(self, window, *, key=None, skip=None):
        super().__init__()
        if isinstance(window, int):
            self.window = window
        elif isinstance(window, timedelta):
            self.window = window
            if key is None:
                raise ValueError("Should provide a key when using a time-based window")
            self.key = Key(key)
        else:
            raise ValueError("Window should be an integer or a timedelta")
        self.memory = []
        if not (isinstance(skip, int) or skip is None):
            raise ValueError("Skip parameter should be None (for fixed windows) or an integer (for sliding windows)")
        self.skip = skip
        # if this is a count-based sliding window, we keep a counter of rows to skip.
        # once the counter is <= 0, we must yield if the window is full
        # we set the counter to 0 here so that the first window is always yielded
        self.skip_counter = 0
        self.first_time = True

    def apply_row(self, row):
        self.memory.append(row)
        # if this is a time-based sliding window, we keep the first row timestamp
        # as the watermark to keep a track of rows to skip
        if self.first_time and isinstance(self.window, timedelta):
            start_ts = to_datetime(self.key.get_value(row)).timestamp()
            window_ts = self.window.total_seconds()
            start_ts //= window_ts
            start_ts *= window_ts
            self.watermark = to_datetime(start_ts).replace(tzinfo=self.key.get_value(row).tzinfo)
            self.first_time = False
        # if this is a count-based window, we simply keep `self.window` rows
        # we yield when memory is full
        # if this is a sliding window, we decrement the skip counter on each row
        # and we yield only when counter <= 0
        if isinstance(self.window, int):
            self.memory = self.memory[-self.window:]
            if len(self.memory) == self.window:
                must_yield = False
                if self.skip is None:
                    must_yield = True
                else:
                    if self.skip_counter == 0:
                        must_yield = True
                        self.skip_counter = self.skip
                    self.skip_counter -= 1
                if must_yield:
                    yield self.memory
                    if self.skip is None:
                        self.memory = []
        # if this is a time-based window, we have a watermark specifying the beginning of the current window
        # we yield when the duration of the window >= self.window
        # if fixed, we add self.window to the watermark when we yield
        # if sliding we add self.skip to the watermark when we yield
        # we keep only rows after the watermark
        elif isinstance(self.window, timedelta):
            now = to_datetime(self.key.get_value(row))
            # use while because if holes between rows are large, we might trigger several windows
            # when we receive only one row
            while now - self.watermark >= self.window:
                high_watermark = self.watermark + self.window
                # retrieve all rows before the end of the current window
                to_yield = []
                for elem in self.memory:
                    t = to_datetime(self.key.get_value(elem))
                    if t < high_watermark:
                        to_yield.append(elem)
                yield to_yield
                # adjust watermark depending on window type (fixed/sliding)
                if self.skip is None:
                    self.watermark += self.window
                else:
                    self.watermark += timedelta(seconds=self.skip)
                # keep only rows after the beginning of the next window
                remaining = []
                for elem in self.memory:
                    t = to_datetime(self.key.get_value(elem))
                    if t >= self.watermark:
                        remaining.append(elem)
                self.memory = remaining

    def apply(self, data):
        if isinstance(data, list):
            for row in data:
                yield from self.apply_row(row)
        else:
            yield from self.apply_row(data)


class GroupBy(Component):
    def __init__(self, key):
        super().__init__()
        self.key = Key(key)

    def apply(self, rows):
        if not isinstance(rows, list):
            raise ValueError("GroupBy must receive a list")
        grouped = defaultdict(lambda: [])
        for row in rows:
            grouped[self.key.get_value(row)].append(row)
        yield from grouped.values()


class Flatten(Component):
    def apply(self, rows):
        if isinstance(rows, list):
            yield from rows
        else:
            raise ValueError("Flatten must receive a list")


def run_component(component):
    component.run()


class Pipeline:
    def __init__(self, block=True):
        self.block = block
        self.components = []
        self.processes = []
        self.name = uuid.uuid4()

    def __or__(self, source):
        if not isinstance(source, Source):
            raise ValueError("First element must be a Source")
        self.add_component(source)
        return source

    def add_component(self, component):
        component.set_pipeline(self)
        self.components.append(component)

    def run(self):
        for compo in self.components:
            p = Process(target=run_component, args=(compo,))
            p.start()
            self.processes.append(p)

    def join(self):
        for p in self.processes:
            p.join()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.run()
        if self.block:
            self.join()
