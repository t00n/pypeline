from abc import ABC, abstractmethod
from datetime import timedelta
from copy import deepcopy
from collections import defaultdict

from .utils import to_datetime


class Component:
    def __init__(self):
        self.children = []
        self.name = ''

    def __or__(self, other):
        if not isinstance(other, Component):
            raise ValueError("{} should be a Component".format(other))
        self.children.append(other)
        return other

    def propagate(self, row):
        for child in self.children:
            res = self.apply(row)
            if res is not None:
                for r in res:
                    child.propagate(r)

    def apply(self, row):
        yield row

    def __matmul__(self, name):
        self.name = name
        return self


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


class Source(Component, ABC):
    @abstractmethod
    def read(self):
        pass


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

    def apply_row(self, row):
        must_yield = False
        first_time = False
        if len(self.memory) == 0:
            first_time = True
        self.memory.append(row)
        # if this is a time-based sliding window, we keep the first row timestamp
        # as the watermark to keep a track of rows to skip
        if first_time and isinstance(self.window, timedelta):
            self.watermark = to_datetime(self.key.get_value(self.memory[0]))
        # if this is a count-based window, we simply keep `self.window` rows
        # we yield when memory is full
        # if this is a sliding window, we decrement the skip counter on each row
        # and we yield only when counter <= 0
        if isinstance(self.window, int):
            self.memory = self.memory[-self.window:]
            if len(self.memory) == self.window:
                if self.skip is None:
                    must_yield = True
                else:
                    if self.skip_counter == 0:
                        must_yield = True
                        self.skip_counter = self.skip
                    self.skip_counter -= 1
        # if this is a time-based window, we have a watermark specifying the beginning of the current window
        # we yield when the duration of the window >= self.window
        # if fixed, we add self.window to the watermark when we yield
        # if sliding we add self.skip to the watermark when we yield
        # we keep only rows after the watermark
        elif isinstance(self.window, timedelta):
            now = to_datetime(self.key.get_value(row))
            oldest = to_datetime(self.key.get_value(self.memory[0]))
            # use while because if holes between rows are large, we might trigger several windows
            # when we receive only one row
            while now - oldest >= self.window - timedelta(seconds=1):
                watermark = self.watermark + self.window
                to_yield = []
                for elem in self.memory:
                    t = to_datetime(self.key.get_value(elem))
                    if t < watermark:
                        to_yield.append(elem)
                yield deepcopy(to_yield)
                if self.skip is None:
                    self.watermark += self.window
                else:
                    self.watermark += timedelta(seconds=self.skip)
                remaining = []
                for elem in self.memory:
                    t = to_datetime(self.key.get_value(elem))
                    if t >= self.watermark:
                        remaining.append(elem)
                self.memory = remaining
                # in the case we exhaust the memory, we break
                if len(self.memory) > 0:
                    oldest = to_datetime(self.key.get_value(self.memory[0]))
                else:
                    break
        if must_yield:
            yield deepcopy(self.memory)
            if self.skip is None:
                self.memory = []

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


class Pipeline:
    def __or__(self, source):
        if not isinstance(source, Source):
            raise ValueError("First element must be a Source")
        self.source = source
        return self.source

    def run(self):
        for row in self.source.read():
            self.source.propagate(row)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.run()
