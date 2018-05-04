from abc import ABC, abstractmethod
from datetime import timedelta
from copy import deepcopy

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


class Window(Component):
    def __init__(self, window, *, fixed=True, key=None):
        super().__init__()
        if isinstance(window, int) \
            or isinstance(window, timedelta) \
                and (isinstance(key, str) or callable(key)):
            self.window = window
            self.key = key
        else:
            raise ValueError("Window should be an integer or a timedelta and a key")
        self.memory = []
        self.fixed = fixed

    def get_value(self, row):
        if callable(self.key):
            return self.key(row)
        else:
            return row[self.key]

    def apply(self, row):
        self.memory.append(row)
        if isinstance(self.window, int):
            self.memory = self.memory[-self.window:]
            if len(self.memory) == self.window:
                yield deepcopy(self.memory)
                if self.fixed:
                    self.memory = []
        elif isinstance(self.window, timedelta):
            now = to_datetime(self.get_value(row))
            watermark = now - self.window
            remaining = []
            for elem in self.memory:
                t = to_datetime(self.get_value(elem))
                if t > watermark and t <= now:
                    remaining.append(elem)
            self.memory = remaining
            if now - to_datetime(self.get_value(self.memory[0])) >= self.window - timedelta(seconds=1):
                yield self.memory
                if self.fixed:
                    self.memory = []


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
