from abc import ABC, abstractmethod


class Component:
    def __init__(self):
        self.children = []

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
    def __init__(self, window):
        super().__init__()
        self.window = window
        self.memory = []

    def apply(self, row):
        if isinstance(self.window, int):
            self.memory.append(row)
            self.memory[-self.window:]
            if len(self.memory) == self.window:
                yield self.memory


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
