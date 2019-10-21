from __future__ import annotations

import uuid
import typing

import dill

from .engine.runners.local import LocalRunner
from .engine.runners.utils import compatible
from .exceptions import UniqueNodeError, GraphExecutionError


if typing.TYPE_CHECKING:
    from .engine.runners.base import AbstractRunner
    from typing import *


NoneType = type(None)
T = typing.TypeVar('T')


DEFAULT_RUNNER = LocalRunner()


class _BasePort(object):

    edge = None

    def __init__(self, id_, type_):
        self.id = id_
        self.type = type_

    def __repr__(self):
        return '<{}[{}]>'.format(self.__class__.__name__, self.type)


class Out(_BasePort, typing.Generic[T]):

    def send(self, data: T) -> None:
        if self.edge is None:
            raise RuntimeError('No edge')
        self.edge.send(data)


class In(_BasePort, typing.Iterable, typing.Generic[T]):

    def __iter__(self) -> T:
        if self.edge is None:
            return
        yield from self.edge


class Connection(typing.List[_BasePort]):
    def __init__(self, *args):
        super(Connection, self).__init__(args)


def validate_types(left, right):
    # FIXME: improve this...
    if left.type == typing.Any:
        return True
    if right.type == typing.Any:
        return True
    if isinstance(left.type, typing.TypeVar):
        return True
    if isinstance(right.type, typing.TypeVar):
        return True
    if right.type == left.type:
        return True
    return False


class Node(object):

    def __init__(self, fn, name=None):
        if name is None:
            name = fn.__name__

        self.id = name
        self.fn = fn

        self.inports = {}
        self.outports = {}
        for k, v in typing.get_type_hints(self.fn).items():
            if hasattr(v, '__origin__'):
                if v.__origin__ is In:
                    self.inports[k] = v('/'.join((self.id, k)), v.__args__[0])
                elif v.__origin__ is Out:
                    self.outports[k] = v('/'.join((self.id, k)), v.__args__[0])

        self.initializations = {}
        self._runner = None

    @property
    def name(self):
        return self.id

    def __getstate__(self):
        return (
            self.id,
            dill.dumps(self.fn),
            dill.dumps(self.inports),
            dill.dumps(self.outports),
            dill.dumps(self.initializations),
            dill.dumps(self._runner)
        )

    def __setstate__(self, state):
        id_, fn, inports, outports, initializations, runner = state
        self.id = id_
        self.fn = dill.loads(inports)
        self.inports = dill.loads(inports)
        self.outports = dill.loads(outports)
        self.initializations = dill.loads(initializations)
        self._runner = dill.loads(runner)

    def __repr__(self):
        # <Node[fn1](fn(foo='bar'))>
        return '<{}[{}]({}({})>'.format(
            self.__class__.__name__,
            self.id,
            self.fn.__name__,
            ', '.join('{}={!r}'.format(k, v)
                      for k, v in self.initializations.items()))

    def __getitem__(self, item):
        return self.outports[item]

    def get_kwargs(self):
        kwargs = self.initializations.copy()

        runner = self.runner or DEFAULT_RUNNER

        # add edges to in/out ports
        for name, port in self.inports.items():
            try:
                connection = kwargs[name]
            except KeyError:
                pass
            else:
                assert isinstance(connection, Connection)
                port.edge = runner.edge(*(x.id for x in connection))
            kwargs[name] = port
        for name, port in self.outports.items():
            port.edge = runner.edge(port.id)
            kwargs[name] = port

        return kwargs

    def validate(self):
        for k in self.inports:
            if k not in self.initializations:
                raise ValueError('In port {!r} not initialized'.format(k))

    def __call__(self):

        self.validate()

        kwargs = self.get_kwargs()

        for k in self.inports:
            if k not in kwargs:
                raise ValueError('In port {!r} not initialized'.format(k))

        for k in self.outports:
            if k not in kwargs:
                raise ValueError('Out port {!r} not initialized'.format(k))

        # FIXME: remove testing code
        _mod = self.fn.__module__ + '.' if self.fn.__module__ != '__main__' \
            else ''
        print('{}{}({})'.format(
            _mod,
            self.fn.__name__,
            ', '.join('{}={!r}'.format(k, v) for k, v in kwargs.items())))

        outports = [x for x in kwargs.values() if isinstance(x, Out)]

        for port in outports:
            port.edge.start()
        try:
            self.fn(**kwargs)
        finally:
            for port in outports:
                port.edge.stop()

    def init(self, **kwargs):
        for k, v in kwargs.items():
            try:
                inport = self.inports[k]
            except KeyError:
                self.initializations[k] = v
            else:
                if isinstance(v, Out):
                    assert validate_types(v, inport)
                    c = self.initializations.get(k)
                    if c is None:
                        c = Connection(v)
                    else:
                        c.append(v)
                    self.initializations[k] = c
                elif (isinstance(v, typing.Iterable)
                      and all(isinstance(x, Out) for x in v)):
                    assert all(validate_types(x, inport) for x in v)
                    c = self.initializations.get(k)
                    if c is None:
                        c = Connection(*v)
                    else:
                        c.extend(v)
                    self.initializations[k] = c
                else:
                    raise NotImplementedError(
                        'Cannot provide static values to ports.')
        return self

    def set_runner(
            self,
            value: AbstractRunner,
    ):
        self._runner = value
        return self

    @property
    def runner(self):
        return self._runner

    @runner.setter
    def runner(self, value):
        self._runner = value


class Graph(object):

    def __init__(
            self,
            id_=None,
            default_runner: Optional[AbstractRunner] = None,
    ):
        self.id = id_ or str(uuid.uuid4())
        self.nodes = {}
        self._default_runner = default_runner

    def __repr__(self):
        return '<{}({!r})>'.format(self.__class__.__name__, self.id)

    def _get_unique_node_name(
            self,
            fn: Callable,
    ):
        i = 1
        while True:
            nodeid = '{}{}'.format(fn.__name__, i)
            if nodeid not in self.nodes:
                return nodeid
            i += 1

    def add(
            self,
            fn: Callable,
            nodeid: Optional[str] = None,
    ):
        if nodeid is None:
            nodeid = self._get_unique_node_name(fn)
        if nodeid in self.nodes:
            raise UniqueNodeError(
                '{!r} already exists in the graph'.format(nodeid))

        node = Node(fn, '/'.join((self.id, nodeid)))
        self.nodes[nodeid] = node

        return node

    def get_runners(self):

        results = []

        default_runner = self._default_runner or DEFAULT_RUNNER

        for node in self.nodes.values():
            node.validate()
            runner = node.runner or default_runner
            runner.add(node)
            if runner not in results:
                results.append(runner)

        compatible(*results)

        return results

    def submit(
            self,
            timeout: Optional[float] = None,
    ):
        from concurrent.futures import ThreadPoolExecutor, wait

        runners = self.get_runners()

        executor = ThreadPoolExecutor(len(runners))
        futures = []
        for runner in runners:
            futures.append(executor.submit(runner.execute))

        result = wait(futures, timeout=timeout)

        for future in futures:
            future.cancel()

        errors = []
        for future in futures:
            e = future.exception()
            if e:
                errors.append(e)
        if errors:
            raise GraphExecutionError(errors)

        if result.not_done:
            raise TimeoutError('Some nodes were still executing.')

        executor.shutdown(wait=False)
