import pytest
import typing
import multiprocessing
import random

import flo.api
import flo.exceptions


def test_single(runner):

    state = multiprocessing.Queue()

    def fn(arg1: int):
        state.put(arg1)

    node = flo.api.Node(fn).init(arg1=1)
    runner.add(node)
    runner.execute()

    expected = [1]
    result = sorted(state.get() for _ in expected)

    assert expected == result


def test_basic(runner):

    def init(arg: int, outflow: flo.api.Out[int]):
        for i in range(arg):
            outflow.send(i)

    def log(inflow: flo.api.In[typing.Any], outflow: flo.api.Out[typing.Any]):
        for x in inflow:
            print(x)
            outflow.send(x)

    def sleep(inflow: flo.api.In[typing.Any],
              duration: float,
              outflow: flo.api.Out[typing.Any]):
        import time
        for x in inflow:
            time.sleep(duration)
            outflow.send(x)

    state = multiprocessing.Queue()

    def capture(inflow: flo.api.In[int]):
        for i in inflow:
            state.put(i)

    g = flo.api.Graph(default_runner=runner)

    l1 = g.add(init).init(arg=10)
    l2 = g.add(sleep).init(inflow=l1['outflow'], duration=0.02)

    r1 = g.add(init).init(arg=5)
    r2 = g.add(sleep).init(inflow=r1['outflow'], duration=0.01)

    n3 = g.add(log).init(inflow=(l2['outflow'], r2['outflow']))
    n4 = g.add(capture).init(inflow=n3['outflow'])

    g.submit()

    expected = sorted(list(range(10)) + list(range(5)))
    result = sorted(state.get() for _ in expected)

    assert expected == result


def test_timeout(runner):

    def init(arg: int, outflow: flo.api.Out[int]):
        for i in range(arg):
            outflow.send(i)

    def sleep(inflow: flo.api.In[typing.Any],
              duration: float,
              outflow: flo.api.Out[typing.Any]):
        import time
        for x in inflow:
            time.sleep(duration)
            outflow.send(x)

    g = flo.api.Graph(default_runner=runner)

    l1 = g.add(init).init(arg=1)
    l2 = g.add(sleep).init(inflow=l1['outflow'], duration=0.5)

    with pytest.raises(TimeoutError):
        g.submit(timeout=0.2)


def test_error(runner):

    def init(arg: int, outflow: flo.api.Out[int]):
        for i in range(arg):
            outflow.send(i)

    def error(inflow: flo.api.In[int], raise_on: typing.Optional[int] = None):
        for i in inflow:
            if raise_on is None:
                raise ValueError('Error!')
            elif raise_on == i:
                raise ValueError('I hate {!r}'.format(i))

    g = flo.api.Graph(default_runner=runner)

    l1 = g.add(init).init(arg=5)
    l2 = g.add(error).init(inflow=l1['outflow'], raise_on=3)

    with pytest.raises(flo.exceptions.GraphExecutionError) as excinfo:
        g.submit()
    assert str(excinfo.value).endswith('I hate 3')


@pytest.mark.parametrize('num_nodes', (10, 100, 500))
def test_many_nodes(num_nodes, runner):
    import multiprocessing

    state = multiprocessing.Queue()

    def fn(arg1: int):
        state.put(arg1)

    for i in range(num_nodes):
        runner.add(flo.api.Node(fn).init(arg1=i))

    # We can only start so many threads, so this runner won't scale to a
    # very high number of nodes.
    runner.execute()

    expected = list(range(num_nodes))
    result = sorted(state.get() for _ in expected)

    assert expected == result


def test_multi_runners(all_runners):

    def init(arg: int, outflow: flo.api.Out[int]):
        for i in range(arg):
            outflow.send(i)

    def log(inflow: flo.api.In[typing.Any], outflow: flo.api.Out[typing.Any]):
        for x in inflow:
            print(x)
            outflow.send(x)

    def sleep(inflow: flo.api.In[typing.Any],
              duration: float,
              outflow: flo.api.Out[typing.Any]):
        import time
        for x in inflow:
            time.sleep(duration)
            outflow.send(x)

    state = multiprocessing.Queue()

    def capture(inflow: flo.api.In[int]):
        for i in inflow:
            state.put(i)

    runner1 = all_runners[0]()
    try:
        runner2 = all_runners[1]()
    except IndexError:
        runner2 = all_runners[0]()
    try:
        runner3 = all_runners[2]()
    except IndexError:
        try:
            runner3 = all_runners[1]()
        except IndexError:
            runner3 = all_runners[0]()

    g = flo.api.Graph()

    l1 = g.add(init) \
        .set_runner(runner1) \
        .init(arg=10)
    l2 = g.add(sleep) \
        .set_runner(runner2) \
        .init(inflow=l1['outflow'], duration=0.02)

    r1 = g.add(init) \
        .set_runner(runner3) \
        .init(arg=5)
    r2 = g.add(sleep) \
        .set_runner(runner1) \
        .init(inflow=r1['outflow'], duration=0.01)

    n3 = g.add(log) \
        .set_runner(runner2) \
        .init(inflow=(l2['outflow'], r2['outflow']))
    n4 = g.add(capture) \
        .init(inflow=n3['outflow'])

    graph_runners = g.get_runners()

    runner_nodes = []
    for runner in graph_runners:
        runner_nodes.extend(runner.nodes)
    assert len(runner_nodes) == 6

    assert len(graph_runners) == 4

    assert len(runner1.nodes) == 2
    assert len(runner2.nodes) == 2
    assert len(runner3.nodes) == 1

    g.submit()

    expected = sorted(list(range(10)) + list(range(5)))
    result = sorted(state.get() for _ in expected)

    assert expected == result
