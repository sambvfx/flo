import pytest

import flo.api
from flo.engine.runners.local import LocalRunner
from flo.engine.edge.local import InMemoryEdge
from flo.engine.edge.redis import RedisEdge
from flo.exceptions import UniqueNodeError, RunnerCompatibilityError


def test_graph_build():

    def fn():
        pass

    g = flo.api.Graph()
    assert len(g.nodes) == 0

    g.add(fn, 'fn')
    assert len(g.nodes) == 1

    with pytest.raises(UniqueNodeError):
        g.add(fn, 'fn')

    assert len(g.nodes) == 1


def test_runner_compat():

    runner1 = LocalRunner(edge=InMemoryEdge)
    runner2 = LocalRunner(edge=RedisEdge)

    def fn():
        pass

    g = flo.api.Graph()
    g.add(fn).set_runner(runner1)
    g.add(fn).set_runner(runner2)

    with pytest.raises(RunnerCompatibilityError):
        g.submit()
