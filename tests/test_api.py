import pytest

import flo.api
from flo.exceptions import UniqueNodeError


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
