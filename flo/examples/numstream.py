import typing

import flo.api


def generate(arg: int, outflow: flo.api.Out[int]):
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


def main(runner=None):

    from flo.edge.base import InMemoryEdge

    if runner:
        print('Executing runner {!r}'.format(runner))

    g = flo.api.Graph(default_runner=runner)

    l1 = g.add(generate).init(arg=10).set_default_edge(InMemoryEdge)
    l2 = g.add(sleep).init(inflow=l1['outflow'], duration=0.2).set_default_edge(InMemoryEdge)

    r1 = g.add(generate).init(arg=5).set_default_edge(InMemoryEdge)
    r2 = g.add(sleep).init(inflow=r1['outflow'], duration=0.1).set_default_edge(InMemoryEdge)

    n3 = g.add(log).init(inflow=(l2['outflow'], r2['outflow'])).set_default_edge(InMemoryEdge)

    g.submit()


if __name__ == '__main__':
    main()
