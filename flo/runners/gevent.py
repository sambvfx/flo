import functools
import gevent

from .base import Runner as _Runner
from ..edge.redis import RedisEdge as _RedisEdge
from ..exceptions import RunnerExecutionError


class GeventEdge(_RedisEdge):

    def _pre_poll(self):
        # We want to avoid blocking so we sleep at the beginning of each edge
        # poll to ensure we don't spend all our time in the blocked xread call.
        gevent.sleep(0.2)

    def pull(self, count=None, block=500):
        for x in super().pull(count, block):
            yield x
            # This added sleep allows for us to switch greenlets to ensure
            # we're spreading the processing time across all executing nodes.
            gevent.sleep(0.001)


class GeventRunner(_Runner):

    def execute(self):
        errors = {}

        def _check_error(n, g):
            if getattr(g, 'exception', None) is not None:
                errors[n] = g.exception

        greenlets = []
        for node in self.nodes:
            node.set_default_edge(GeventEdge)
            greenlet = gevent.spawn(node)
            greenlet.rawlink(functools.partial(_check_error, node))
            greenlets.append(greenlet)
        gevent.joinall(greenlets)

        if errors:
            raise RunnerExecutionError(self, errors)
