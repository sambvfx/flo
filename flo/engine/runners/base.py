from ..edge.redis import RedisEdge

from typing import *


if TYPE_CHECKING:
    from ..edge.base import AbstractBaseEdge


class AbstractRunner(object):
    """
    Abstract Runner object.

    A runner is responsible for executing a collection of `Node`s.
    """

    DEFAULT_EDGE = None  # type: AbstractBaseEdge

    def __init__(self, edge=None):
        self._edge = edge or self.DEFAULT_EDGE
        self.nodes = []

    def edge(self, *ids):
        return self._edge(*ids)

    def add(self, *nodes):
        for n in nodes:
            n.set_runner(self)
            if n not in self.nodes:
                self.nodes.append(n)

    def execute(self):
        """
        Entrypoint for executing the runner.

        Raises
        ------
        RunnerExecutionError

        Returns
        -------
        None
        """
        raise NotImplementedError


class AbstractRemoteRunner(AbstractRunner):
    """
    Base class for consolidating common attributes of runners that execute in
    separate processes.
    """

    DEFAULT_EDGE = RedisEdge
