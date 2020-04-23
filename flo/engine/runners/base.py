from ..edge.base import AbstractBaseEdge
from ..edge.redis import RedisEdge

from typing import *


if TYPE_CHECKING:
    from ...api import Node


class AbstractRunner(object):
    """
    Abstract Runner object.

    A runner is responsible for executing a collection of `Node`s.
    """

    DEFAULT_EDGE = None  # type: Type[AbstractBaseEdge]

    def __init__(
            self,
            edge: Type[AbstractBaseEdge] = None,
    ):
        self._edge = edge or self.DEFAULT_EDGE
        self.nodes = []

    def edge(
            self,
            *ids,
    ) -> AbstractBaseEdge:
        return self._edge(*ids)

    def add(
            self,
            *nodes: 'Node',
    ) -> None:
        for n in nodes:
            n.set_runner(self)
            if n not in self.nodes:
                self.nodes.append(n)

    def execute(self) -> None:
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
