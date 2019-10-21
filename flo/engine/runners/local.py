from ..edge.local import InMemoryEdge
from ...exceptions import RunnerExecutionError
from .base import AbstractRunner

from typing import *

if TYPE_CHECKING:
    from ...api import Node
    from concurrent.futures import Future


class LocalRunner(AbstractRunner):
    """
    Local runner that uses a thread pool to execute all nodes.
    """

    DEFAULT_EDGE = InMemoryEdge

    def execute(self):
        from concurrent.futures import ThreadPoolExecutor, Future

        results = {}  # type: Dict[Node, Future]
        # NOTE: Only viable for a moderate # of nodes (< 1000?)
        with ThreadPoolExecutor(max_workers=len(self.nodes)) as executor:
            for node in self.nodes:
                results[node] = executor.submit(node)

        errors = {}
        for k, v in results.items():
            e = v.exception()
            if e:
                errors[k] = e
        if errors:
            raise RunnerExecutionError(self, errors)
