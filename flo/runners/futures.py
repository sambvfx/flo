from .base import Runner as _Runner
from ..exceptions import RunnerExecutionError


class ThreadPoolRunner(_Runner):

    def execute(self):
        from concurrent.futures import ThreadPoolExecutor

        results = {}
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
