import multiprocessing
import traceback

from .base import Runner as _Runner
from ..exceptions import RunnerExecutionError


class Process(multiprocessing.Process):
    def __init__(self, *args, **kwargs):
        super(Process, self).__init__(*args, **kwargs)
        self._pconn, self._cconn = multiprocessing.Pipe()
        self._exception = None

    def run(self):
        try:
            multiprocessing.Process.run(self)
            self._cconn.send(None)
        except Exception as e:
            tb = traceback.format_exc()
            self._cconn.send((e, tb))

    @property
    def exception(self):
        if self._pconn.poll():
            self._exception = self._pconn.recv()
        return self._exception


class SubProcessRunner(_Runner):

    def execute(self):
        results = {}
        for node in self.nodes:
            proc = Process(target=node)
            results[node] = proc
            proc.start()
        for proc in results.values():
            proc.join()

        errors = {}
        for node, proc in results.items():
            e = proc.exception
            if e:
                errors[node] = e[0]
        if errors:
            raise RunnerExecutionError(self, errors)
