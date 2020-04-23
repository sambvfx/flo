try:
    import cPickle as pickle
except ImportError:
    import pickle

import multiprocessing

from .base import AbstractRemoteRunner
from ...exceptions import RunnerExecutionError


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
            self._cconn.send(
                pickle.dumps(e, protocol=pickle.HIGHEST_PROTOCOL))

    @property
    def exception(self):
        if self._pconn.poll():
            e = self._pconn.recv()
            if e is not None:
                self._exception = pickle.loads(e)
        return self._exception


class SubProcessRunner(AbstractRemoteRunner):

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
                errors[node] = e
        if errors:
            raise RunnerExecutionError(self, errors)
