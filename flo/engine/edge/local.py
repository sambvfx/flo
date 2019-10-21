from .base import AbstractBaseEdge


class InMemoryEdge(AbstractBaseEdge):
    """
    Edge that can only be used when all runners are being executed by local
    threads.
    """

    _state = {}

    def send(self, data, key=b'NULL'):
        for id_ in self.ids:
            store = self._state.get(id_, [])
            store.append((key, data))
            self._state[id_] = store

    def pull(self):
        streams = {k: v for k, v in self._state.items() if k in self.ids}
        while streams:
            for id_, stream in streams.copy().items():
                for k, v in stream:
                    if k == self.INIT:
                        continue
                    elif k == self.DONE:
                        streams.pop(id_)
                        continue
                    else:
                        stream.remove((k, v))
                        yield v
                    # allows for round robin
                    break