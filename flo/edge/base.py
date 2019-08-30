
class Edge(object):

    INIT = b'<INIT>'
    DONE = b'<DONE>'

    def __init__(self, *ids):
        self.ids = ids

    def start(self):
        self.send(b'NULL', key=self.INIT)

    def stop(self):
        self.send(b'NULL', key=self.DONE)

    def send(self, data, key=b'NULL'):
        raise NotImplementedError

    def pull(self):
        raise NotImplementedError

    def __iter__(self):
        yield from self.pull()


class InMemoryEdge(Edge):
    """
    Edge for debugging purposes only.
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


class StateEdge(Edge):
    """
    A stateful edge introduces an interface for the user to checkpoint
    progress in the iteration process. If re-invoked, it's expected the edge
    yield messages starting from the last checkpoint.
    """

    def checkpoint(self):
        raise NotImplementedError
