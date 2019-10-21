
class AbstractBaseEdge(object):

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


class AbstractRemoteEdge(AbstractBaseEdge):
    """
    An edge that can be used across processes.
    """
    pass
