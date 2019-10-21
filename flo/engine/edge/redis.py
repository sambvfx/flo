import os
import re
import pickle
import threading

import redis

from .base import AbstractRemoteEdge


class _ClientManager(object):
    DEFAULT_HOST = 'localhost'
    DEFAULT_PORT = 6379
    DEFAULT_DB = 0

    REDIS_URL_REGEX = re.compile(
        r'(?P<host>[^:]+)(:(?P<port>[0-9]+))?(/(?P<db>[0-9]+))?')

    _lock = threading.Lock()

    def __init__(self):
        self.pools = {}

    def get(self, url=None):

        if url is None:
            url = os.environ.get('FLO_REDIS_URL')

        kwargs = {
            'host': self.DEFAULT_HOST,
            'port': self.DEFAULT_PORT,
            'db': self.DEFAULT_DB,
        }

        match = self.REDIS_URL_REGEX.match(url or '')
        if match:
            kwargs.update({k: v for k, v in match.groupdict().items() if v})
            kwargs['port'] = int(kwargs['port'])
            kwargs['db'] = int(kwargs['db'])

        with self._lock:
            _key = '{host}:{port}/{db}'.format(**kwargs)
            pool = self.pools.get(_key)
            if pool is None:
                pool = redis.ConnectionPool(**kwargs)
                self.pools[_key] = pool

        client = redis.Redis(connection_pool=pool)

        if not client.ping():
            raise RuntimeError('Redis is not available at {!r}'.format(kwargs))
        return client


_manager = _ClientManager()


def serialize(data):
    return pickle.dumps(data)


def deserialize(data):
    return pickle.loads(data)


class RedisEdge(AbstractRemoteEdge):

    lock = threading.RLock()

    def __init__(self, *args, url=None):
        super(RedisEdge, self).__init__(*args)
        self.db = _manager.get(url=url)

    def checkpoint(self):
        # TODO
        raise NotImplementedError

    def send(self, data, key=b'NULL'):
        for id_ in self.ids:
            self.db.xadd(id_, {key: serialize(data)})

    def _pre_poll(self):
        pass

    def _post_poll(self):
        pass

    def pull(self, count=None, block=2000):
        active = list(bytes(x.encode()) for x in self.ids)

        streams = {k: b'0-0' for k in active}

        while active:

            self._pre_poll()

            for id_, payload in self.db.xread(
                    streams, count=count, block=block):
                print(id_, payload)
                with self.lock:
                    for msgid, kv in payload:
                        streams[id_] = msgid
                        # `kv` should only be 1 item
                        for k, v in kv.items():
                            if k == self.INIT:
                                continue
                            elif k == self.DONE:
                                active.remove(id_)
                                continue
                            else:
                                yield deserialize(v)

            self._post_poll()
