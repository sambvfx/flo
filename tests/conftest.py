import os
import time
import collections
import subprocess
import pytest

import flo.engine.runners.local
import flo.engine.runners.multiproc
import flo.engine.edge.local
import flo.engine.edge.redis


_SUPPORTED = {
    flo.engine.runners.local.LocalRunner: [
        flo.engine.edge.local.InMemoryEdge,
        flo.engine.edge.redis.RedisEdge,
    ],
    flo.engine.runners.multiproc.SubProcessRunner: [
        flo.engine.edge.redis.RedisEdge,
    ],
}

try:
    import flo.engine.runners.gevent
    _SUPPORTED[flo.engine.runners.gevent.GeventRunner] = [
        flo.engine.runners.gevent.GeventEdge,
    ]
except ImportError:
    pass


_ALL_RUNNERS = {}
_COMPAT = collections.defaultdict(list)
for runner, edges in _SUPPORTED.items():
    for edge in edges:
        _ALL_RUNNERS['{}({})'.format(runner.__name__, edge.__name__)] = \
            (runner, edge)
        _COMPAT[edge].append(runner)


@pytest.fixture(scope='session', autouse=True)
def docker_redis():
    """
    Session level redis docker instance.
    """
    # FIXME:
    #  - redisilte (https://pypi.org/project/redislite/)
    #    - doesn't support xadd
    #  - fakeredis (https://pypi.org/project/fakeredis/)
    #    - doesn't support xadd
    #  It would be easy enough to make a mock client myself...

    # b'30f2b70c177ca2ee88d409cb27a2a8fd2661a4b344b12d200423b2ba6f3c1213'
    container_id = subprocess.check_output([
        'docker', 'run', '-d', '--rm',
        '--name', 'flo-test-redis',
        '-p', '0:6379',
        'redis',
    ]).strip().decode()

    try:
        # b'6379/tcp -> 0.0.0.0:32772'
        port = subprocess.check_output([
            'docker', 'port', container_id
        ]).strip().decode().rsplit(':')[-1]

        os.environ['FLO_REDIS_URL'] = 'localhost:{}/0'.format(port)

        time.sleep(1)

        tries = 5
        while tries:
            status = subprocess.check_output([
                'docker', 'inspect', '-f', '{{.State.Status}}', container_id
            ]).strip().decode()
            if status == 'running':
                break
            time.sleep(1)
            tries -= 1

        time.sleep(3)
        yield

    finally:
        subprocess.call([
            'docker', 'kill', container_id
        ])


@pytest.fixture(scope='session', autouse=True)
def rlimit():
    """
    Raise the resource limit for this session to avoid issues with spawning
    too many threads.
    """
    import resource
    resource.setrlimit(resource.RLIMIT_NOFILE, (2000, -1))


@pytest.fixture(ids=_ALL_RUNNERS.keys(), params=_ALL_RUNNERS.values())
def runner(request):
    runner, edge = request.param
    yield runner(edge=edge)


@pytest.fixture(params=_COMPAT.keys())
def runners_by_edge(request):
    edge = request.param
    yield [x(edge=edge) for x in _COMPAT[edge]]
