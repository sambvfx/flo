import os
import time
import subprocess
import pytest

import flo.runners.multiproc
import flo.runners.futures


_RUNNERS = [
    flo.runners.multiproc.SubProcessRunner,
    flo.runners.futures.ThreadPoolRunner,
]


try:
    import flo.runners.gevent
    _RUNNERS.append(flo.runners.gevent.GeventRunner)
except ImportError:
    pass


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


@pytest.fixture
def all_runners():
    return _RUNNERS


@pytest.fixture(params=_RUNNERS)
def runner_cls(request):
    yield request.param


@pytest.fixture
def runner(runner_cls):
    yield runner_cls()
