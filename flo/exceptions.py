import traceback


class FloError(Exception):
    pass


class UniqueNodeError(FloError):
    pass


class RunnerCompatibilityError(Exception):
    pass


class RunnerExecutionError(FloError):
    def __init__(self, runner, errors):
        msg = ['Errors in {!r} execution:'.format(runner)]
        for node, err in errors.items():
            msg.append('\n{}'.format(node))
            msg.append(''.join(traceback.format_tb(err.__traceback__)).rstrip())
            msg.append(str(err))

        super(RunnerExecutionError, self).__init__('\n'.join(msg))


class GraphExecutionError(FloError):
    def __init__(self, errors):
        super(GraphExecutionError, self).__init__(
            '\n\n'.join(str(x) for x in errors))
