from ...exceptions import RunnerCompatibilityError


def compatible(*runners):
    stack = None
    for runner in runners:
        choices = runner._edge.mro()
        if not stack or (len(choices) < len(stack)):
            stack = choices
    edge = stack[0]
    for runner in runners:
        if not issubclass(runner._edge, edge):
            raise RunnerCompatibilityError(
                'Runner {!r} edge is not compatible with '
                '{!r}'.format(runner, edge))
