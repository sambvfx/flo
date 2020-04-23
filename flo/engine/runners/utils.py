from .base import AbstractRunner
from ...exceptions import RunnerCompatibilityError

from typing import *


def compatible(*runners: AbstractRunner):
    """
    Raises an error if `runners` are not compatible with each other.

    Parameters
    ----------
    runners : *AbstractRunner

    Raises
    ------
    RunnerCompatibilityError

    Returns
    -------
    None
    """
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
