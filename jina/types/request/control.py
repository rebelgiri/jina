from . import Request

from .mixin import CommandMixin


class ControlRequest(Request, CommandMixin):
    """Control request class."""

    pass
