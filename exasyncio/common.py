""" Common functionality for exasyncio """

from enum import IntEnum
from types import TracebackType
from typing import (
    Optional,
    Type,
)


class ExaConnStatus(IntEnum):
    """ Indicates the status of a connection """
    CLOSED = 0
    WS_CONNECTED = 1
    CONNECTED = 2
    DISCONNECTING = 3
    CLOSING = 4


class AsyncContextMixin:
    """ Mixin to provide asynchronous context support """

    async def __aenter__(self):
        return self

    async def __aexit__(
            self,
            exc_type: Optional[Type[BaseException]],
            exc_val: Optional[BaseException],
            exc_tb: Optional[TracebackType],
            ) -> None:
        await self.close()
