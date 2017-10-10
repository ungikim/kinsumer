""":mod:`linkage.ctx` --- Implements the objects required to keep the context
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

"""
import sys
from contextlib import AbstractContextManager
from typing import TYPE_CHECKING

from .globals import _consumer_ctx_stack, _shard_ctx_stack

if TYPE_CHECKING:
    from .consumer import Consumer
    from .streams import KinesisShard

_sentinel = object()


def has_shard_context() -> bool:
    return _shard_ctx_stack.top is not None


def has_consumer_context() -> bool:
    return _consumer_ctx_stack.top is not None


class ConsumerContext(AbstractContextManager):
    def __init__(self, consumer: 'Consumer') -> None:
        self.consumer = consumer
        self.__refcnt = 0

    def __enter__(self):
        self.push()
        return self

    def __exit__(self, exc_type, exc_value, tb):
        self.pop(exc_value)
        return None

    def push(self):
        self.__refcnt += 1
        _consumer_ctx_stack.push(self)

    def pop(self, exc=_sentinel):
        try:
            self.__refcnt -= 1
            if self.__refcnt <= 0:
                if exc is _sentinel:
                    exc = sys.exc_info()[1]
                self.consumer.do_teardown_consumer(exc)
        finally:
            rv = _consumer_ctx_stack.pop()
        assert rv is self, 'Popped wrong consumer context.  ' \
                           '({0!r} instead of {1!r}'.format(rv, self)


class ShardContext(AbstractContextManager):
    def __init__(self, shard: 'KinesisShard') -> None:
        self.shard = shard

    def __enter__(self):
        self.push()
        return self

    def __exit__(self, exc_type, exc_val, tb):
        self.pop(exc_val)
        return None

    def push(self):
        _shard_ctx_stack.push(self)

    def pop(self, exc=_sentinel):
        try:
            self.shard.consumer.close_shard(self.shard)
        finally:
            rv = _shard_ctx_stack.pop()
        assert rv is self, 'Popped wrong shard context.  ' \
                           '({0!r} instead of {1!r}'.format(rv, self)
