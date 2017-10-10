""":mod:`kinsumer.globals` --- Defines all the global contexts
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

"""
from functools import partial

from werkzeug.local import LocalProxy, LocalStack


def __lookup_shard_object(name):
    top = _shard_ctx_stack.top
    if top is None:
        raise RuntimeError('Working outside of shard context.')
    return getattr(top, name)


def __find_consumer():
    top = _consumer_ctx_stack.top
    if top is None:
        raise RuntimeError('Working outside of consumer context.')
    return top.consumer


_shard_ctx_stack = LocalStack()
_consumer_ctx_stack = LocalStack()
current_consumer = LocalProxy(__find_consumer)
shard = LocalProxy(partial(__lookup_shard_object, 'shard'))
