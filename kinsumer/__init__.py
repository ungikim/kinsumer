""":mod:`kinsumer` --- High level Amazon Kinesis Streams consumer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

"""
from .checkpointer import FileCheckpointer, InMemoryCheckpointer
from .consumer import Consumer
from .version import VERSION

__all__ = ('FileCheckpointer', 'InMemoryCheckpointer', 'Consumer')
__version__ = VERSION
