""":mod:`linkage.consumer` --- High level Amazon Kinesis Streams consumer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

"""
from gevent import monkey

monkey.patch_all()  # noqa: E402

import os.path
import sys
import pkgutil
import gevent
import signal
from datetime import timedelta, datetime
from typing import Any, Callable, List, Optional

import boto3
from gevent import Greenlet
from gevent.pool import Group
from werkzeug.datastructures import ImmutableDict
from typeguard import typechecked

from .config import ConfigAttribute, Config
from .checkpointer import Checkpointer, InMemoryCheckpointer
from .ctx import ConsumerContext, has_shard_context
from .streams import KinesisStream, KinesisShard
from .globals import shard as current_shard
from .helpers import locked_cached_property
from .logging import create_logger, _Logger

# a singleton sentinel value for parameter defaults
_sentinel = object()


def _get_root_path(import_name: str) -> str:
    """
    Returns the path to a package or cwd if that cannot be found
    """
    mod = sys.modules.get(import_name)
    if mod is not None and hasattr(mod, '__file__'):
        return os.path.dirname(os.path.abspath(mod.__file__))

    loader = pkgutil.get_loader(import_name)
    if loader is None or import_name == '__main__':
        return os.getcwd()

    filepath: str = loader.get_filename(import_name)
    return os.path.dirname(os.path.abspath(filepath))


def _make_timedelta(value: Any) -> timedelta:
    if not isinstance(value, timedelta):
        return timedelta(seconds=value)
    return value


class Consumer(object):
    #: The debug flag
    #:
    #: This attribute can also be configured from the config with the ``DEBUG``
    #: configuration key.  Defaults to ``False``.
    debug: ConfigAttribute = ConfigAttribute('DEBUG')
    #: A :class:`~datetime.timedelta` which is used as
    #: shard iterator interval.
    #:
    #: This attribute can also be configured from the config with
    #: ``SHARD_ITERATOR_INTERVAL`` configuration key.  Defaults to
    #: ``timedelta(seconds=1)``
    shard_iterator_interval: ConfigAttribute = ConfigAttribute(
        'SHARD_ITERATOR_INTERVAL',
        get_converter=_make_timedelta
    )
    #: A :class:`~datetime.timedelta` which is used as
    #: shard monitoring interval.
    #:
    #: This attribute can also be configured from the config with
    #: ``SHARD_MONITORING_INTERVAL`` configuration key.  Defaults to
    #: ``timedelta(hours=1)``
    shard_monitoring_interval: ConfigAttribute = ConfigAttribute(
        'SHARD_MONITORING_INTERVAL',
        get_converter=_make_timedelta
    )
    #: A :class:`~datetime.timedelta` which is used as overhang interval.
    #:
    #: This attribute can also be configured from the config with
    #: ``PROTRACTOR_OVERHANG_INTERVAL`` configuration key.  Defaults to
    #: ``timedelta(seconds=30)``
    protractor_overhang_interval: ConfigAttribute = ConfigAttribute(
        'PROTRACTOR_OVERHANG_INTERVAL',
        get_converter=_make_timedelta
    )
    #: Default configuration parameters.
    __default_config: ImmutableDict = ImmutableDict({
        'DEBUG': False,
        'STREAM_REGION': 'ap-south-1',
        'STREAM_NAME': None,
        'SHARD_ITERATOR_TYPE': 'TRIM_HORIZON',
        'SHARD_READ_LIMIT': 50,
        'SHARD_ITERATOR_INTERVAL': timedelta(seconds=1),
        'SHARD_MONITORING_INTERVAL': timedelta(hours=1),
        'PROTRACTOR_ENABLE': False,
        'PROTRACTOR_OVERHANG_INTERVAL': timedelta(seconds=30),
        'LOGGER_HANDLER_POLICY': 'always',
        'LOG_ROLLOVER': 'd',
        'LOG_INTERVAL': 1,
        'LOG_BACKUP_COUNT': 2,
        'BUCKET_SIZE_LIMIT': 10000,
        'BUCKET_COUNT_LIMIT': 120,
    })
    #: The name of the package or module that this consumer belongs to.
    #: Do not change this once it is set by the constructor.
    import_name: str = None
    #: Absolute path to the package on the filesystem.
    root_path: str = None

    def __init__(self,
                 import_name: str,
                 root_path: str = None,
                 stream_region: str = None,
                 stream_name: str = None,
                 log_folder: str = 'log',
                 checkpointer: Checkpointer = None) -> None:
        self.import_name = import_name
        if root_path is None:
            root_path = _get_root_path(import_name)
        self.root_path = root_path
        self.log_folder = log_folder

        #: The configuration directory as :class:`Config`.
        self.config = Config(self.root_path, self.__default_config)
        if stream_region is not None:
            self.config['STREAM_REGION'] = stream_region
        if stream_name is not None:
            self.config['STREAM_NAME'] = stream_name

        #:
        self.checkpointer = checkpointer
        if self.checkpointer is None:
            self.checkpointer = InMemoryCheckpointer()

        #: A list of functions that will be called at the bucket is full.
        self.__transform_funcs = []
        #: A list of functions that should be called after transform.
        self.__after_consume_func = []
        #: A list of functions that are called when the consumer context
        #: is destroyed.  Since the consumer context is also torn down
        self.__teardown_consumer_func = []

        #:
        self.__threads = Group()
        self.shards = set()

    @locked_cached_property
    def name(self) -> str:
        if self.import_name == '__main__':
            fn = getattr(sys.modules['__main__'], '__file__', None)
            if fn is None:
                return '__main__'
            return os.path.splitext(os.path.basename(fn))[0]
        return self.import_name

    @locked_cached_property
    def logger(self) -> _Logger:
        return create_logger(self)

    @locked_cached_property
    def kinesis_client(self):
        return boto3.client('kinesis',
                            region_name=self.config['STREAM_REGION'])

    @typechecked
    def transform(self, func: Callable[[List[Any],
                                        str,
                                        str,
                                        datetime],
                                       List[Any]]) -> Callable:
        self.__transform_funcs.append(func)
        return func

    @typechecked
    def after_consume(self, func: Callable[[Optional[List[Any]],
                                            str,
                                            Optional[str],
                                            Optional[datetime]],
                                           None]) -> Callable:
        self.__after_consume_func.append(func)
        return func

    @typechecked
    def teardown_consumer(self, func: Callable[[Any], None]) -> Callable:
        self.__teardown_consumer_func.append(func)
        return func

    @typechecked
    def do_transform(self,
                     data: List[Any],
                     shard_id: str,
                     last_sequence_number: str,
                     last_arrival_timestamp: datetime) -> List[Any]:
        for func in reversed(self.__transform_funcs):
            data = func(
                data,
                shard_id,
                last_sequence_number,
                last_arrival_timestamp
            )
        return data

    @typechecked
    def do_after_consume(self,
                         data: Optional[List[Any]],
                         shard_id: str,
                         last_sequence_number: Optional[str],
                         last_arrival_timestamp: Optional[datetime]) -> None:
        for func in reversed(self.__after_consume_func):
            func(
                data,
                shard_id,
                last_sequence_number,
                last_arrival_timestamp
            )

    @typechecked
    def do_teardown_consumer(self, exc=_sentinel) -> None:
        if exc is _sentinel:
            exc = sys.exc_info()[1]
        for func in reversed(self.__teardown_consumer_func):
            func(exc)

    def handle_shard_exception(self, e) -> None:
        exc_type, exc_value, tb = sys.exc_info()
        assert exc_value is e
        self.log_exception((exc_type, exc_value, tb))

    def handle_exception(self, e) -> None:
        exc_type, exc_value, tb = sys.exc_info()
        self.log_exception((exc_type, exc_value, tb))

    def log_exception(self, exc_info) -> None:
        if has_shard_context():
            self.logger.error(
                'Exception on {0}'.format(current_shard.id),
                exc_info=exc_info
            )
        else:
            self.logger.error(
                'Exception', exc_info=exc_info
            )

    def get_context(self) -> ConsumerContext:
        return ConsumerContext(self)

    def get_stream(self) -> KinesisStream:
        return KinesisStream(self.kinesis_client.describe_stream(
            StreamName=self.config['STREAM_NAME']
        ))

    def dispatch(self) -> None:
        stream = self.get_stream()
        if stream.status == 'ACTIVE':
            gevent.signal(signal.SIGQUIT, gevent.killall)
            shards = stream.get_shards(self)
            for shard in shards:
                self.spawn_shard(shard)
            self.__threads.start(ShardMonitor(self))
            self.__threads.join()
        else:
            sys.exit()

    def spawn_shard(self, shard: KinesisShard) -> None:
        self.__threads.start(shard)
        self.shards.add(shard)

    def close_shard(self, shard: KinesisShard) -> None:
        self.logger.warn('Stream \'{0}\' Shard \'{1}\' closed'.format(
            self.config['STREAM_NAME'], shard.id
        ))
        self.shards.remove(shard)

    def process(self, debug=None) -> None:
        if debug is not None:
            self.debug = bool(debug)
        ctx = self.get_context()
        error = None
        try:
            try:
                ctx.push()
                self.dispatch()
            except Exception as e:
                error = e
                self.handle_exception(e)
        finally:
            ctx.pop(error)

    def __repr__(self) -> str:
        return '<{0!s} {1!r} - \'{2!s}\'>'.format(
            self.__class__.__name__,
            self.name,
            self.config['STREAM_NAME']
        )


class ShardMonitor(Greenlet):
    def __init__(self, consumer: Consumer):
        super().__init__()
        self.consumer = consumer
        self.interval = self.consumer.shard_monitoring_interval
        self.running = False

    def _run(self):
        self.running = True
        while self.running:
            stream = self.consumer.get_stream()
            shards = set(
                {shard for shard in stream.get_shards(self.consumer)}
            )
            self.consumer.logger.warn(
                'Monitoring shards:  Consumer({0}), AWS({1})'.format(
                    self.consumer.shards,
                    shards
                ))
            diff_shards = shards - self.consumer.shards
            if len(diff_shards) > 0:
                self.consumer.logger.warn(
                    'Stream {0} Spawning New Shards {1}'.format(
                        self.consumer.config['STREAM_NAME'],
                        len(diff_shards)
                    )
                )
                for shard in diff_shards:
                    self.consumer.spawn_shard(shard)
            gevent.sleep(seconds=self.interval.seconds)
