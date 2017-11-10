""":mod:`linkage.streams` --- Implements the Amazon Kinesis Streams related
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

"""
import sys

import gevent
import json
from botocore.exceptions import ClientError
from datetime import datetime, timezone
from gevent import Greenlet
from typeguard import typechecked
from typing import TYPE_CHECKING, Tuple, List, Optional

from .bucket import InMemoryBucket
from .ctx import ShardContext
from .helpers import locked_cached_property, reraise

if TYPE_CHECKING:
    from .consumer import Consumer


class KinesisRecord(object):
    def __init__(self, shard_id: str, raw_record: dict) -> None:
        self.shard_id: str = shard_id
        self.sequence_number: str = raw_record.get('SequenceNumber')
        self.approximate_arrival_timestamp: datetime = raw_record.get(
            'ApproximateArrivalTimestamp'
        )
        self.data: bytes = raw_record.get('Data')
        self.partition_key: str = raw_record.get('PartitionKey')
        self.raw_record_str: str = json.dumps({
            'SequenceNumber': self.sequence_number,
            'ApproximateArrivalTimestamp':
                self.approximate_arrival_timestamp.isoformat(sep=' '),
            'Data': self.data.decode(),
            'PartitionKey': self.partition_key
        })

    def __repr__(self):
        return str(self.raw_record_str)

    @staticmethod
    def build(shard_id: str,
              sequence_number: str,
              approximate_arrival_timestamp: datetime,
              data: bytes,
              partition_key: str):
        return KinesisRecord(shard_id, {
            'SequenceNumber': sequence_number,
            'ApproximateArrivalTimestamp': approximate_arrival_timestamp,
            'Data': data,
            'PartitionKey': partition_key,
        })


class KinesisShard(Greenlet):
    def __init__(self,
                 consumer: 'Consumer',
                 raw_response: dict) -> None:
        super().__init__()
        self.consumer = consumer
        self.bucket = InMemoryBucket(
            self.consumer.config['BUCKET_SIZE_LIMIT'],
            self.consumer.config['BUCKET_COUNT_LIMIT']
        )
        self.stream_name: str = self.consumer.config['STREAM_NAME']
        self.id = raw_response['ShardId']
        self.iterator_type: str = self.consumer.config['SHARD_ITERATOR_TYPE']
        self.iterator_interval: datetime = \
            self.consumer.shard_iterator_interval
        self.read_limit: int = self.consumer.config['SHARD_READ_LIMIT']
        self.sequence_number: str = self.consumer.checkpointer.get_checkpoint(
            self.id
        )
        self.iterator = self.__get_iterator(
            sequence_number=self.sequence_number
        )
        self.__closed = False

    def __repr__(self) -> str:
        return '<{0!s} {1!s}>'.format(
            self.__class__.__name__,
            self.id,
        )

    def __hash__(self) -> int:
        return hash(self.id)

    def __eq__(self, o: object) -> bool:
        return self.id == o.id

    def get_context(self) -> ShardContext:
        return ShardContext(self)

    def dispatch(self) -> None:
        while not self.__closed:
            self.consumer.logger.info('Processing Shard %s', self.id)
            records = None
            try:
                records, next_iterator = self.__get_records()
            except self.consumer.kinesis_client.exceptions. \
                    ExpiredIteratorException as e:
                raise e
            except self.consumer.kinesis_client.exceptions \
                    .ProvisionedThroughputExceededException as e:
                self.consumer.handle_shard_exception(e)
                gevent.sleep(seconds=self.iterator_interval.seconds)
                continue
            except ClientError as e:
                self.consumer.handle_shard_exception(e)
            last_arrival_timestamp = None
            if len(records) > 0:
                try:
                    for record in records:
                        self.bucket.add(record)
                except Exception as e:
                    self.consumer.handle_shard_exception(e)
                last_arrival_timestamp = records[-1]. \
                    approximate_arrival_timestamp
            if next_iterator:
                self.iterator = self.__get_iterator(
                    iterator=next_iterator,
                    last_arrival_timestamp=last_arrival_timestamp
                )
            else:
                self.__closed = True
            del records, next_iterator
            if self.__closed:
                (data,
                 last_sequence_number,
                 last_arrival_timestamp) = self.bucket.get(True)
            else:
                (data,
                 last_sequence_number,
                 last_arrival_timestamp) = self.bucket.get(False)
            if len(data) > 0:
                try:
                    data = self.consumer.do_transform(
                        data=data,
                        shard_id=self.id,
                        last_sequence_number=last_sequence_number,
                        last_arrival_timestamp=last_arrival_timestamp
                    )
                    self.consumer.do_after_consume(
                        data=data,
                        shard_id=self.id,
                        last_sequence_number=last_sequence_number,
                        last_arrival_timestamp=last_arrival_timestamp
                    )
                except Exception as e:
                    self.consumer.handle_shard_exception(e)
                    exc_type, exc_value, tb = sys.exc_info()
                    if exc_value is e:
                        reraise(exc_type, exc_value, tb)
                    else:
                        raise e
                finally:
                    self.bucket.flush()
                    self.consumer.checkpointer.checkpoint(self.id,
                                                          last_sequence_number)
            del data, last_sequence_number, last_arrival_timestamp
            gevent.sleep(seconds=self.iterator_interval.seconds)
        self.consumer.close_shard(self)

    @typechecked
    def __get_iterator(self,
                       iterator: str = None,
                       sequence_number: str = None,
                       last_arrival_timestamp: datetime = None) -> str:
        if self.consumer.config['PROTRACTOR_ENABLE']:
            if last_arrival_timestamp is not None \
                    and self.__overhang(last_arrival_timestamp):
                self.consumer.logger.warn('Shard %s Overhang sequence! move '
                                          'to LATEST', self.id)
                return self.consumer.kinesis_client.get_shard_iterator(
                    StreamName=self.stream_name,
                    ShardId=self.id,
                    ShardIteratorType='LATEST'
                )['ShardIterator']
        if iterator is not None:
            return iterator
        elif sequence_number is not None:
            return self.consumer.kinesis_client.get_shard_iterator(
                StreamName=self.stream_name,
                ShardId=self.id,
                ShardIteratorType='AFTER_SEQUENCE_NUMBER',
                StartingSequenceNumber=sequence_number
            )['ShardIterator']
        else:
            return self.consumer.kinesis_client.get_shard_iterator(
                StreamName=self.stream_name,
                ShardId=self.id,
                ShardIteratorType=self.iterator_type
            )['ShardIterator']

    @typechecked
    def __overhang(self, last_arrival_timestamp: datetime) -> bool:
        overhang_interval = self.consumer.protractor_overhang_interval
        now = datetime.now(tz=timezone.utc)
        if now - last_arrival_timestamp > overhang_interval:
            return True
        return False

    def __get_records(self, limit=None) -> Tuple[Optional[List[KinesisRecord]],
                                                 Optional[str]]:
        if limit is not None:
            limit = int(limit)
        else:
            limit = self.read_limit
        raw_response = self.consumer.kinesis_client.get_records(
            ShardIterator=self.iterator,
            Limit=limit
        )
        return (
            [
                KinesisRecord(self.id, record) for record in
                raw_response.get('Records', [])
            ],
            raw_response.get('NextShardIterator')
        )

    def _run(self) -> None:
        ctx = self.get_context()
        try:
            ctx.push()
            self.dispatch()
        except Exception as e:
            self.consumer.handle_exception(e)
        finally:
            ctx.pop()


class KinesisStream(object):
    def __init__(self, raw_response: dict) -> None:
        self.__raw_response = raw_response

    @locked_cached_property
    def status(self):
        return self.__raw_response.get('StreamDescription', {}).get(
            'StreamStatus'
        )

    def get_shards(self, consumer: 'Consumer') -> List[KinesisShard]:
        return [
            KinesisShard(consumer, shard) for shard in
            self.__raw_response.get('StreamDescription', {}).get('Shards', [])
        ]
