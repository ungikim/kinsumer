""":mod:`kinsumer.bucket` --- Temporary saving records
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

"""
import abc
from datetime import datetime
from typeguard import typechecked
from typing import List, Optional, Tuple, Any, TYPE_CHECKING

if TYPE_CHECKING:
    from .streams import KinesisRecord


class Bucket(abc.ABC, object):
    """Bucket is the interface for temporary saving records for Kinesis shards

    """

    @abc.abstractmethod
    def flush(self) -> None:
        """Clear the temporary storage"""

    @abc.abstractmethod
    def get(self, force: bool = False) -> Tuple[List[Any],
                                                Optional[str],
                                                Optional[datetime]]:
        """Get a list whose records if available"""

    @abc.abstractmethod
    def add(self, record: 'KinesisRecord') -> None:
        """Temporary save a given record"""


class InMemoryBucket(Bucket):
    def __init__(self, size_limit: int, count_limit: int) -> None:
        self.__data: List['KinesisRecord'] = []
        self.__count = 0
        self.__size_limit = size_limit
        self.__count_limit = count_limit

    def flush(self) -> None:
        del self.__data[:]
        self.__count = 0

    @typechecked
    def get(self, force: bool = False) -> Tuple[List[Any],
                                                Optional[str],
                                                Optional[datetime]]:
        if force:
            return self.__get()
        else:
            self.__count += 1
            size = len(self.__data)
            if size >= self.__size_limit or self.__count >= self.__count_limit:
                if size > 0:
                    return self.__get()
            return [], None, None

    def add(self, record: 'KinesisRecord') -> None:
        self.__data.append(record)

    def __get(self):
        return (
            self.__data,
            self.__data[-1].sequence_number,
            self.__data[-1].approximate_arrival_timestamp
        )
