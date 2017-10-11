""":mod:`kinsumer.checkpointer` --- Persisting positions for Kinesis shards
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

"""
import abc
import json
import os.path
from typing import Optional, Dict


class Checkpointer(abc.ABC, object):
    """Checkpointer is the interface for persisting positions for Kinesis shards

    """

    @abc.abstractmethod
    def get_checkpoints(self) -> Dict[str, str]:
        """Get a dictionary whose keys are all the shard ids we are aware of,
        and whose values are the sequence id of the last record processed for
        its shard

        """

    @abc.abstractmethod
    def checkpoint(self, shard_id: str, sequence: str) -> None:
        """Persist the sequence number for a given shard"""

    @abc.abstractmethod
    def get_checkpoint(self, shard_id: str) -> Optional[str]:
        """Get the sequence number of the last successfully processed record"""


class InMemoryCheckpointer(Checkpointer):
    def __init__(self) -> None:
        self._checkpoints = {}

    def get_checkpoints(self) -> Dict[str, str]:
        return self._checkpoints.copy()

    def checkpoint(self, shard_id: str, sequence: str) -> None:
        self._checkpoints[shard_id] = sequence

    def get_checkpoint(self, shard_id: str) -> Optional[str]:
        return self._checkpoints.get(shard_id)


class FileCheckpointer(InMemoryCheckpointer):
    def __init__(self, file: str) -> None:
        super().__init__()
        self.file = os.path.expanduser(file)
        if os.path.exists(self.file):
            with open(self.file, 'rb') as f:
                self._checkpoints = json.load(f)

    def checkpoint(self, shard_id: str, sequence: str) -> None:
        super().checkpoint(shard_id, sequence)
        with open(self.file, 'wb') as f:
            f.write(json.dumps(self._checkpoints, ensure_ascii=False).encode())
