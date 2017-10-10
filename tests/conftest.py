import datetime
from unittest.mock import MagicMock

import pytest


def pytest_configure(config):
    import sys
    sys._called_from_test = True


@pytest.fixture
def kinesis_client() -> MagicMock:
    mock = MagicMock()
    mock.describe_stream.return_value = {
        'StreamDescription': {
            'StreamStatus': 'ACTIVE',
            'Shards': [
                {'ShardId': 'shardId-000000000000', }
            ],
        }
    }
    mock.get_shard_iterator.return_value = {'ShardIterator': 'iterator1'}
    mock.get_records.return_value = {
        'Records': [
            {'Data': b'{"_key": "1", "message": "message1"}',
             'SequenceNumber': 'sequence1',
             'ApproximateArrivalTimestamp': datetime.datetime.now()},
            {'Data': b'{"_key": "2", "message": "message2"}',
             'SequenceNumber': 'sequence2',
             'ApproximateArrivalTimestamp': datetime.datetime.now()},
            {'Data': b'{"_key": "3", "message": "message3"}',
             'SequenceNumber': 'sequence3',
             'ApproximateArrivalTimestamp': datetime.datetime.now()},
        ],
        'NextShardIterator': 'iterator2'
    }
    return mock


@pytest.fixture
def example(mocker, kinesis_client):
    mocker.patch('boto3.client', return_value=kinesis_client)
