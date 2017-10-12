kinsumer
========

High level [Amazon Kinesis Streams](https://aws.amazon.com/kinesis/streams/) consumer.

Some features
-------------

* Automatically detect shard count changes
* Checkpoints/sequences persistence can be customized
* Provided Checkpointer implementation for memory, and file
* Memory bucket for temporary saving records

Missing features
----------------

* Redis Checkpointer

Installation
------------

Install the latest stable version from Pypi with `pip install kinsumer`

Usage
-----

```python
from kinsumer import Consumer

STREAM_REGION = 'ap-south-1'
STREAM_NAME = 'my-stream'
consumer = Consumer(__name__)
consumer.config.from_object(__name__)

@consumer.transform
def transform(data, shard_id, last_sequence_number, last_arrival_timestamp):
    """do transform and return"""
    return data

@consumer.after_consume
def after(data, shard_id, last_sequence_number, last_arrival_timestamp):
    """after transform and do something"""

if __name__ == '__main__':
    consumer.process()
```
