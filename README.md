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
