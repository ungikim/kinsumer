import pytest

from kinsumer import Consumer

KINESIS_REGION = 'my-aws-region'
KINESIS_STREAM_NAME = 'my-stream'
BUCKET_COUNT_LIMIT = 1
pytestmark = pytest.mark.usefixtures('example')


def test_simple_example():
    consumer = Consumer(__name__)
    consumer.config.from_object(__name__)
    consumer.process(debug=True)
