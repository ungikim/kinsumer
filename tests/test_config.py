from kinsumer import Consumer

TEST_KEY = 'foo'
FOO_OPTION_1 = 'foo option 1'
FOO_OPTION_2 = 'foo option 2'
BAR_STUFF_1 = 'bar stuff 1'
BAR_STUFF_2 = 'bar stuff 2'


def common_object_test(consumer):
    assert consumer.config['TEST_KEY'] == 'foo'
    assert 'TestConfig' not in consumer.config


def test_config_from_file():
    consumer = Consumer(__name__)
    consumer.config.from_pyfile(__file__.rsplit('.', 1)[0] + '.py')
    common_object_test(consumer)


def test_config_from_object():
    consumer = Consumer(__name__)
    consumer.config.from_object(__name__)
    common_object_test(consumer)


def test_get_namespace():
    consumer = Consumer(__name__)
    consumer.config.from_object(__name__)
    foo_options = consumer.config.get_namespace('FOO_')
    assert 2 == len(foo_options)
    assert 'foo option 1' == foo_options['option_1']
    assert 'foo option 2' == foo_options['option_2']
    bar_options = consumer.config.get_namespace('BAR_', lowercase=False)
    assert 2 == len(bar_options)
    assert 'bar stuff 1' == bar_options['STUFF_1']
    assert 'bar stuff 2' == bar_options['STUFF_2']
    foo_options = consumer.config.get_namespace('FOO_', trim_namespace=False)
    assert 2 == len(foo_options)
    assert 'foo option 1' == foo_options['foo_option_1']
    assert 'foo option 2' == foo_options['foo_option_2']
    bar_options = consumer.config.get_namespace('BAR_',
                                                lowercase=False,
                                                trim_namespace=False)
    assert 2 == len(bar_options)
    assert 'bar stuff 1' == bar_options['BAR_STUFF_1']
    assert 'bar stuff 2' == bar_options['BAR_STUFF_2']
