import pytest

from target_oracle.sinks import OracleSink


@pytest.fixture
def sink():
    # Use __new__ to bypass __init__ which requires DB connectivity.
    return OracleSink.__new__(OracleSink)


def test_snakecase_conversion(sink):
    assert sink.snakecase('CustomerIDNumber') == 'customer_id_number'


def test_move_leading_underscores(sink):
    assert sink.move_leading_underscores('__CustomerID') == 'CustomerID__'
    assert sink.move_leading_underscores('NormalName') == 'NormalName'


def test_conform_name_camelcase(sink):
    assert sink.conform_name('CustomerIDNumber') == 'customer_id_number'


def test_conform_name_leading_underscores(sink):
    assert sink.conform_name('__CustomerIDNumber') == 'customer_id_number__'


def test_conform_name_special_chars(sink):
    assert sink.conform_name('foo-bar') == 'foo_bar'


def test_conform_name_leading_digit(sink):
    assert sink.conform_name('1abc') == 'babc'
