from datetime import datetime as dt, date
from decimal import Decimal
from freezegun import freeze_time
import pytest

from components.ticker import Ticker
from db import db
from test.fixtures import simple


def setup_function():
    db._env = 'test'
    db.ensure_connected()


def test_ticker_class_instantiates():
    vcn = Ticker('VCN.TO')
    assert vcn is not None
    assert vcn.ticker_name == 'VCN.TO'
    assert 'Empty' in vcn.__repr__()
    assert 'Empty' in str(vcn)


def test_price_does_not_crash_when_empty():
    assert Ticker('VCN.TO').price(date.today()) is None


def test_price(simple):
    with freeze_time(dt(2017, 3, 7)):
        vcn = Ticker('VCN.TO')

    assert vcn.price(date(2017, 3, 1)) is None
    assert vcn.price(date(2017, 3, 2)) == Decimal('30.00')
    assert vcn.price(date(2017, 3, 3)) == Decimal('30.10')
    assert vcn.price(date(2017, 3, 4)) == Decimal('30.10')
    assert vcn.price(date(2017, 3, 5)) == Decimal('30.10')
    assert vcn.price(date(2017, 3, 6)) == Decimal('29.85')
    assert vcn.price(date(2017, 3, 7)) is None
