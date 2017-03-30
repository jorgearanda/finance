from datetime import datetime as dt, date
from freezegun import freeze_time
import pytest

from components.days import Days
from db import db
from test.fixtures import simple


def setup_function():
    db._env = 'test'
    db.ensure_connected()


def test_days_class_instantiates():
    d = Days()
    assert d is not None
    assert 'Empty' in d.__repr__()
    assert 'Empty' in str(d)


def test_open_is_none_when_no_dates():
    assert Days().open(date.today()) is None


def test_open(simple):
    with freeze_time(dt(2017, 3, 7)):
        d = Days()

    assert d.open(date(2017, 3, 1)) is None
    assert d.open(date(2017, 3, 2))
    assert not d.open(date(2017, 3, 4))
    assert d.open(date(2017, 3, 6))
    assert d.open(date(2017, 3, 7)) is None
