from datetime import datetime as dt, date
from freezegun import freeze_time

from components.deposits import Deposits
from db import db
from test.fixtures import simple


def setup_function():
    db._env = 'test'
    db.ensure_connected()


def test_deposits_class_instantiates():
    d = Deposits()
    assert d is not None
    assert not d.account
    assert 'Empty' in d.__repr__()
    assert 'Empty' in str(d)


def test_deposits_class_holds_deposit_values(simple):
    with freeze_time(dt(2017, 3, 7)):
        d = Deposits()

    assert len(d.deposits) == 1
    assert d.deposits.loc[date(2017, 3, 2)]['amount'] == 10000
