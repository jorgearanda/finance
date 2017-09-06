from datetime import datetime as dt, date
from freezegun import freeze_time

from components.portfolio import Portfolio
from db import db
from test.fixtures import simple


def setup_function():
    db._env = 'test'
    db.ensure_connected()


def test_portfolio_class_instantiates():
    p = Portfolio()
    assert p is not None
    assert not p.account
    assert not p.from_day


def test_portfolio_class_holds_portfolio_structures(simple):
    with freeze_time(dt(2017, 3, 7)):
        p = Portfolio()

    assert p.deposits
    assert p.tickers
    assert p.positions
