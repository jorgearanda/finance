from datetime import datetime as dt, date
from freezegun import freeze_time
from pytest import approx

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


def test_portfolio_class_calculations(simple):
    with freeze_time(dt(2017, 3, 7)):
        p = Portfolio()

    assert len(p.by_day) == 5

    assert p.by_day.ix['2017-03-02']['days_from_start'] == 1
    assert p.by_day.ix['2017-03-06']['days_from_start'] == 5

    assert p.by_day.ix['2017-03-02']['years_from_start'] == approx(1 / 365)
    assert p.by_day.ix['2017-03-06']['years_from_start'] == approx(5 / 365)

    assert p.by_day.ix['2017-03-02']['market_day']
    assert not p.by_day.ix['2017-03-04']['market_day']
    assert p.by_day.ix['2017-03-06']['market_day']

    assert p.by_day.ix['2017-03-02']['day_deposits'] == 10000
    assert p.by_day.ix['2017-03-03']['day_deposits'] == 0

    assert p.by_day.ix['2017-03-02']['capital'] == 10000
    assert p.by_day.ix['2017-03-06']['capital'] == 10000

    assert p.by_day.ix['2017-03-02']['avg_capital'] == 10000
    assert p.by_day.ix['2017-03-06']['avg_capital'] == 10000

    assert p.by_day.ix['2017-03-02']['positions_cost'] == 0
    assert p.by_day.ix['2017-03-06']['positions_cost'] == approx(5810.70)

    assert p.by_day.ix['2017-03-02']['positions_value'] == 0
    assert p.by_day.ix['2017-03-06']['positions_value'] == approx(6185)

    assert p.by_day.ix['2017-03-02']['appreciation'] == 0
    assert p.by_day.ix['2017-03-06']['appreciation'] == approx(6185 - 5810.70)

    assert p.by_day.ix['2017-03-02']['dividends'] == 0
    assert p.by_day.ix['2017-03-03']['dividends'] == approx(10.10)
    assert p.by_day.ix['2017-03-06']['dividends'] == approx(20)

    assert p.by_day.ix['2017-03-02']['cash'] == 10000
    assert p.by_day.ix['2017-03-06']['cash'] == approx(10000 - 5810.70 + 20)

    assert p.by_day.ix['2017-03-02']['total_value'] == 10000
    assert p.by_day.ix['2017-03-06']['total_value'] == approx(10000 + 6185 - 5810.70 + 20)
