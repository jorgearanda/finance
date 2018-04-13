from datetime import datetime as dt, date
from freezegun import freeze_time
import math
from pandas import Timestamp
from pytest import approx

from portfolio import Portfolio
from db import db
from test.fixtures import simple


def setup_function():
    db._env = 'test'
    db.ensure_connected()


def test_portfolio_class_instantiates():
    p = Portfolio(update=False)
    assert p is not None
    assert not p.account
    assert not p.from_day


def test_portfolio_class_holds_portfolio_structures(simple):
    with freeze_time(dt(2017, 3, 7)):
        p = Portfolio(update=False)

    assert p.deposits
    assert p.tickers
    assert p.positions


def test_portfolio_class_calculations(simple):
    with freeze_time(dt(2017, 3, 7)):
        p = Portfolio(update=False)

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

    assert p.by_day.ix['2017-03-02']['day_profit'] == 0
    assert p.by_day.ix['2017-03-06']['day_profit'] == approx(384.90)

    assert p.by_day.ix['2017-03-02']['day_returns'] == 0
    assert p.by_day.ix['2017-03-06']['day_returns'] == approx(0.038453853)

    assert p.by_day.ix['2017-03-02']['profit'] == 0
    assert p.by_day.ix['2017-03-06']['profit'] == approx(6185 - 5810.70 + 20)

    assert p.by_day.ix['2017-03-02']['appreciation_returns'] == 0
    assert p.by_day.ix['2017-03-06']['appreciation_returns'] == approx((6185 - 5810.70) / 10000)

    assert p.by_day.ix['2017-03-02']['distribution_returns'] == 0
    assert p.by_day.ix['2017-03-06']['distribution_returns'] == approx(20 / 10000)

    assert p.by_day.ix['2017-03-02']['returns'] == 0
    assert p.by_day.ix['2017-03-06']['returns'] == approx((6185 - 5810.70 + 20) / 10000)

    assert p.by_day.ix['2017-03-02']['twrr'] == 0
    assert p.by_day.ix['2017-03-06']['twrr'] == approx((6185 - 5810.70 + 20) / 10000)

    assert p.by_day.ix['2017-03-02']['twrr_annualized'] == 0
    assert p.by_day.ix['2017-03-06']['twrr_annualized'] == approx(15.828796)

    assert p.by_day.ix['2017-03-02']['mwrr'] == 0
    assert p.by_day.ix['2017-03-06']['mwrr'] == approx((6185 - 5810.70 + 20) / 10000)

    assert p.by_day.ix['2017-03-02']['mwrr_annualized'] == 0
    assert p.by_day.ix['2017-03-06']['mwrr_annualized'] == approx(15.828796)

    assert math.isnan(p.by_day.ix['2017-03-02']['volatility'])
    assert not math.isnan(p.by_day.ix['2017-03-04']['volatility'])
    assert p.by_day.ix['2017-03-06']['volatility'] == approx(0.0219350238)

    assert p.by_day.ix['2017-03-02']['10k_equivalent'] == 10000
    assert p.by_day.ix['2017-03-06']['10k_equivalent'] == approx(10000 + 6185 - 5810.70 + 20)

    assert p.by_day.ix['2017-03-02']['last_peak_twrr'] == 0
    assert p.by_day.ix['2017-03-06']['last_peak_twrr'] == approx((6185 - 5810.70 + 20) / 10000)

    assert p.by_day.ix['2017-03-02']['last_peak'] == Timestamp('2017-03-02 00:00:00')
    assert p.by_day.ix['2017-03-06']['last_peak'] == Timestamp('2017-03-06 00:00:00')

    assert p.by_day.ix['2017-03-02']['current_drawdown'] == 0
    assert p.by_day.ix['2017-03-06']['current_drawdown'] == 0

    assert p.by_day.ix['2017-03-02']['greatest_drawdown'] == 0
    assert p.by_day.ix['2017-03-06']['greatest_drawdown'] == 0

    assert math.isnan(p.by_day.ix['2017-03-02']['sharpe'])
    assert p.by_day.ix['2017-03-06']['sharpe'] == approx(1.7869651525257371)


def test_latest(simple):
    with freeze_time(dt(2017, 3, 7)):
        p = Portfolio(update=False)
        latest = p.latest()

    assert latest['days_from_start'] == 5
    assert latest['total_value'] == approx(10000 + 6185 - 5810.70 + 20)


def test_allocations(simple):
    with freeze_time(dt(2017, 3, 7)):
        p = Portfolio(update=False)
        allocations = p.allocations()

    assert allocations['Cash'] == approx(0.40496233512598245)
