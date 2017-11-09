from datetime import datetime as dt
from freezegun import freeze_time
import pandas as pd
from pytest import approx

from components.positions import Positions
from db import db
from test.fixtures import simple


def setup_function():
    db._env = 'test'
    db.ensure_connected()


def test_positions_class_instantiates():
    p = Positions()
    assert p is not None
    assert p.__repr__() == '{}'
    assert str(p) == '{}'


def test_loads_with_data(simple):
    with freeze_time(dt(2017, 3, 7)):
        p = Positions()

    assert len(p.ticker_names) == 2
    assert p.ticker_names[0] == 'VCN.TO'
    assert p.ticker_names[1] == 'VEE.TO'

    assert len(p.positions) == 2

    assert p.units.columns[0] == 'VCN.TO'
    assert p.units.columns[1] == 'VEE.TO'
    assert p.units.loc['2017-03-02']['VCN.TO'] == approx(0)
    assert p.units.loc['2017-03-03']['VCN.TO'] == approx(100)
    assert p.costs.loc['2017-03-03']['VCN.TO'] == approx(3010.35)
    assert p.costs_per_unit.loc['2017-03-03']['VCN.TO'] == approx(30.1035)
    assert p.current_prices.loc['2017-03-03']['VCN.TO'] == approx(30.10)
    assert p.market_values.loc['2017-03-03']['VCN.TO'] == approx(3010)
    assert p.open_profits.loc['2017-03-03']['VCN.TO'] == approx(-0.35)
    assert p.distributions.loc['2017-03-03']['VCN.TO'] == approx(10.1)
    assert p.distribution_returns.loc['2017-03-03']['VCN.TO'] == approx(10.1 / 3010.35)
    assert p.appreciation_returns.loc['2017-03-03']['VCN.TO'] == approx((3010.0 - 3010.35) / 3010.35)
    assert p.total_returns.loc['2017-03-03']['VCN.TO'] == approx((3010.0 + 10.1 - 3010.35) / 3010.35)
