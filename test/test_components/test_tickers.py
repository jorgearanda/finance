from datetime import datetime as dt
from freezegun import freeze_time
import pandas as pd
from pytest import approx

from components.tickers import Tickers
from db import db
from test.fixtures import simple


def setup_function():
    db._env = 'test'
    db.ensure_connected()


def test_tickers_class_instantiates():
    t = Tickers()
    assert t is not None
    assert t.__repr__() == '{}'
    assert str(t) == '{}'


def test_loads_with_data(simple):
    with freeze_time(dt(2017, 3, 7)):
        t = Tickers()

    assert len(t.ticker_names) == 2
    assert t.ticker_names[0] == 'VCN.TO'
    assert t.ticker_names[1] == 'VEE.TO'

    assert len(t.tickers) == 2

    assert t.prices.columns[0] == 'VCN.TO'
    assert t.prices.columns[1] == 'VEE.TO'
    assert t.prices.loc['2017-03-02']['VCN.TO'] == approx(30.00)
    assert t.changes.loc['2017-03-03']['VCN.TO'] == approx(30.10 / 30.00 - 1)
    assert t.changes_from_start.loc['2017-03-06']['VCN.TO'] == approx(29.85 / 30.00 - 1)
    assert t.yields_from_start.loc['2017-03-06']['VCN.TO'] == approx(0.02 / 3.0)
    assert t.volatilities['VEE.TO'] == t.tickers['VEE.TO'].volatility
    assert t.correlations['VCN.TO']['VCN.TO'] == approx(1.00)
    assert t.correlations['VCN.TO']['VEE.TO'] == approx(-0.988212)

    assert t.price('2017-03-02', 'VCN.TO') == approx(30.00)
