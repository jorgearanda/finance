from datetime import datetime as dt, date
from freezegun import freeze_time
import math
import pandas as pd
from pytest import approx

from components.position import Position
from db import db
from test.fixtures import simple


def setup_function():
    db._env = 'test'
    db.ensure_connected()


def test_position_class_instantiates():
    vcn = Position('VCN.TO')
    assert vcn is not None
    assert vcn.ticker_name == 'VCN.TO'
    assert 'Empty' in vcn.__repr__()
    assert 'Empty' in str(vcn)


def test_position_class_instantiates_with_data(simple):
    with freeze_time(dt(2017, 3, 7)):
        vcn = Position('VCN.TO')

    assert vcn is not None
    assert vcn.ticker_name == 'VCN.TO'
    assert 'total_returns' in vcn.__repr__()
    assert 'total_returns' in str(vcn)


def test_values(simple):
    with freeze_time(dt(2017, 3, 7)):
        vcn = Position('VCN.TO')

    assert vcn.units(date(2017, 3, 1)) is None
    assert vcn.units(date(2017, 3, 2)) == 0
    assert vcn.units(date(2017, 3, 3)) == 100
    assert vcn.units(date(2017, 3, 4)) == 100
    assert vcn.units(date(2017, 3, 7)) is None

    assert vcn.cost(date(2017, 3, 1)) is None
    assert vcn.cost(date(2017, 3, 2)) == 0
    assert vcn.cost(date(2017, 3, 3)) == 3010.35
    assert vcn.cost(date(2017, 3, 4)) == 3010.35
    assert vcn.cost(date(2017, 3, 7)) is None

    assert vcn.cost_per_unit(date(2017, 3, 1)) is None
    assert math.isnan(vcn.cost_per_unit(date(2017, 3, 2)))
    assert vcn.cost_per_unit(date(2017, 3, 3)) == 30.1035
    assert vcn.cost_per_unit(date(2017, 3, 4)) == 30.1035
    assert vcn.cost_per_unit(date(2017, 3, 7)) is None

    assert vcn.current_price(date(2017, 3, 1)) is None
    assert vcn.current_price(date(2017, 3, 2)) == 30.00
    assert vcn.current_price(date(2017, 3, 3)) == 30.10
    assert vcn.current_price(date(2017, 3, 6)) == 29.85
    assert vcn.current_price(date(2017, 3, 7)) is None

    assert vcn.market_value(date(2017, 3, 1)) is None
    assert vcn.market_value(date(2017, 3, 2)) == 0
    assert vcn.market_value(date(2017, 3, 3)) == 3010.0
    assert vcn.market_value(date(2017, 3, 6)) == 2985.0
    assert vcn.market_value(date(2017, 3, 7)) is None

    assert vcn.open_profit(date(2017, 3, 1)) is None
    assert vcn.open_profit(date(2017, 3, 2)) == 0
    assert approx(vcn.open_profit(date(2017, 3, 3)), -0.35)
    assert approx(vcn.open_profit(date(2017, 3, 6)), -25.35)
    assert vcn.open_profit(date(2017, 3, 7)) is None

    assert vcn.distributions(date(2017, 3, 1)) is None
    assert vcn.distributions(date(2017, 3, 2)) == 0
    assert vcn.distributions(date(2017, 3, 3)) == 10.1
    assert vcn.distributions(date(2017, 3, 6)) == 20.0
    assert vcn.distributions(date(2017, 3, 7)) is None

    assert vcn.distribution_returns(date(2017, 3, 1)) is None
    assert math.isnan(vcn.distribution_returns(date(2017, 3, 2)))
    assert vcn.distribution_returns(date(2017, 3, 3)) == 10.1 / 3010.35
    assert vcn.distribution_returns(date(2017, 3, 6)) == 20.0 / 3010.35
    assert vcn.distribution_returns(date(2017, 3, 7)) is None

    assert vcn.appreciation_returns(date(2017, 3, 1)) is None
    assert math.isnan(vcn.appreciation_returns(date(2017, 3, 2)))
    assert vcn.appreciation_returns(date(2017, 3, 3)) == (3010.0 - 3010.35) / 3010.35
    assert vcn.appreciation_returns(date(2017, 3, 6)) == (2985.0 - 3010.35) / 3010.35
    assert vcn.appreciation_returns(date(2017, 3, 7)) is None

    assert vcn.total_returns(date(2017, 3, 1)) is None
    assert vcn.total_returns(date(2017, 3, 2)) == 0
    assert approx(vcn.total_returns(date(2017, 3, 3)), (3010.0 + 10.1 - 3010.35) / 3010.35)
    assert approx(vcn.total_returns(date(2017, 3, 6)), (2985.0 + 20.0 - 3010.35) / 3010.35)
    assert vcn.total_returns(date(2017, 3, 7)) is None
