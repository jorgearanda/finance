from datetime import datetime as dt
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

    assert vcn.units('2017-03-01') is None
    assert vcn.units('2017-03-02') == 0
    assert vcn.units('2017-03-03') == 100
    assert vcn.units('2017-03-04') == 100
    assert vcn.units('2017-03-07') is None

    assert vcn.cost('2017-03-01') is None
    assert vcn.cost('2017-03-02') == 0
    assert vcn.cost('2017-03-03') == 3010.35
    assert vcn.cost('2017-03-04') == 3010.35
    assert vcn.cost('2017-03-07') is None

    assert vcn.cost_per_unit('2017-03-01') is None
    assert math.isnan(vcn.cost_per_unit('2017-03-02'))
    assert vcn.cost_per_unit('2017-03-03') == 30.1035
    assert vcn.cost_per_unit('2017-03-04') == 30.1035
    assert vcn.cost_per_unit('2017-03-07') is None

    assert vcn.current_price('2017-03-01') is None
    assert vcn.current_price('2017-03-02') == 30.00
    assert vcn.current_price('2017-03-03') == 30.10
    assert vcn.current_price('2017-03-06') == 29.85
    assert vcn.current_price('2017-03-07') is None

    assert vcn.market_value('2017-03-01') is None
    assert vcn.market_value('2017-03-02') == 0
    assert vcn.market_value('2017-03-03') == 3010.0
    assert vcn.market_value('2017-03-06') == 2985.0
    assert vcn.market_value('2017-03-07') is None

    assert vcn.open_profit('2017-03-01') is None
    assert vcn.open_profit('2017-03-02') == 0
    assert approx(vcn.open_profit('2017-03-03'), -0.35)
    assert approx(vcn.open_profit('2017-03-06'), -25.35)
    assert vcn.open_profit('2017-03-07') is None

    assert vcn.distributions('2017-03-01') is None
    assert vcn.distributions('2017-03-02') == 0
    assert vcn.distributions('2017-03-03') == 10.1
    assert vcn.distributions('2017-03-06') == 20.0
    assert vcn.distributions('2017-03-07') is None

    assert vcn.distribution_returns('2017-03-01') is None
    assert math.isnan(vcn.distribution_returns('2017-03-02'))
    assert vcn.distribution_returns('2017-03-03') == 10.1 / 3010.35
    assert vcn.distribution_returns('2017-03-06') == 20.0 / 3010.35
    assert vcn.distribution_returns('2017-03-07') is None

    assert vcn.appreciation_returns('2017-03-01') is None
    assert math.isnan(vcn.appreciation_returns('2017-03-02'))
    assert vcn.appreciation_returns('2017-03-03') == (3010.0 - 3010.35) / 3010.35
    assert vcn.appreciation_returns('2017-03-06') == (2985.0 - 3010.35) / 3010.35
    assert vcn.appreciation_returns('2017-03-07') is None

    assert vcn.total_returns('2017-03-01') is None
    assert vcn.total_returns('2017-03-02') == 0
    assert approx(vcn.total_returns('2017-03-03'), (3010.0 + 10.1 - 3010.35) / 3010.35)
    assert approx(vcn.total_returns('2017-03-06'), (2985.0 + 20.0 - 3010.35) / 3010.35)
    assert vcn.total_returns('2017-03-07') is None
