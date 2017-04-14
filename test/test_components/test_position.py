from datetime import datetime as dt, date
from freezegun import freeze_time
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