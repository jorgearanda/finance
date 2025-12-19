from datetime import datetime as dt, date
from freezegun import freeze_time
import pandas as pd
from pytest import approx

from conftest import simple_fixture, simple_fixture_teardown
from components.ticker import Ticker
from db import db
from db.data import Data


def setup_function():
    db._env = "test"
    db.ensure_connected()


def test_ticker_class_instantiates():
    vcn = Ticker("VCN.TO", data=Data())
    assert vcn is not None
    assert vcn.ticker_name == "VCN.TO"
    assert "Empty" in vcn.__repr__()
    assert "Empty" in str(vcn)


def test_price_does_not_crash_when_empty():
    assert Ticker("VCN.TO", data=Data()).price(date.today()) is None


def test_values():
    simple_fixture()
    with freeze_time(dt(2017, 3, 7)):
        vcn = Ticker("VCN.TO", data=Data())

    assert vcn.price("2017-03-01") is None
    assert vcn.price("2017-03-02") == approx(30.00)
    assert vcn.price("2017-03-03") == approx(30.10)
    assert vcn.price("2017-03-04") == approx(30.10)
    assert vcn.price("2017-03-05") == approx(30.10)
    assert vcn.price("2017-03-06") == approx(29.85)
    assert vcn.price("2017-03-08") is None

    assert vcn.change("2017-03-01") is None
    assert pd.isnull(vcn.change("2017-03-02"))  # Because the previous price is None
    assert vcn.change("2017-03-03") == approx(30.10 / 30.00 - 1)
    assert vcn.change("2017-03-04") == 0
    assert vcn.change("2017-03-05") == 0
    assert vcn.change("2017-03-06") == approx(29.85 / 30.10 - 1)

    assert vcn.change_from_start("2017-03-01") is None
    assert vcn.change_from_start("2017-03-02") == approx(0)
    assert vcn.change_from_start("2017-03-03") == approx(30.10 / 30.00 - 1)
    assert vcn.change_from_start("2017-03-04") == approx(30.10 / 30.00 - 1)
    assert vcn.change_from_start("2017-03-05") == approx(30.10 / 30.00 - 1)
    assert vcn.change_from_start("2017-03-06") == approx(29.85 / 30.00 - 1)

    assert vcn.distribution("2017-03-01") is None
    assert vcn.distribution("2017-03-02") == approx(0)
    assert vcn.distribution("2017-03-03") == approx(0.1010)
    assert vcn.distribution("2017-03-04") == approx(0)
    assert vcn.distribution("2017-03-05") == approx(0)
    assert vcn.distribution("2017-03-06") == approx(0.0990)

    assert vcn.distributions_from_start("2017-03-01") is None
    assert vcn.distributions_from_start("2017-03-02") == approx(0)
    assert vcn.distributions_from_start("2017-03-03") == approx(0.1010)
    assert vcn.distributions_from_start("2017-03-04") == approx(0.1010)
    assert vcn.distributions_from_start("2017-03-05") == approx(0.1010)
    assert vcn.distributions_from_start("2017-03-06") == approx(0.2)

    assert vcn.yield_from_start("2017-03-01") is None
    assert vcn.yield_from_start("2017-03-02") == approx(0)
    assert vcn.yield_from_start("2017-03-03") == approx(0.1010 / 30.00)
    assert vcn.yield_from_start("2017-03-04") == approx(0.1010 / 30.00)
    assert vcn.yield_from_start("2017-03-05") == approx(0.1010 / 30.00)
    assert vcn.yield_from_start("2017-03-06") == approx(0.2 / 30.00)

    assert vcn.returns("2017-03-01") is None
    assert vcn.returns("2017-03-02") == approx(0)
    assert vcn.returns("2017-03-03") == approx(
        vcn.change_from_start("2017-03-03") + vcn.yield_from_start("2017-03-03")
    )
    assert vcn.returns("2017-03-06") == approx(
        vcn.change_from_start("2017-03-06") + vcn.yield_from_start("2017-03-06")
    )

    assert vcn.volatility == approx(vcn.values[vcn.values.open == 1]["change"].std(axis=0))
    simple_fixture_teardown()
