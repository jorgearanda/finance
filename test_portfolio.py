from datetime import datetime as dt, date
from freezegun import freeze_time
import math
from pandas import Timestamp
import pytest
from pytest import approx

from conftest import simple_fixture, simple_fixture_teardown
from portfolio import Portfolio
from db import db

p = None


def load_portfolio():
    global p
    if p is None:
        with freeze_time(dt(2017, 3, 7)):
            p = Portfolio(update=False)


# @pytest.mark.usefixtures("simple")  # has data starting on 2017-03-02
class TestSimplePortfolio:
    def setup_method(self):
        global p
        p = None  # Reset the global portfolio for each test
        simple_fixture()
        load_portfolio()

    def teardown_method(self):
        simple_fixture_teardown()

    def test_smoke(self):
        assert p.deposits
        assert p.tickers
        assert p.positions
        assert p.by_day is not None
        assert p.by_month is not None
        assert p.by_year is not None
        assert len(p.by_day) == 6

    def test_days_from_start(self):
        assert p.val("days_from_start", "2017-03-02") == 1
        assert p.val("days_from_start", "2017-03-06") == 5

    def test_years_from_start(self):
        assert p.val("years_from_start", "2017-03-02") == approx(1 / 365)
        assert p.val("years_from_start", "2017-03-06") == approx(5 / 365)

    def test_market_day(self):
        assert p.val("market_day", "2017-03-02")
        assert not p.val("market_day", "2017-03-04")
        assert p.val("market_day", "2017-03-06")

    def test_day_deposits(self):
        assert p.val("day_deposits", "2017-03-02") == 10000
        assert p.val("day_deposits", "2017-03-06") == 0

    def test_capital(self):
        assert p.val("capital", "2017-03-02") == 10000
        assert p.val("capital", "2017-03-06") == 10000

    def test_avg_capital(self):
        assert p.val("avg_capital", "2017-03-02") == 10000
        assert p.val("avg_capital", "2017-03-06") == 10000

    def test_positions_cost(self):
        assert p.val("positions_cost", "2017-03-02") == 0
        assert p.val("positions_cost", "2017-03-06") == approx(5810.70)

    def test_positions_value(self):
        assert p.val("positions_value", "2017-03-02") == 0
        assert p.val("positions_value", "2017-03-06") == approx(6185.00)

    def test_appreciation(self):
        assert p.val("appreciation", "2017-03-02") == 0
        assert p.val("appreciation", "2017-03-06") == approx(6185.00 - 5810.70)

    def test_dividends(self):
        assert p.val("dividends", "2017-03-02") == 0
        assert p.val("dividends", "2017-03-03") == approx(10.10)
        assert p.val("dividends", "2017-03-06") == approx(20.00)

    def test_cash(self):
        assert p.val("cash", "2017-03-02") == 10000
        assert p.val("cash", "2017-03-06") == approx(10000 - 5810.70 + 20)

    def test_total_value(self):
        assert p.val("total_value", "2017-03-02") == 10000
        assert p.val("total_value", "2017-03-06") == approx(10394.30)

    def test_day_profit(self):
        assert p.val("day_profit", "2017-03-02") == 0
        assert p.val("day_profit", "2017-03-06") == approx(384.90)

    def test_day_returns(self):
        assert p.val("day_returns", "2017-03-02") == 0
        assert p.val("day_returns", "2017-03-06") == approx(0.038453853)

    def test_profit(self):
        assert p.val("profit", "2017-03-02") == 0
        assert p.val("profit", "2017-03-06") == approx(6185 - 5810.70 + 20)

    def test_appreciation_returns(self):
        assert p.val("appreciation_returns", "2017-03-02") == 0
        assert p.val("appreciation_returns", "2017-03-06") == approx(0.03743)

    def test_distribution_returns(self):
        assert p.val("distribution_returns", "2017-03-02") == 0
        assert p.val("distribution_returns", "2017-03-06") == approx(0.002)

    def test_returns(self):
        assert p.val("returns", "2017-03-02") == 0
        assert p.val("returns", "2017-03-06") == approx(0.03743 + 0.002)

    def test_twrr(self):
        assert p.val("twrr", "2017-03-02") == 0
        assert p.val("twrr", "2017-03-06") == approx(0.03943)

    def test_twrr_annualized(self):
        # Same as twrr when under a year
        assert p.val("twrr_annualized", "2017-03-02") == 0
        assert p.val("twrr_annualized", "2017-03-06") == approx(0.03943)

    def test_mwrr(self):
        assert p.val("mwrr", "2017-03-02") == 0
        assert p.val("mwrr", "2017-03-06") == approx(0.03943)

    def test_mwrr_annualized(self):
        # Same as mwrr when under a year
        assert p.val("mwrr_annualized", "2017-03-02") == 0
        assert p.val("mwrr_annualized", "2017-03-06") == approx(0.03943)

    def test_volatility(self):
        assert math.isnan(p.val("volatility", "2017-03-02"))
        assert not math.isnan(p.val("volatility", "2017-03-04"))
        assert p.val("volatility", "2017-03-06") == approx(0.0219350238)

    def test_10k_equivalent(self):
        assert p.val("10k_equivalent", "2017-03-02") == 10000
        assert p.val("10k_equivalent", "2017-03-06") == approx(10394.30)

    def test_last_peak_twrr(self):
        assert p.val("last_peak_twrr", "2017-03-02") == 0
        assert p.val("last_peak_twrr", "2017-03-06") == approx(0.03943)

    def test_last_peak(self):
        assert p.val("last_peak", "2017-03-02") == Timestamp("2017-03-02")
        assert p.val("last_peak", "2017-03-06") == Timestamp("2017-03-06")

    def test_current_drawdown(self):
        assert p.val("current_drawdown", "2017-03-02") == 0
        assert p.val("current_drawdown", "2017-03-06") == 0

    def test_greatest_drawdown(self):
        assert p.val("greatest_drawdown", "2017-03-02") == 0
        assert p.val("greatest_drawdown", "2017-03-06") == 0

    def test_sharpe(self):
        assert math.isnan(p.val("sharpe", "2017-03-02"))
        assert p.val("sharpe", "2017-03-06") == approx(1.7869651525257371)

    def test_latest(self):
        latest = p.latest()

        assert latest["days_from_start"] == 6
        assert latest["total_value"] == approx(10394.30)

    def test_allocations(self):
        allocations = p.allocations()

        assert allocations["Cash"] == approx(0.40496233512598245)
