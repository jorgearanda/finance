from decimal import Decimal
from nose.tools import assert_almost_equals, assert_raises
from util.cagr import cagr


def test_cagr_missing_period_raises_exception():
    assert_raises(Exception, cagr, 1000, 1000)


def test_cagr_zero_initial_value_raises_exception():
    assert_raises(Exception, cagr, 0, 1000, years=1)


def test_cagr_instant():
    assert cagr(1000, 1000, days=0) is None


def test_cagr_zero():
    assert cagr(1000, 1000, years=1) == Decimal(0)


def test_cagr_positive():
    assert_almost_equals(cagr(1000, 1100, years=1), Decimal(0.1))


def test_cagr_negative():
    assert_almost_equals(cagr(1000, 900, years=1), Decimal(-0.1))


def test_cagr_with_months():
    assert cagr(1000, 2000, months=12) == Decimal(1.0)


def test_cagr_with_days():
    assert cagr(1000, 2000, days=365) == Decimal(1.0)


def test_cagr_with_days():
    assert cagr(1000, 2000, days=365) == Decimal(1.0)


def test_cagr_part_of_a_year():
    assert_almost_equals(cagr(1000, 1100, years=0.5), Decimal(0.21))
    assert_almost_equals(cagr(1000, 1100, months=6), Decimal(0.21))


def test_cagr_many_years():
    assert_almost_equals(cagr(1000, 2593.7424601, years=10), Decimal(0.1))
    assert_almost_equals(cagr(1000, 2593.7424601, months=120), Decimal(0.1))
    assert_almost_equals(cagr(1000, 2593.7424601, days=3650), Decimal(0.1))
