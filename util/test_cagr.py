from decimal import Decimal
import pytest
from util.cagr import cagr


def test_cagr_missing_period_raises_exception():
    with pytest.raises(Exception):
        cagr(1000, 1000)


def test_cagr_zero_initial_value_raises_exception():
    with pytest.raises(Exception):
        cagr(0, 1000, years=1)


def test_cagr_instant():
    assert cagr(1000, 1000, days=0) is None


def test_cagr_zero():
    assert cagr(1000, 1000, years=1) == Decimal(0)


def test_cagr_positive():
    pytest.approx(cagr(1000, 1100, years=1), Decimal(0.1))


def test_cagr_negative():
    pytest.approx(cagr(1000, 900, years=1), Decimal(-0.1))


def test_cagr_with_months():
    assert cagr(1000, 2000, months=12) == Decimal(1.0)


def test_cagr_with_days():
    assert cagr(1000, 2000, days=365) == Decimal(1.0)


def test_cagr_with_days():
    assert cagr(1000, 2000, days=365) == Decimal(1.0)


def test_cagr_part_of_a_year():
    pytest.approx(cagr(1000, 1100, years=0.5), Decimal(0.21))
    pytest.approx(cagr(1000, 1100, months=6), Decimal(0.21))


def test_cagr_many_years():
    pytest.approx(cagr(1000, 2593.7424601, years=10), Decimal(0.1))
    pytest.approx(cagr(1000, 2593.7424601, months=120), Decimal(0.1))
    pytest.approx(cagr(1000, 2593.7424601, days=3650), Decimal(0.1))
