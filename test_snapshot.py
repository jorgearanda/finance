import pytest

from conftest import simple_fixture, simple_fixture_teardown
from db import db
from snapshot import snapshot

s = None


def load_snapshot():
    global s
    if s is None:
        s = snapshot(
            {
                "--accounts": None,
                "--update": False,
                "--verbose": False,
                "--positions": True,
                "--recent": True,
                "--months": True,
                "--years": True,
            }
        )


class TestSimpleSnapshot:
    def setup(self):
        simple_fixture()
        load_snapshot()

    def teardown(self):
        simple_fixture_teardown()

    def test_smoke(self):
        assert True

    def test_title(self):
        assert "Portfolio Snapshot" in s

    def test_total_value(self):
        assert "Total Value:     10,324.30" in s

    def test_profit(self):
        assert "Profit:             324.30" in s

    def test_month_profit(self):
        found = ["Month Profit:       324.30" in line for line in s]
        assert True in found

    def test_month_returns(self):
        found = ["Month TWRR:           3.24%" in line for line in s]
        assert True in found

    def test_year_profit(self):
        found = ["Year Profit:        324.30" in line for line in s]
        assert True in found

    def test_year_returns(self):
        found = ["Year TWRR:            3.24%" in line for line in s]
        assert True in found
