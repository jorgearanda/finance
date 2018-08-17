import pytest

from db import db
from snapshot import snapshot

s = None


def load_snapshot():
    global s
    if s is None:
        s = snapshot({
            '--accounts': None,
            '--update': False,
            '--verbose': False,
            '--positions': True
        })


@pytest.mark.usefixtures('simple')
class TestSimpleSnapshot():
    def setup(self):
        load_snapshot()

    def test_smoke(self):
        assert True

    def test_title(self):
        assert 'Portfolio Snapshot' in s

    def test_total_value(self):
        assert 'Total Value:     10,324.30' in s

    def test_profit(self):
        assert 'Profit:             324.30' in s

    def test_month_profit(self):
        found = ['Month Profit:       324.30' in line for line in s]
        assert True in found

    def test_month_returns(self):
        found = ['Month Returns:        3.24%' in line for line in s]
        assert True in found
