from datetime import datetime as dt, date
from freezegun import freeze_time
import pytest

from components.deposits import Deposits
from db import db
from db.data import Data


class TestDeposits:
    def setup(self):
        db._env = "test"
        self._data = Data()

    def test_deposits_class_instantiates(self):
        d = Deposits(data=self._data)

        assert d is not None
        assert "Empty" in d.__repr__()
        assert "Empty" in str(d)

    @pytest.mark.usefixtures("simple")
    def test_deposits_class_holds_deposit_values(self):
        with freeze_time(dt(2017, 3, 7)):
            d = Deposits(data=self._data)

        assert len(d.deposits) == 1
        assert d.deposits.loc[date(2017, 3, 2)]["amount"] == 10000
        assert not d.amount(date(2017, 3, 1))
        assert d.amount(date(2017, 3, 2)) == 10000
        assert not d.amount(date(2017, 3, 3))
