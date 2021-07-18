from datetime import date
import pandas as pd
from unittest.mock import patch

from components.deposits import Deposits
from db.data import Data


empty = pd.DataFrame()
two_deposits = pd.DataFrame(
    data={"day": [date(2017, 1, 1), date(2017, 2, 1)], "amount": [10000, 5000]}
).set_index("day")


class TestDeposits:
    @patch.object(Data, "df_from_sql", return_value=empty)
    def test_create(self, data_call):
        d = Deposits(data=Data())

        assert d is not None
        assert "Empty" in str(d)

    @patch.object(Data, "df_from_sql", return_value=two_deposits)
    def test_query_default_values(self, data_call):
        d = Deposits(data=Data())
        _, kwargs = data_call.call_args

        assert len(d.deposits) == 2
        assert d.deposits.loc[date(2017, 1, 1)]["amount"] == 10000
        assert kwargs["params"]["accounts"] == []
        assert kwargs["params"]["from_day"] is None
        assert kwargs["params"]["today"] == date.today()

    @patch.object(Data, "df_from_sql", return_value=two_deposits)
    def test_query_custom_values(self, data_call):
        d = Deposits(accounts=["Account1"], from_day=date(2017, 1, 1), data=Data())
        _, kwargs = data_call.call_args

        assert kwargs["params"]["accounts"] == ["Account1"]
        assert kwargs["params"]["from_day"] == date(2017, 1, 1)
        assert kwargs["params"]["today"] == date.today()

    @patch.object(Data, "df_from_sql", return_value=two_deposits)
    def test_amount(self, data_call):
        d = Deposits(data=Data())

        assert d.amount(date(2016, 12, 31)) == 0  # out of range
        assert d.amount(date(2017, 1, 3)) == 0
        assert d.amount(date(2017, 2, 1)) == 5000
