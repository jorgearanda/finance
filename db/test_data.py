from datetime import datetime
import pytest
from sqlalchemy import bindparam

from conftest import simple_fixture, simple_fixture_teardown
from db.data import Data


class TestData:
    def setup_method(self):
        simple_fixture()

    def teardown_method(self):
        simple_fixture_teardown()

    def test_create(self):
        d = Data()

    def test_df_from_sql(self):
        d = Data()
        sql = """SELECT SUM(total) AS amount, day
            FROM transactions
            WHERE account IN :accounts
                AND (:from_day IS NULL OR day >= :from_day)
                AND day <= :today
                AND txtype = 'deposit'
            GROUP BY day
            ORDER BY day ASC;
        """
        params = {
            "accounts": ["RRSP1"],
            "from_day": None,
            "today": datetime(2017, 3, 7),
        }
        index_col = "day"
        parse_dates = ["day"]

        df = d.df_from_sql(
            sql, params, index_col, parse_dates,
            bindparams=[bindparam("accounts", expanding=True)]
        )

        assert len(df) == 1
        assert df.loc[datetime(2017, 3, 2)]["amount"] == 10000
