from datetime import datetime
import pytest

from db.data import Data


@pytest.mark.usefixtures("simple")
class TestData:
    def test_create(self):
        d = Data()

    def test_df_from_sql(self):
        d = Data()
        sql = """SELECT SUM(total)::double precision AS amount, day
            FROM transactions
            WHERE account = ANY(%(accounts)s)
                AND (%(from_day)s IS NULL OR day >= %(from_day)s)
                AND day <= %(today)s
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

        df = d.df_from_sql(sql, params, index_col, parse_dates)

        assert len(df) == 1
        assert df.loc[datetime(2017, 3, 2)]["amount"] == 10000
