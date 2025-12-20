from datetime import datetime
import pytest
from sqlalchemy import bindparam, text

from conftest import simple_fixture, simple_fixture_teardown
from db import db


class TestConnectivity:
    def setup_method(self):
        db.conn = None
        db._env = "test"

    def test_db_connects(self):
        assert db.connect("test")
        assert db.is_alive()

    def test_db_does_not_connect_to_unknown_env(self):
        with pytest.raises(KeyError):
            db.connect("broken")

        assert db.is_alive() is False

    def test_ensure_connected_connects(self):
        assert db.ensure_connected()
        assert db.is_alive()

    def test_ensure_connected_does_not_reconnect_pointlessly(self):
        db.ensure_connected()
        conn1 = db.conn
        db.ensure_connected()
        conn2 = db.conn

        assert conn1 == conn2

    def test_no_switching_db_environments(self):
        db.ensure_connected()

        assert db._env == "test"

        with pytest.raises(Exception):
            db.ensure_connected("diff")

        assert db._env == "test"
        assert db.is_alive()


class TestDfFromSql:
    def setup_method(self):
        simple_fixture()

    def teardown_method(self):
        pass

    def test_query(self):
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

        df = db.df_from_sql(
            sql, params, index_col, parse_dates,
            bindparams=[bindparam("accounts", expanding=True)]
        )

        assert len(df) == 1
        assert df.loc[datetime(2017, 3, 2)]["amount"] == 10000
