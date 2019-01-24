import pandas as pd

from db import db


class Data:
    """Data access intermediary.

    Provide an intermediary to query the database.
    While the database can be queried directly using db.conn,
    this intermediary can be passed on to component classes instead of
    direct access to the database connection, and this indirection
    helps with testing and improving modularity.
    """

    def __init__(self):
        db.ensure_connected()
        self._conn = db.conn

    def df_from_sql(self, sql, params, index_col, parse_dates):
        """Return a dataframe from a SQL query.

        Return a dataframe from a sql query, given the parameters provided.
        This function just wraps around pandas.read_sql_query, but it is useful because
        other components may use this without exposing the database connection to them.
        """
        return pd.read_sql_query(
            sql=sql,
            con=self._conn,
            params=params,
            index_col=index_col,
            parse_dates=parse_dates,
        )
