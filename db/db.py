import pandas as pd
import psycopg2
from psycopg2.extras import NamedTupleCursor
from sqlalchemy import create_engine

import config


conn = None
_env = "dev"


def connect(env=_env):
    """Connect to finance database.

    The connection becomes available on the `conn` singleton variable.
    Subsequent calls to `connect()` release previous connections and reconnect.

    Keyword arguments:
    env -- environment to connect to.
           Must be a key in the `config.db` dict (default 'dev')

    Returns:
    bool -- True if the connection is alive
    """
    global _env
    _env = env

    global conn
    engine = create_engine(
        f"postgresql://{config.db[env]['user']}@localhost/{config.db[env]['db']}",
        execution_options={"isolation_level": "AUTOCOMMIT"},
    )
    conn = engine.connect().execution_options(autocommit=True)

    return is_alive()


def ensure_connected(env=None):
    """Check if there is a database connection, and connect if there is not.

    This module does not allow switching between connections to different
    environments.

    If there is an existing connection, and `ensure_connected` is called
    with a different environment parameter than the one we are connected to,
    an exception will be raised.
    """
    global _env
    if env is None:
        env = _env

    if conn is not None and env != _env:
        raise Exception("Already connected to a different environment.")

    _env = env
    if is_alive():
        return True
    else:
        return connect(_env)


def is_alive():
    """Report whether the database connection is alive."""
    global conn
    return conn is not None and not conn.closed


def df_from_sql(sql, params, index_col, parse_dates):
    """Return a dataframe from a SQL query.

    Return a dataframe from a sql query, given the parameters provided.
    This function just wraps around pandas.read_sql_query, but it is useful because
    other components may use this without exposing the database connection to them.
    """
    ensure_connected()
    return pd.read_sql_query(
        sql=sql, con=conn, params=params, index_col=index_col, parse_dates=parse_dates
    )
