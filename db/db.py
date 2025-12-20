import pandas as pd
import sqlite3

from datetime import date, datetime
from pathlib import Path
from sqlalchemy import create_engine, text

import config


conn = None
engine = None
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
    global _env, conn, engine
    _env = env

    if conn is not None:
        conn.close()
        conn = None
    if engine is not None:
        engine.dispose()
        engine = None

    db_config = config.db[env]
    db_path = db_config["path"]

    # Test databases are in :memory: and have no file
    if db_path != ":memory:":
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    def adapt_date_iso(val):
        return val.isoformat()

    def adapt_datetime_iso(val):
        return val.isoformat()

    def convert_date(val):
        return date.fromisoformat(val.decode())

    def convert_datetime(val):
        return datetime.fromisoformat(val.decode())

    sqlite3.register_adapter(date, adapt_date_iso)
    sqlite3.register_adapter(datetime, adapt_datetime_iso)
    sqlite3.register_converter("date", convert_date)
    sqlite3.register_converter("timestamp", convert_datetime)

    new_engine = create_engine(
        f"sqlite:///{db_path}",
        execution_options={"isolation_level": "AUTOCOMMIT"},
        connect_args={"detect_types": sqlite3.PARSE_DECLTYPES},
    )

    engine = new_engine
    conn = engine.connect().execution_options(autocommit=True)
    return is_alive()


def ensure_connected(env=None):
    """Check if there is a database connection, and connect if there is not."""
    global _env
    if env is None:
        env = _env

    if conn is not None and env != _env:
        raise ValueError("Already connected to a different environment.")

    _env = env
    return True if is_alive() else connect(_env)


def is_alive():
    """Report whether the database connection is alive."""
    global conn
    return conn is not None and not conn.closed


def df_from_sql(sql, params, index_col, parse_dates, bindparams=None):
    """Return a dataframe from a SQL query.

    Return a dataframe from a sql query, given the parameters provided.
    This function just wraps around pandas.read_sql_query, but it is useful because
    other components may use this without exposing the database connection to them.
    """
    ensure_connected()
    sql_text = text(sql)
    if bindparams:
        sql_text = sql_text.bindparams(*bindparams)

    return pd.read_sql_query(
        sql=sql_text,
        con=conn,
        params=params,
        index_col=index_col,
        parse_dates=parse_dates,
    )
