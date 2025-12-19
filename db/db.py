import pandas as pd
from sqlalchemy import bindparam, create_engine, text

import config


conn = None
_env = "dev"


def connect(env=_env):
    """Connect to finance database (PostgreSQL or SQLite).

    The connection becomes available on the `conn` singleton variable.
    Subsequent calls to `connect()` release previous connections and reconnect.

    Keyword arguments:
    env -- environment to connect to.
           Must be a key in the `config.db` dict (default 'dev')

    Returns:
    bool -- True if the connection is alive
    """
    global _env, conn
    _env = env

    db_config = config.db[env]

    if db_config.get("type") == "sqlite":
        # SQLite connection
        from pathlib import Path
        db_path = db_config["path"]
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)

        engine = create_engine(
            f"sqlite:///{db_path}",
            execution_options={"isolation_level": "AUTOCOMMIT"},
        )
    else:
        # PostgreSQL connection (existing)
        engine = create_engine(
            f"postgresql://{db_config['user']}@localhost/{db_config['db']}",
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
        sql=sql_text, con=conn, params=params, index_col=index_col, parse_dates=parse_dates
    )
