import psycopg2
from psycopg2.extras import NamedTupleCursor

import config


conn = None
_env = 'dev'


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
    conn = psycopg2.connect(
        database=config.db[env]['db'],
        user=config.db[env]['user'],
        cursor_factory=NamedTupleCursor)
    conn.autocommit = True

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
        raise Exception('Already connected to a different environment.')

    _env = env
    if is_alive():
        return True
    else:
        return connect(_env)


def is_alive():
    """Report whether the database connection is alive."""
    global conn
    return conn is not None and not conn.closed
