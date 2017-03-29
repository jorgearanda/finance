import pytest

from db import db


def setup_function():
    db.conn = None
    db._env = 'test'


def test_db_connects():
    assert db.connect('test')
    assert db.is_alive()


def test_db_does_not_connect_to_unknown_env():
    with pytest.raises(KeyError):
        db.connect('broken')

    assert db.is_alive() is False


def test_ensure_connected_connects():
    assert db.ensure_connected()
    assert db.is_alive()


def test_ensure_connected_does_not_reconnect_pointlessly():
    db.ensure_connected()
    conn1 = db.conn
    db.ensure_connected()
    conn2 = db.conn

    assert conn1 == conn2
