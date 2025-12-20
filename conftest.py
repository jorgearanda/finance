import pytest
from sqlalchemy import text

from db import db


db._env = "test"


@pytest.fixture(autouse=True)
def setup_test_database():
    db.connect("test")
    _create_schema()

    yield

    if db.conn is not None:
        db.conn.close()
    if db.engine is not None:
        db.engine.dispose()


def _create_schema():
    with open("db/schemas.sql", "r") as f:
        schema_sql = f.read()

    raw_conn = db.conn.connection.driver_connection
    raw_conn.executescript(schema_sql)


def simple_fixture():
    db._env = "test"
    db.ensure_connected("test")
    db.conn.execute(
        text(
            """
        INSERT INTO accounttypes (name, tax, margin)
        VALUES ('RRSP', 'deferred', 0);"""
        )
    )
    db.conn.execute(
        text(
            """
        INSERT INTO investors (name)
        VALUES ('Someone');"""
        )
    )
    db.conn.execute(
        text(
            """
        INSERT INTO accounts (name, accounttype, investor, datecreated)
        VALUES ('RRSP1', 'RRSP', 'Someone', '2017-03-02');"""
        )
    )
    db.conn.execute(
        text(
            """
        INSERT INTO marketdays (day, open)
        VALUES
            ('2017-03-02', 1),
            ('2017-03-03', 1),
            ('2017-03-04', 0),
            ('2017-03-05', 0),
            ('2017-03-06', 1),
            ('2017-03-07', 1),
            ('2017-03-08', 1);"""
        )
    )
    db.conn.execute(
        text(
            """
        INSERT INTO assetclasses (name, domesticcurrency)
        VALUES
            ('Cash', 1),
            ('Domestic Equity', 1),
            ('Emergent Markets Equity', 0);"""
        )
    )
    db.conn.execute(
        text(
            """
        INSERT INTO assets (ticker, class)
        VALUES
            ('Cash', 'Cash'),
            ('VCN.TO', 'Domestic Equity'),
            ('VEE.TO', 'Emergent Markets Equity');"""
        )
    )
    db.conn.execute(
        text(
            """
        INSERT INTO assetprices (ticker, day, close)
        VALUES
            ('VCN.TO', '2017-03-02', 30.00),
            ('VCN.TO', '2017-03-03', 30.10),
            ('VCN.TO', '2017-03-06', 29.85),
            ('VCN.TO', '2017-03-07', 29.85),
            ('VCN.TO', '2017-03-08', 30.15),
            ('VEE.TO', '2017-03-02', 29.00),
            ('VEE.TO', '2017-03-03', 28.00),
            ('VEE.TO', '2017-03-06', 32.00),
            ('VEE.TO', '2017-03-07', 32.00),
            ('VEE.TO', '2017-03-08', 31.00);"""
        )
    )
    db.conn.execute(
        text(
            """
        INSERT INTO distributions (ticker, day, amount)
        VALUES
            ('VCN.TO', '2017-03-03', 0.1010),
            ('VCN.TO', '2017-03-06', 0.0990);"""
        )
    )
    db.conn.execute(
        text(
            """
        INSERT INTO transactions (day, txtype, account, source, target, units, unitprice, commission, total)
        VALUES
            ('2017-03-02', 'deposit', 'RRSP1', null, 'Cash', null, null, null, 10000),
            ('2017-03-03', 'buy', 'RRSP1', 'Cash', 'VCN.TO', 100, 30.10, 0.35, 3010.35),
            ('2017-03-03', 'buy', 'RRSP1', 'Cash', 'VEE.TO', 100, 28.00, 0.35, 2800.35),
            ('2017-03-03', 'dividend', 'RRSP1', 'VCN.TO', 'Cash', 100, 0.1010, 0, 10.10),
            ('2017-03-06', 'dividend', 'RRSP1', 'VCN.TO', 'Cash', 100, 0.0990, 0, 9.90);"""
        )
    )


def simple_fixture_teardown():
    if db.conn is not None:
        db.conn.close()
    if db.engine is not None:
        db.engine.dispose()
