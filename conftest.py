import pytest
from sqlalchemy import text

from db import db


db._env = "test"


def simple_fixture():
    db._env = "test"
    db.ensure_connected("test")
    db.conn.execute(text("""DELETE FROM transactions;"""))
    db.conn.execute(text("""DELETE FROM distributions;"""))
    db.conn.execute(text("""DELETE FROM assetprices;"""))
    db.conn.execute(text("""DELETE FROM marketdays;"""))
    db.conn.execute(text("""DELETE FROM assets;"""))
    db.conn.execute(text("""DELETE FROM assetclasses;"""))
    db.conn.execute(text("""DELETE FROM accounts;"""))
    db.conn.execute(text("""DELETE FROM investors;"""))
    db.conn.execute(text("""DELETE FROM accounttypes;"""))
    db.conn.execute(
        text(
            """
        INSERT INTO accounttypes (name, tax, margin)
        VALUES ('RRSP', 'deferred', false);"""
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
            ('2017-03-02', true),
            ('2017-03-03', true),
            ('2017-03-04', false),
            ('2017-03-05', false),
            ('2017-03-06', true),
            ('2017-03-07', true),
            ('2017-03-08', true);"""
        )
    )
    db.conn.execute(
        text(
            """
        INSERT INTO assetclasses (name, domesticcurrency)
        VALUES
            ('Cash', true),
            ('Domestic Equity', true),
            ('Emergent Markets Equity', false);"""
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
    db.conn.close()
    db.ensure_connected("test")


def simple_fixture_teardown():
    db.ensure_connected("test")
    db.conn.execute(text("""DELETE FROM transactions;"""))
    db.conn.execute(text("""DELETE FROM distributions;"""))
    db.conn.execute(text("""DELETE FROM assetprices;"""))
    db.conn.execute(text("""DELETE FROM marketdays;"""))
    db.conn.execute(text("""DELETE FROM assets;"""))
    db.conn.execute(text("""DELETE FROM assetclasses;"""))
    db.conn.execute(text("""DELETE FROM accounts;"""))
    db.conn.execute(text("""DELETE FROM investors;"""))
    db.conn.execute(text("""DELETE FROM accounttypes;"""))
    db.conn.close()
