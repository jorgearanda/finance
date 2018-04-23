import pytest

from db import db


@pytest.fixture
def simple():
    with db.conn.cursor() as cur:
        cur.execute('''
            INSERT INTO accounttypes (name, tax, margin)
            VALUES ('RRSP', 'deferred', false);''')
        cur.execute('''
            INSERT INTO investors (name)
            VALUES ('Someone');''')
        cur.execute('''
            INSERT INTO accounts (name, accounttype, investor, datecreated)
            VALUES ('RRSP1', 'RRSP', 'Someone', '2017-03-02');''')
        cur.execute('''
            INSERT INTO marketdays (day, open)
            VALUES
                ('2017-03-02', true),
                ('2017-03-03', true),
                ('2017-03-04', false),
                ('2017-03-05', false),
                ('2017-03-06', true),
                ('2017-03-07', true),
                ('2017-03-08', true);''')
        cur.execute('''
            INSERT INTO assetclasses (name, domesticcurrency)
            VALUES
                ('Cash', true),
                ('Domestic Equity', true),
                ('Emergent Markets Equity', false);''')
        cur.execute('''
            INSERT INTO assets (ticker, class)
            VALUES
                ('Cash', 'Cash'),
                ('VCN.TO', 'Domestic Equity'),
                ('VEE.TO', 'Emergent Markets Equity');''')
        cur.execute('''
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
                ('VEE.TO', '2017-03-08', 31.00);''')
        cur.execute('''
            INSERT INTO distributions (ticker, day, amount)
            VALUES
                ('VCN.TO', '2017-03-03', 0.1010),
                ('VCN.TO', '2017-03-06', 0.0990);''')
        cur.execute('''
            INSERT INTO transactions (day, txtype, account, source, target, units, unitprice, commission, total)
            VALUES
                ('2017-03-02', 'deposit', 'RRSP1', null, 'Cash', null, null, null, 10000),
                ('2017-03-03', 'buy', 'RRSP1', 'Cash', 'VCN.TO', 100, 30.10, 0.35, 3010.35),
                ('2017-03-03', 'buy', 'RRSP1', 'Cash', 'VEE.TO', 100, 28.00, 0.35, 2800.35),
                ('2017-03-03', 'dividend', 'RRSP1', 'VCN.TO', 'Cash', 100, 0.1010, 0, 10.10),
                ('2017-03-06', 'dividend', 'RRSP1', 'VCN.TO', 'Cash', 100, 0.0990, 0, 9.90);''')

    yield True
    with db.conn.cursor() as cur:
        cur.execute('''DELETE FROM transactions;''')
        cur.execute('''DELETE FROM distributions;''')
        cur.execute('''DELETE FROM assetprices;''')
        cur.execute('''DELETE FROM marketdays;''')
        cur.execute('''DELETE FROM assets;''')
        cur.execute('''DELETE FROM assetclasses;''')
        cur.execute('''DELETE FROM accounts;''')
        cur.execute('''DELETE FROM investors;''')
        cur.execute('''DELETE FROM accounttypes;''')
