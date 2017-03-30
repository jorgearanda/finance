import pytest

from db import db


@pytest.fixture
def simple():
    with db.conn.cursor() as cur:
        cur.execute('''
            INSERT INTO marketdays (day, open)
            VALUES
                ('2017-03-02', true),
                ('2017-03-03', true),
                ('2017-03-04', false),
                ('2017-03-05', false),
                ('2017-03-06', true),
                ('2017-03-07', true);''')
        cur.execute('''
            INSERT INTO assetclasses (name, domesticcurrency)
            VALUES
                ('Domestic Equity', true),
                ('Emergent Markets Equity', false);''')
        cur.execute('''
            INSERT INTO assets (ticker, class)
            VALUES
                ('VCN.TO', 'Domestic Equity'),
                ('VEE.TO', 'Emergent Markets Equity');''')
        cur.execute('''
            INSERT INTO assetprices (ticker, day, close)
            VALUES
                ('VCN.TO', '2017-03-02', 30.00),
                ('VCN.TO', '2017-03-03', 30.10),
                ('VCN.TO', '2017-03-06', 29.85),
                ('VCN.TO', '2017-03-07', 30.15),
                ('VEE.TO', '2017-03-02', 29.00),
                ('VEE.TO', '2017-03-03', 28.00),
                ('VEE.TO', '2017-03-06', 32.00),
                ('VEE.TO', '2017-03-07', 31.00);''')
    yield True
    with db.conn.cursor() as cur:
        cur.execute('''DELETE FROM assetprices;''')
        cur.execute('''DELETE FROM marketdays;''')
        cur.execute('''DELETE FROM assets;''')
        cur.execute('''DELETE FROM assetclasses;''')
