import psycopg2
from psycopg2.extras import NamedTupleCursor

import config

conn = None
tables = [
    'distributions',
    'assetPrices',
    'transactions',
    'accounts',
    'accountTypes',
    'investors',
    'marketDays',
    'assets',
    'assetClasses'
]


def clean_postgres():
    global conn
    cur = conn.cursor()
    for table in tables:
        cur.execute('DELETE FROM ' + table)
    conn.commit()
    cur.close()


def setup():
    global conn
    conn = psycopg2.connect(database=config.db['test']['db'], user=config.db['test']['user'], cursor_factory=NamedTupleCursor)
    clean_postgres()


def teardown():
    clean_postgres()
