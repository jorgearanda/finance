import psycopg2

import config

conn = None
tables = [
    'accounts',
    'accountTypes',
    'investors',
    'marketDays'
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
    conn = psycopg2.connect(database=config.database['test'])
    clean_postgres()


def teardown():
    pass
