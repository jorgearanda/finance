from docopt import docopt
import pandas as pd
import psycopg2
from psycopg2.extras import NamedTupleCursor

import config

usage = '''
Load missing asset prices from Yahoo Finance into the database.

Usage:
    update_prices.py [-h] [--env <env>]

Options:
    -h --help               Show this
    -e <env> --env <env>    Environment to load prices into [default: dev]
'''

conn = None


def connect(env):
    global conn
    conn = psycopg2.connect(database=config.database[env], cursor_factory=NamedTupleCursor)


def get_tickers():
    with conn.cursor() as cur:
        cur.execute('''SELECT ticker FROM assets WHERE class != 'Domestic Cash';''')
        return cur.fetchall()


def main(args):
    env = args['--env']
    connect(env)
    prices = {x.ticker: None for x in get_tickers()}
    cur = conn.cursor()
    for ticker in prices.keys():
        table = pd.read_csv('http://ichart.finance.yahoo.com/table.csv?s=' + ticker + '&g=d')
        for row in table.iterrows():
            if row[1]['Date'] >= '2016-09-08':  # TODO: so hacky; fix
                cur.execute('''
                    INSERT INTO assetprices (ticker, day, ask, bid)
                    VALUES (%(ticker)s, %(day)s, %(close)s, %(close)s) ON CONFLICT DO NOTHING;''',
                    {
                        'ticker': ticker,
                        'day': row[1]['Date'],
                        'close': row[1]['Close']
                    })

    cur.close()
    conn.commit()


if __name__ == '__main__':
    args = docopt(usage, argv=None, help=True, version=None, options_first=False)
    main(args)
