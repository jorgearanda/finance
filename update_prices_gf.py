from bs4 import BeautifulSoup
from datetime import date, datetime as dt
from docopt import docopt
import html5lib
import psycopg2
from psycopg2.extras import NamedTupleCursor
import urllib.request as req

import config

usage = '''
Load missing asset prices from Google Finance into the database.

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
        print('Updating prices for ' + ticker + '...')
        tse_ticker = 'TSE:' + ticker[:-3]  # e.g. TSE:VAB rather than VAB.TO
        page = req.urlopen('https://www.google.com/finance/historical?q=' + tse_ticker).read()
        soup = BeautifulSoup(page, 'html5lib')
        table = soup.find('table', class_='gf-table historical_price')

        counter = 0
        inserted = 0
        format = '%b %d, %Y'
        for td in table.find_all('td'):
            if counter == 0:
                day = dt.strptime(td.text.strip(), format).date()
            if counter == 4:
                close = td.text.strip()
            counter += 1
            if counter > 5:
                counter = 0
                if day != date.today():
                    cur.execute('''
                        INSERT INTO assetprices (ticker, day, ask, bid, close)
                        VALUES (%(ticker)s, %(day)s, %(close)s, %(close)s, %(close)s) ON CONFLICT DO NOTHING;''',
                        {
                            'ticker': ticker,
                            'day': day,
                            'close': close
                        })
                    inserted += cur.rowcount

        print(str(inserted) + ' prices updated')

    cur.close()
    conn.commit()


if __name__ == '__main__':
    args = docopt(usage, argv=None, help=True, version=None, options_first=False)
    main(args)
