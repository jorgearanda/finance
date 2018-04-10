import calendar
from datetime import date, datetime as dt, timedelta
from docopt import docopt
import psycopg2
from psycopg2.extras import NamedTupleCursor
import re
import requests
import time

import config
from db import db


usage = '''
Load missing asset prices from Yahoo Finance into the database.

Usage:
    update_prices_yahoo.py [-h] [--env <env>]

Options:
    -h --help               Show this
    -e <env> --env <env>    Environment to load prices into [default: dev]
'''


def get_tickers():
    """Get all the tickers to poll from the database."""
    with db.conn.cursor() as cur:
        cur.execute('''
            SELECT ticker AS name
            FROM assets
            WHERE class != 'Domestic Cash';''')

        return cur.fetchall()


def get_cookie_and_crumb(symbol):
    """Get cookie and crumb for further calls. The crumb is a bit tricky."""
    print('* Getting a cookie')
    url = f'https://finance.yahoo.com/quote/{symbol}/history?p={symbol}'
    r = requests.get(url, timeout=5.0)
    cookie = {'B': r.cookies['B']}
    content = r.content.decode('unicode-escape')
    match = re.search(r'CrumbStore":{"crumb":"(.*?)"}', content)
    crumb = match.group(1)

    return cookie, crumb


def get_quote(symbol, crumb, cookie):
    """Query for the historical data of `symbol` for the last month."""
    print(f'* Getting quotes for {symbol}')
    ts_from = calendar.timegm((date.today() - timedelta(days=30)).timetuple())
    ts_to = calendar.timegm(date.today().timetuple())
    url = f'https://query1.finance.yahoo.com/v7/finance/download/{symbol}?' + \
        f'period1={ts_from}&period2={ts_to}&' + \
        f'interval=1d&events=historical&crumb={crumb}'
    res = None
    tries = 0

    while res is None and tries < 5:
        try:
            res = requests.get(url, cookies=cookie, timeout=5.0)
        except:
            print('Error while fetching quotes. Will sleep, then try again.')
            tries += 1
            time.sleep(1)

    if res is not None:
        return res.text
    else:
        raise Exception(f'Unable to get quote for {symbol}')


def update_prices_for_ticker(symbol, lines):
    """Use historical data in `lines` to populate prices table."""
    for line in lines:
        if len(line) == 0:
            continue
        values = line.split(',')
        day = dt.strptime(values[0], '%Y-%m-%d').date()
        if values[4] == 'null':
            print(f'  - Skipping null price on {day}')
            continue

        with db.conn.cursor() as cur:
            cur.execute('''
                INSERT INTO assetprices (ticker, day, ask, bid, close)
                VALUES (%(ticker)s, %(day)s, %(close)s, %(close)s, %(close)s)
                ON CONFLICT DO NOTHING;''', {
                    'ticker': symbol,
                    'day': day,
                    'close': values[4]
                })

            if cur.rowcount == 1:
                print(f'  - Inserted price for {day}')


def main(args):
    env = args['--env']
    cookie = None
    crumb = None
    db.connect(env)
    for ticker in get_tickers():
        if not cookie:
            cookie, crumb = get_cookie_and_crumb(ticker.name)
        quote_lines = get_quote(ticker.name, crumb, cookie).split('\n')[1:]
        update_prices_for_ticker(ticker.name, quote_lines)
        time.sleep(0.1)  # Be kind to the server

    print('Done!')


if __name__ == '__main__':
    args = docopt(usage, argv=None, help=True)
    main(args)
