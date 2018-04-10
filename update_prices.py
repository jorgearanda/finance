import calendar
from datetime import date, datetime as dt, timedelta
from docopt import docopt
import grequests
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


def update_prices_for_ticker(symbol, lines):
    """Use historical data in `lines` to populate prices table."""
    print(f'* Saving quotes for {symbol}')
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


def create_ticker_requests(tickers, cookie, crumb):
    ts_from = calendar.timegm((date.today() - timedelta(days=30)).timetuple())
    ts_to = calendar.timegm(date.today().timetuple())
    base = 'https://query1.finance.yahoo.com/v7/finance/download/'
    params = f'?period1={ts_from}&period2={ts_to}&' + \
        f'interval=1d&events=historical&crumb={crumb}'

    return (grequests.get(
        base + ticker.name + params,
        cookies=cookie, timeout=5.0) for ticker in tickers)


def main(args):
    env = args['--env']
    cookie = None
    crumb = None
    db.connect(env)
    tickers = get_tickers()
    if len(tickers) == 0:
        print('--no tickers in database--')
        return

    cookie, crumb = get_cookie_and_crumb(tickers[0].name)
    reqs = create_ticker_requests(tickers, cookie, crumb)
    print('* Sending requests for quotes')
    for res in grequests.map(reqs):
        symbol = re.search(r'download/(.*)\?', res.request.url).group(1)
        quote_lines = res.text.split('\n')[1:]
        update_prices_for_ticker(symbol, quote_lines)

    print('Done!')


if __name__ == '__main__':
    args = docopt(usage, argv=None, help=True)
    main(args)
