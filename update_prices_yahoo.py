# Adapted from
# https://stackoverflow.com/questions/44044263/yahoo-finance-historical-data-downloader-url-is-not-working

import calendar
from datetime import date, datetime as dt, timedelta
from docopt import docopt
import psycopg2
from psycopg2.extras import NamedTupleCursor
import re
import time
from urllib.request import urlopen, Request, URLError

import config

usage = '''
Load missing asset prices from Yahoo Finance into the database.

Usage:
    update_prices_yahoo.py [-h] [--env <env>]

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
        cur.execute('''SELECT ticker AS name FROM assets WHERE class != 'Domestic Cash';''')
        return cur.fetchall()


def get_crumble_and_cookie(symbol):
    res = urlopen(f'https://finance.yahoo.com/quote/{symbol}/history?p={symbol}')

    match = re.search(r'set-cookie: (.*?); ', str(res.info()))
    cookie = match.group(1)

    text = res.read().decode('utf-8')
    match = re.search(r'CrumbStore":{"crumb":"(.*?)"}', text)
    crumble = match.group(1)

    return crumble, cookie


def download_quote(symbol, crumble, cookie):
    ts_from = calendar.timegm((date.today() - timedelta(days=30)).timetuple())
    ts_to = calendar.timegm(date.today().timetuple())
    link = f'https://query1.finance.yahoo.com/v7/finance/download/{symbol}?' + \
        f'period1={ts_from}&period2={ts_to}&interval=1d&events=historical&crumb={crumble}'
    req = Request(link, headers={'Cookie': cookie})

    try:
        res = urlopen(req)
        text = res.read().decode('utf-8')
        return text
    except URLError:
        print(f'==={symbol} request failed ===')
        print(URLError.reason)
        return ''


def main(args):
    env = args['--env']
    connect(env)
    cur = conn.cursor()
    crumble = None
    cookie = None
    for ticker in get_tickers():
        print(f'Inserting prices for {ticker.name}...')
        if not cookie:
            crumble, cookie = get_crumble_and_cookie(ticker.name)
        quote_lines = download_quote(ticker.name, crumble, cookie).split('\n')[1:]
        inserted = 0

        # dates are zeroth item and closing prices are fourth
        for line in quote_lines:
            if len(line) == 0:
                continue
            values = line.split(',')
            day = dt.strptime(values[0], '%Y-%m-%d').date()
            if day > date.today() - timedelta(days=7):
                print(f'Found ticker for {day}')
            cur.execute('''
                INSERT INTO assetprices (ticker, day, ask, bid, close)
                VALUES (%(ticker)s, %(day)s, %(close)s, %(close)s, %(close)s)
                ON CONFLICT DO NOTHING;''',
                {
                    'ticker': ticker.name,
                    'day': day,
                    'close': values[4]
                })
            inserted += cur.rowcount

        print(f'{inserted} prices inserted')
        time.sleep(1)

    cur.close()
    conn.commit()

if __name__ == '__main__':
    args = docopt(usage, argv=None, help=True, version=None, options_first=False)
    main(args)
