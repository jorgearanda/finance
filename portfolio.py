from collections import namedtuple
from datetime import date, datetime, timedelta
import psycopg2


AskPrice = namedtuple('AskPrice', 'ticker day ask')
conn = psycopg2.connect(database='finance')


def ticker_prices(day=date.today()):
    global conn
    cur = conn.cursor()
    cur.execute('''
                WITH lastDate (ticker, day) AS
                    (SELECT ticker, MAX(day) FROM assetPrices WHERE day <= %s GROUP BY ticker)
                SELECT assetPrices.ticker, assetPrices.day, ask
                FROM assetPrices JOIN lastDate ON lastDate.day = assetPrices.day
                AND lastDate.ticker = assetPrices.ticker;''',
                (day,))
    prices = [AskPrice(*values) for values in cur.fetchall()]
    cur.close()
    return prices
