from collections import namedtuple, OrderedDict
from copy import deepcopy
from datetime import date, datetime, timedelta
from decimal import Decimal
import psycopg2
from psycopg2.extras import NamedTupleCursor

import config


class Portfolio():
    def __init__(self, account=None, env='dev', conn=None):
        if conn is None:
            self.conn = psycopg2.connect(database=config.database[env], cursor_factory=NamedTupleCursor)
        else:
            self.conn = conn
        self.account = account
        self.date_created = self.get_date_created()
        self.performance = OrderedDict()
        self.load()

    def load(self):
        self.load_market_days()
        self.load_daily_prices()
        self.load_transactions()
        self.calculate_dailies()

    def load_market_days(self):
        cur = self.conn.cursor()
        cur.execute('''
                    SELECT day, open
                    FROM marketDays
                    WHERE day >= %(created)s AND day <= %(today)s
                    ORDER BY day;''',
                    {'created': self.date_created, 'today': date.today()})

        for row in cur.fetchall():
            self.performance[row.day] = {
                'date': row.day,
                'open': row.open,
                'assets': {},
                'cash': Decimal(0.0),
                'dayDeposits': Decimal(0.0),
                'totalDeposits': Decimal(0.0),
                'dayDividends': Decimal(0.0),
                'totalDividends': Decimal(0.0),
                'marketValue': Decimal(0.0)
            }
        cur.close()

        dates = list(self.performance.items())
        if dates[0][0] > self.date_created:
            raise DataError('marketDays table starts after this account was opened')
        if dates[-1][0] < date.today():
            raise DataError('marketDays table ends before today')

    def load_daily_prices(self):
        cur = self.conn.cursor()
        cur.execute('''
                    SELECT ticker, day, ask
                    FROM assetPrices
                    ORDER BY day;''')

        for asset_price in cur.fetchall():
            self.performance[asset_price.day]['assets'][asset_price.ticker] = {
                'units': 0,
                'positionCost': 0,
                'averagePrice': 0,
                'currentPrice': asset_price.ask,
                'marketValue': 0
            }

        cur.close()

    def load_transactions(self):
        cur = self.conn.cursor()
        cur.execute('''
                    SELECT day, txtype, account, source, target, units, unitprice, commission, total
                    FROM transactions
                    WHERE %(account)s is null OR account = %(account)s
                    ORDER BY day;''',
                    {'account': self.account})

        for tx in cur.fetchall():
            if tx.txtype == 'deposit':
                self.add_deposit(tx)
            elif tx.txtype == 'buy':
                self.add_buy(tx)
            elif tx.txtype == 'dividend':
                self.add_dividend(tx)

        cur.close()

    def calculate_dailies(self):
        for day, data in self.performance.items():
            if day == self.date_created:
                data['totalDeposits'] = data['dayDeposits']
                data['cash'] = data['totalDeposits'] + data['totalDividends']
                data['marketValue'] = data['cash']
                continue
            prev = self.performance[day - timedelta(days=1)]
            data['totalDeposits'] = data['dayDeposits'] + prev['totalDeposits']
            data['totalDividends'] = data['dayDividends'] + prev['totalDividends']
            data['cash'] = data['totalDeposits'] + data['totalDividends']
            for ticker, asset in prev['assets'].items():
                if data['assets'].get(ticker) is None:
                    data['assets'][ticker] = {
                        'units': 0,
                        'positionCost': 0,
                        'currentPrice': asset['currentPrice']
                    }
            for ticker, asset in data['assets'].items():
                if prev['assets'].get(ticker) is not None:
                    asset['units'] += prev['assets'][ticker]['units']
                    asset['positionCost'] += prev['assets'][ticker]['positionCost']
                if asset['units'] > 0:
                    asset['averagePrice'] = asset['positionCost'] / asset['units']
                    asset['marketValue'] = asset['units'] * asset['currentPrice']
            for ticker in data['assets'].keys():
                data['cash'] -= data['assets'][ticker]['positionCost']
                data['marketValue'] += data['assets'][ticker]['marketValue']
            data['marketValue'] += data['cash']

    def get_date_created(self):
        cur = self.conn.cursor()
        cur.execute('''
                    SELECT MIN(dateCreated)
                    FROM accounts
                    WHERE %(name)s is null OR name = %(name)s;''',
                    {'name': self.account})

        start = cur.fetchone()[0]
        cur.close()

        if start is None:
            raise DataError('No account records found')

        return start

    def add_deposit(self, tx):
        self.performance[tx.day]['dayDeposits'] += tx.total

    def add_buy(self, tx):
        self.performance[tx.day]['assets'][tx.target]['units'] += tx.units
        self.performance[tx.day]['assets'][tx.target]['positionCost'] += tx.total
        self.performance[tx.day]['assets'][tx.target]['averagePrice'] += tx.total / tx.units

    def add_dividend(self, tx):
        self.performance[tx.day]['dayDividends'] += tx.total


class DataError(Exception):
    pass
