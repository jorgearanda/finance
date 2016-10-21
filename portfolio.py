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
            print(row)
            self.performance[row.day] = {
                'date': row.day,
                'open': row.open,
                'assets': {},
                'dayDeposits': Decimal(0.0),
                'dayDividends': Decimal(0.0)
            }
        cur.close()

        dates = list(self.performance.items())
        if dates[0][0] > self.date_created:
            raise DataError('marketDays table starts after this account was opened')
        if dates[-1][0] < date.today():
            raise DataError('marketDays table ends before today')

    def load_daily_prices(self):
        pass

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
        previous_data = {
            'cash': 0,
            'totalDeposits': 0,
            'totalDividends': 0,
            'assets': {}
        }
        for day, data in self.performance.items():
            data['totalDeposits'] = data['dayDeposits'] + previous_data['totalDeposits']
            data['totalDividends'] = data['dayDividends'] + previous_data['totalDividends']
            data['cash'] = data['totalDeposits'] + data['totalDividends']
            for ticker, asset in previous_data['assets'].items():
                if data['assets'].get(ticker) is None:
                    data['assets'][ticker] = deepcopy(asset)
                else:
                    data['assets'][ticker]['units'] += asset['units']
                    data['assets'][ticker]['positionCost'] += asset['positionCost']
                    data['assets'][ticker]['averagePrice'] = \
                        data['assets'][ticker]['positionCost'] / data['assets'][ticker]['units']
            for ticker in data['assets'].keys():
                data['cash'] -= data['assets'][ticker]['positionCost']
            previous_data = data

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
        if self.performance[tx.day]['assets'].get(tx.target) is None:
            self.performance[tx.day]['assets'][tx.target] = {
                'units': 0,
                'positionCost': 0,
                'averagePrice': 0
            }
        self.performance[tx.day]['assets'][tx.target]['units'] += tx.units
        self.performance[tx.day]['assets'][tx.target]['positionCost'] += tx.total
        self.performance[tx.day]['assets'][tx.target]['averagePrice'] += tx.total / tx.units

    def add_dividend(self, tx):
        self.performance[tx.day]['dayDividends'] += tx.total


class DataError(Exception):
    pass
