from collections import namedtuple, OrderedDict
from copy import deepcopy
from datetime import date, datetime, timedelta
from decimal import Decimal
import psycopg2

import config


Transaction = namedtuple('Transaction', 'day, txType, account, source, target, units, unitPrice, commission, total')


class Portfolio():
    def __init__(self, account=None, env='dev', conn=None):
        if conn is None:
            self.conn = psycopg2.connect(database=config.database[env])
        else:
            self.conn = conn
        self.account = account
        self.date_created = self.get_date_created()
        self.performance = OrderedDict()
        self.load()

    def load(self):
        self.load_market_days()
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
            self.performance[row[0]] = {
                'date': row[0],
                'open': row[1],
                'assets': {},
                'dayDeposits': Decimal(0.0)
            }
        cur.close()

        dates = list(self.performance.items())
        if dates[0][0] > self.date_created:
            raise DataError('marketDays table starts after this account was opened')
        if dates[-1][0] < date.today():
            raise DataError('marketDays table ends before today')

    def load_transactions(self):
        cur = self.conn.cursor()
        cur.execute('''
                    SELECT day, txType, account, source, target, units, unitPrice, commission, total
                    FROM transactions
                    WHERE %(account)s is null OR account = %(account)s
                    ORDER BY day;''',
                    {'account': self.account})

        transactions = [Transaction(*values) for values in cur.fetchall()]
        cur.close()

        for tx in transactions:
            if tx.txType == 'deposit':
                self.add_deposit(tx)
            elif tx.txType == 'buy':
                self.add_buy(tx)

    def calculate_dailies(self):
        previous_data = {'totalDeposits': 0, 'assets': {}}
        for day, data in self.performance.items():
            data['totalDeposits'] = data['dayDeposits'] + previous_data['totalDeposits']
            for ticker, asset in previous_data['assets'].items():
                if data['assets'].get(ticker) is None:
                    data['assets'][ticker] = deepcopy(asset)
                else:
                    data['assets'][ticker]['units'] += asset['units']
                    data['assets'][ticker]['paidValue'] += asset['paidValue']
                    data['assets'][ticker]['averagePrice'] = \
                        data['assets'][ticker]['paidValue'] / data['assets'][ticker]['units']
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
                'paidValue': 0,
                'averagePrice': 0
            }
        self.performance[tx.day]['assets'][tx.target]['units'] += tx.units
        self.performance[tx.day]['assets'][tx.target]['paidValue'] += tx.total
        self.performance[tx.day]['assets'][tx.target]['averagePrice'] += tx.total / tx.units


class DataError(Exception):
    pass
