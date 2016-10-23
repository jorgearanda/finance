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
        self.prices = OrderedDict()
        self.performance = OrderedDict()
        self.deposits = OrderedDict()
        self.buys = OrderedDict()
        self.sales = OrderedDict()
        self.dividends = OrderedDict()
        self.load()

    def load(self):
        self.load_market_days()
        self.load_daily_prices()
        self.load_transactions()
        self.load_assets_in_performance()
        self.calculate_dailies()

    def load_market_days(self):
        '''
        Create the skeleton of self.performance: an OrderedDict with daily entries
        which will be populated with the performance data for this portfolio
        '''
        cur = self.conn.cursor()
        cur.execute('''
                    SELECT day, open
                    FROM marketDays
                    WHERE day >= %(created)s AND day <= %(today)s
                    ORDER BY day;''',
                    {'created': self.date_created, 'today': date.today()})

        for row in cur.fetchall():
            self.performance[row.day] = {
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
            self.prices.setdefault(asset_price.day, {})
            self.prices[asset_price.day][asset_price.ticker] = asset_price.ask

        cur.close()

    def load_transactions(self):
        '''
        Get all transactions for the account(s) in question, and populate the relevant
        data structures with their details.
        '''
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

    def add_deposit(self, tx):
        self.deposits.setdefault(tx.day, {}).setdefault('amount', Decimal(0))
        self.deposits[tx.day]['amount'] += tx.total
        self.performance[tx.day]['dayDeposits'] += tx.total

    def add_buy(self, tx):
        empty_buy = {
            'units': 0,
            'positionCost': 0,
            'averagePrice': 0
        }
        self.buys.setdefault(tx.day, {}).setdefault(tx.target, empty_buy)
        self.buys[tx.day][tx.target]['units'] += tx.units
        self.buys[tx.day][tx.target]['positionCost'] += tx.total
        self.buys[tx.day][tx.target]['averagePrice'] += tx.total / tx.units

    def add_dividend(self, tx):
        self.dividends.setdefault(tx.day, {}).setdefault('amount', Decimal(0))
        self.dividends[tx.day]['amount'] += tx.total
        self.performance[tx.day]['dayDividends'] += tx.total

    def load_assets_in_performance(self):
        prev_data = {'assets': {}}
        for day, data in self.performance.items():
            if not data['open']:
                data['assets'] = deepcopy(prev_data['assets'])
                continue

            buy_data = self.buys.get(day, {})
            for ticker, asset_data in prev_data['assets'].items():
                data['assets'][ticker] = deepcopy(asset_data)

            for ticker, buy_data in self.buys.get(day, {}).items():
                data['assets'].setdefault(ticker, {
                    'units': 0,
                    'positionCost': 0,
                    'averagePrice': 0
                })
                data['assets'][ticker]['units'] += buy_data['units']
                data['assets'][ticker]['positionCost'] += buy_data['positionCost']
                data['assets'][ticker]['averagePrice'] = \
                    data['assets'][ticker]['positionCost'] / data['assets'][ticker]['units']

            # TODO: Include sales

            for ticker, ticker_data in data['assets'].items():
                ticker_data['currentPrice'] = self.prices.get(day, {}).get(ticker)
                if ticker_data['currentPrice'] is None:
                    ticker_data['currentPrice'] = prev_data['assets'][ticker]['currentPrice']
                ticker_data['marketValue'] = ticker_data['units'] * ticker_data['currentPrice']

            prev_data = data

    def calculate_dailies(self):
        first_day = self.performance[self.date_created]
        first_day['totalDeposits'] = first_day['dayDeposits']
        first_day['totalDividends'] = first_day['dayDividends']
        first_day['cash'] = first_day['totalDeposits'] + first_day['totalDividends']
        first_day['marketValue'] = first_day['cash']

        for day, data in [x for x in self.performance.items()][1:]:
            prev = self.performance[day - timedelta(days=1)]
            data['totalDeposits'] = data['dayDeposits'] + prev['totalDeposits']
            data['totalDividends'] = data['dayDividends'] + prev['totalDividends']
            data['cash'] = data['totalDeposits'] + data['totalDividends']

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


class DataError(Exception):
    pass
