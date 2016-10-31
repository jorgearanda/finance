from collections import namedtuple, OrderedDict
from copy import deepcopy
from datetime import date, datetime, timedelta
from decimal import Decimal
from itertools import combinations
import psycopg2
from psycopg2.extras import NamedTupleCursor
from scipy.stats import pearsonr
import statistics

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
        self.tickers = set()
        self.correlations = OrderedDict()
        self.load()

    def load(self):
        self.load_market_days()
        self.load_daily_prices()
        self.load_transactions()
        self.load_assets_in_performance()
        self.calculate_dailies()
        self.calculate_deposit_performance()
        self.calculate_correlations()

    def load_market_days(self):
        '''
        Create the skeleton of self.performance: an OrderedDict with daily entries
        which will be populated with the performance data for this portfolio
        '''
        cur = self.conn.cursor()
        cur.execute('''
                    SELECT day, open
                    FROM marketDays
                    WHERE day >= %(created)s AND day < %(today)s
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
        if dates[-1][0] < date.today() - timedelta(days=1):
            raise DataError('marketDays table ends before yesterday')

    def load_daily_prices(self):
        cur = self.conn.cursor()
        cur.execute('''
                    SELECT ticker, day, ask
                    FROM assetPrices
                    ORDER BY day;''')

        for asset_price in cur.fetchall():
            self.prices.setdefault(asset_price.day, {})
            self.prices[asset_price.day][asset_price.ticker] = asset_price.ask
            self.tickers.add(asset_price.ticker)

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
        self.dividends.setdefault(tx.day, {}).setdefault(tx.source, Decimal(0))
        self.dividends[tx.day][tx.source] += tx.total
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
                ticker_data = data['assets'][ticker]
                ticker_data['units'] += buy_data['units']
                ticker_data['positionCost'] += buy_data['positionCost']
                ticker_data['averagePrice'] = ticker_data['positionCost'] / ticker_data['units']

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
        first_day['dayProfitOrLoss'] = Decimal(0)
        first_day['dayReturns'] = Decimal(0)
        first_day['profitOrLoss'] = Decimal(0)
        first_day['ttwr'] = Decimal(0)
        first_day['volatility'] = None
        first_day['10kEquivalent'] = Decimal(10000)
        first_day['lastPeakTtwr'] = Decimal(0)
        first_day['lastPeak10kEquivalent'] = Decimal(10000)
        first_day['currentDrawdown'] = Decimal(0)
        first_day['currentDrawdownStart'] = self.date_created
        first_day['greatestDrawdown'] = Decimal(0)
        first_day['greatestDrawdownStart'] = self.date_created
        first_day['greatestDrawdownEnd'] = self.date_created
        first_day['sharpe'] = Decimal(0)

        returns_lists = {}
        for day, data in [x for x in self.performance.items()][1:]:
            prev = self.performance[day - timedelta(days=1)]
            days_from_start = (day - self.date_created).days
            data['totalDeposits'] = data['dayDeposits'] + prev['totalDeposits']
            data['totalDividends'] = data['dayDividends'] + prev['totalDividends']
            data['cash'] = data['totalDeposits'] + data['totalDividends']

            for ticker, ticker_data in data['assets'].items():
                prev_ticker_data = prev['assets'].get(ticker, {})
                data['cash'] -= ticker_data['positionCost']
                data['marketValue'] += ticker_data['marketValue']
                ticker_data['profitOrLoss'] = ticker_data['marketValue'] - ticker_data['positionCost']
                ticker_data['dayProfitOrLoss'] = \
                    ticker_data['profitOrLoss'] - prev_ticker_data.get('profitOrLoss', 0)
                if prev['assets'].get(ticker) is None:
                    ticker_data['dayReturns'] = None
                    ticker_data['ttwr'] = Decimal(0)
                    ticker_data['10kEquivalent'] = Decimal(10000)
                    ticker_data['lastPeakTtwr'] = ticker_data['ttwr']
                    ticker_data['lastPeak10kEquivalent'] = Decimal(10000)
                    ticker_data['currentDrawdown'] = Decimal(0)
                    ticker_data['currentDrawdownStart'] = day
                    ticker_data['greatestDrawdown'] = Decimal(0)
                    ticker_data['greatestDrawdownStart'] = day
                    ticker_data['greatestDrawdownEnd'] = day
                else:
                    ticker_data['dayReturns'] = \
                        (ticker_data['currentPrice'] - prev['assets'][ticker]['currentPrice']) / \
                        prev['assets'][ticker]['currentPrice']
                    ticker_data['ttwr'] = (prev_ticker_data['ttwr'] + 1) * (ticker_data['dayReturns'] + 1) - 1
                    ticker_data['10kEquivalent'] = prev_ticker_data['10kEquivalent'] * (1 + ticker_data['dayReturns'])

                    if ticker_data['10kEquivalent'] > prev_ticker_data['lastPeak10kEquivalent']:
                        ticker_data['lastPeakTtwr'] = ticker_data['ttwr']
                        ticker_data['lastPeak10kEquivalent'] = ticker_data['10kEquivalent']
                        ticker_data['currentDrawdownStart'] = day
                        ticker_data['currentDrawdownEnd'] = day
                        ticker_data['currentDrawdown'] = Decimal(0)
                    else:
                        ticker_data['lastPeakTtwr'] = prev_ticker_data['lastPeakTtwr']
                        ticker_data['lastPeak10kEquivalent'] = prev_ticker_data['lastPeak10kEquivalent']
                        ticker_data['currentDrawdownStart'] = prev_ticker_data['currentDrawdownStart']
                        ticker_data['currentDrawdown'] = min((ticker_data['10kEquivalent'] - ticker_data['lastPeak10kEquivalent']) /
                                                             ticker_data['lastPeak10kEquivalent'], prev_ticker_data['currentDrawdown'])
                        if ticker_data['currentDrawdown'] <= prev_ticker_data['currentDrawdown']:
                            ticker_data['currentDrawdownEnd'] = day
                        else:
                            ticker_data['currentDrawdownEnd'] = prev_ticker_data['currentDrawdownEnd']

                    if ticker_data['currentDrawdown'] < prev_ticker_data['greatestDrawdown']:
                        ticker_data['greatestDrawdown'] = ticker_data['currentDrawdown']
                        ticker_data['greatestDrawdownStart'] = ticker_data['currentDrawdownStart']
                        ticker_data['greatestDrawdownEnd'] = ticker_data['currentDrawdownEnd']
                    else:
                        ticker_data['greatestDrawdown'] = prev_ticker_data['greatestDrawdown']
                        ticker_data['greatestDrawdownStart'] = prev_ticker_data['greatestDrawdownStart']
                        ticker_data['greatestDrawdownEnd'] = prev_ticker_data['greatestDrawdownEnd']

                    returns_lists.setdefault(ticker, [])
                    if data['open']:
                        returns_lists[ticker].append(ticker_data['dayReturns'])
                    if len(returns_lists[ticker]) > 0:
                        ticker_data['volatility'] = statistics.pstdev(returns_lists[ticker])
                    else:
                        ticker_data['volatility'] = None
                ticker_data['dividends'] = prev_ticker_data.get('dividends', 0) + \
                    self.dividends.get(day, {}).get(ticker, 0)
                ticker_data['dividendReturns'] = ticker_data['dividends'] / ticker_data['positionCost']
                ticker_data['appreciationReturns'] = ticker_data['profitOrLoss'] / ticker_data['positionCost']
                ticker_data['totalReturns'] = ticker_data['dividendReturns'] + ticker_data['appreciationReturns']

            data['marketValue'] += data['cash']
            data['dayProfitOrLoss'] = data['marketValue'] - data['dayDeposits'] - prev['marketValue']
            data['dayReturns'] = data['dayProfitOrLoss'] / prev['marketValue']
            data['profitOrLoss'] = data['marketValue'] - data['totalDeposits']
            data['ttwr'] = (prev['ttwr'] + 1) * (data['dayReturns'] + 1) - 1
            data['10kEquivalent'] = prev['10kEquivalent'] * (1 + data['dayReturns'])

            if data['open']:
                returns_lists.setdefault('all', []).append(data['dayReturns'])
            data['volatility'] = statistics.pstdev(returns_lists['all'])
            data['percentCash'] = data['cash'] / data['marketValue']

            if data['10kEquivalent'] > prev['lastPeak10kEquivalent']:
                data['lastPeakTtwr'] = data['ttwr']
                data['lastPeak10kEquivalent'] = data['10kEquivalent']
                data['currentDrawdownStart'] = day
                data['currentDrawdownEnd'] = day
                data['currentDrawdown'] = Decimal(0)
            else:
                data['lastPeakTtwr'] = prev['lastPeakTtwr']
                data['lastPeak10kEquivalent'] = prev['lastPeak10kEquivalent']
                data['currentDrawdownStart'] = prev['currentDrawdownStart']
                data['currentDrawdown'] = min((data['10kEquivalent'] - data['lastPeak10kEquivalent']) /
                                              data['lastPeak10kEquivalent'], prev['currentDrawdown'])
                if data['currentDrawdown'] <= prev['currentDrawdown']:
                    data['currentDrawdownEnd'] = day
                else:
                    data['currentDrawdownEnd'] = prev['currentDrawdownEnd']

            if data['currentDrawdown'] < prev['greatestDrawdown']:
                data['greatestDrawdown'] = data['currentDrawdown']
                data['greatestDrawdownStart'] = data['currentDrawdownStart']
                data['greatestDrawdownEnd'] = data['currentDrawdownEnd']
            else:
                data['greatestDrawdown'] = prev['greatestDrawdown']
                data['greatestDrawdownStart'] = prev['greatestDrawdownStart']
                data['greatestDrawdownEnd'] = prev['greatestDrawdownEnd']

            if data['volatility'] > 0:
                data['sharpe'] = (data['ttwr'] - Decimal(days_from_start / 365) * config.sharpe) / data['volatility']
            else:
                data['sharpe'] = Decimal(0)

            # One more pass for percentages
            for ticker, ticker_data in data['assets'].items():
                ticker_data['percentPortfolio'] = ticker_data['marketValue'] / data['marketValue']

    def calculate_deposit_performance(self):
        yesterday = date.today() - timedelta(days=1)
        yesterday_perf = self.performance[yesterday]
        for day, deposit in self.deposits.items():
            deposit['returns'] = (1 + yesterday_perf['ttwr']) / (1 + self.performance[day]['ttwr']) - 1
            deposit['currentValue'] = deposit['amount'] * (1 + deposit['returns'])
            deposit['cagr'] = (1 + deposit['returns']) ** Decimal(1 / (((yesterday - day).days + 1) / 365)) - 1

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

    def calculate_correlations(self):
        for first, second in combinations(self.tickers, 2):
            first_list = []
            second_list = []
            for day_prices in self.prices.values():
                if first in day_prices.keys() and second in day_prices.keys():
                    first_list.append(float(day_prices[first]))
                    second_list.append(float(day_prices[second]))
            self.correlations.setdefault(first, {})
            self.correlations.setdefault(second, {})
            corr = pearsonr(first_list, second_list)[0]
            self.correlations[first][second] = corr
            self.correlations[second][first] = corr


class DataError(Exception):
    pass
