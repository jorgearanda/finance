from collections import OrderedDict
from decimal import Decimal
from itertools import combinations
from math import sqrt


class Assets():
    def __init__(self, conn):
        self.conn = conn
        self.tickers = set()
        self.prices = OrderedDict()
        self.correlations = OrderedDict()
        self.load()

    def load(self):
        self.load_prices()
        self.calculate_correlations()

    def price(self, day, ticker):
        return self.prices.get(day, {}).get(ticker)

    def corr(self, ticker1, ticker2=None):
        if ticker2 is None:
            return self.correlations[ticker1]
        else:
            return self.correlations[ticker1][ticker2]

    def load_prices(self):
        with self.conn.cursor() as cur:
            cur.execute('''
                SELECT ticker, day, ask
                FROM assetprices
                ORDER BY day;''')

            for price in cur.fetchall():
                self.tickers.add(price.ticker)
                self.prices.setdefault(price.day, {})
                self.prices[price.day][price.ticker] = price.ask

    def calculate_correlations(self):
        for ticker1, ticker2 in combinations(self.tickers, 2):
            pairs = 0
            ticker1_sum = Decimal(0)
            ticker1_squared = Decimal(0)
            ticker2_sum = Decimal(0)
            ticker2_squared = Decimal(0)
            ticker_product_sum = Decimal(0)
            for day_prices in self.prices.values():
                if ticker1 in day_prices.keys() and ticker2 in day_prices.keys():
                    pairs += 1
                    ticker1_sum += day_prices[ticker1]
                    ticker1_squared += day_prices[ticker1] ** 2
                    ticker2_sum += day_prices[ticker2]
                    ticker2_squared += day_prices[ticker2] ** 2
                    ticker_product_sum += day_prices[ticker1] * day_prices[ticker2]

            self.correlations.setdefault(ticker1, {})
            self.correlations.setdefault(ticker2, {})
            corr = (pairs * ticker_product_sum - (ticker1_sum * ticker2_sum)) / \
                Decimal(sqrt((pairs * ticker1_squared - ticker1_sum ** 2) * (pairs * ticker2_squared - ticker2_sum ** 2)))
            self.correlations[ticker1][ticker2] = corr
            self.correlations[ticker2][ticker1] = corr
