from collections import OrderedDict
from itertools import combinations
from scipy.stats import pearsonr


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
            ticker1_list = []
            ticker2_list = []
            for day_prices in self.prices.values():
                if ticker1 in day_prices.keys() and ticker2 in day_prices.keys():
                    ticker1_list.append(float(day_prices[ticker1]))
                    ticker2_list.append(float(day_prices[ticker2]))
            self.correlations.setdefault(ticker1, {})
            self.correlations.setdefault(ticker2, {})
            corr = pearsonr(ticker1_list, ticker2_list)[0]
            self.correlations[ticker1][ticker2] = corr
            self.correlations[ticker2][ticker1] = corr
