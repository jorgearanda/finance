from datetime import date
import pandas as pd

from components.ticker import Ticker
from db import db


class Tickers():
    """DataFrame-based structure that wraps several Ticker objects

    Public methods:
    price(date, ticker_name) -- Price at the given date of the given ticker

    Instance variables:
    ticker_names -- List with the ticker names contained in the object
    tickers -- Dict with ticker names as keys and Ticker objects as values
    prices -- DataFrame, day-indexed, one ticker per column. Daily ticker prices
    changes -- DataFrame, day-indexed, one ticker per column. Daily price changes
    changes_from_start -- DataFrame, day-indexed, one ticker per column. Price changes since the first day
    distributions -- DataFrame, day-indexed, one ticker per column. Per-unit cash distributions
    distributions_from_start -- DataFrame, day-indexed, one ticker per column. Accumulated distributions
    yields_from_start -- DataFrame, day-indexed, one ticker per column. Per-unit yields
    returns -- DataFrame, day-indexed, one ticker per column. Total returns percentage (appreciation + yield)
    volatilities -- Dict of ticker volatilities (standard deviation of price changes)
    correlations -- DataFrame indexed by ticker name with one column per ticker. Values represent the correlation
        between both tickers' prices
    market_open -- Series, day-indexed, with a boolean representing market openings
    """

    def price(self, day, ticker_name):
        """Return the price of ticker_name on the given day."""
        return self.tickers[ticker_name].price(day)

    def __init__(self, from_day=None):
        """Instantiate a Tickers object, with dates starting on from_day."""
        self.ticker_names = self._get_ticker_names(from_day)
        self.tickers = self._get_tickers(from_day)
        if len(self.ticker_names) > 0:
            self.prices = self._collect_feature('price')
            self.changes = self._collect_feature('change')
            self.changes_from_start = self._collect_feature('change_from_start')
            self.distributions = self._collect_feature('distribution')
            self.distributions_from_start = self._collect_feature('distributions_from_start')
            self.yields_from_start = self._collect_feature('yield_from_start')
            self.returns = self._collect_feature('returns')
            self.volatilities = self._collect_volatilities()
            self.correlations = self._calc_correlations()
            self.market_open = self._collect_market_open()

    def __repr__(self):
        return str(self.tickers)

    def __str__(self):
        return str(self.tickers)

    def _get_ticker_names(self, from_day):
        """Get all ticker names for which there are prices available since from_day."""
        db.ensure_connected()
        with db.conn.cursor() as cur:
            cur.execute('''
                SELECT DISTINCT(ticker) AS name
                FROM assetprices
                WHERE (%(from_day)s IS NULL OR day >= %(from_day)s) AND day < %(today)s
                ORDER BY name ASC;''',
                {'from_day': from_day, 'today': date.today()})

            return [x.name for x in cur.fetchall()]

    def _get_tickers(self, from_day):
        """Get all ticker objects."""
        return {name: Ticker(name, from_day) for name in self.ticker_names}

    def _collect_feature(self, feature):
        """Extract a feature (a column) from several position objects and collect them in a single DataFrame."""
        _feature = pd.concat([self.tickers[name].values[feature] for name in self.ticker_names], axis=1)
        _feature.columns = self.ticker_names
        return _feature

    def _collect_volatilities(self):
        """Extract the volatilities from all tickers and collect them in a dictionary."""
        return {name: self.tickers[name].volatility for name in self.ticker_names}

    def _calc_correlations(self):
        """Calculate the correlations between ticker prices."""
        return self.prices.corr()

    def _collect_market_open(self):
        """Extract the Series of open market days from a Ticker object."""
        return self.tickers[self.ticker_names[0]].values['open']