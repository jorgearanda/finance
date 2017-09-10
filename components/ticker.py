from datetime import date
import pandas as pd

from components.market_days import MarketDays
from db import db


class Ticker():
    """DataFrame-based structure of ticker pricing data.

    Public methods:
    price(date) -- Return the closing price of the ticker on this date
    change(date) -- Return the percentage price change from the date before
    change_from_start(date) -- Return the percentage price change from the initial value
    distribution(date) -- Return the per-unit distribution on the requested date
    distributions_from_start(date) -- Return the accumulated per-unit cash distributions until the requested date
    yield_from_start(date) -- Return the yield (distributions over price) from the start date until the requested one
    returns(date) -- Return the percentage of returns to this date, considering appreciation and distributions

    Instance variables:
    ticker_name -- Name of the ticker
    values -- DataFrame indexed by day, with:
        - a `price` float column with the ticker's closing price
        - a `change` float column with the percentage price change from the day before
        - a `change_from_start` float column with the percentage price change from the initial value
        - a `distributions` float column with per-unit distributions on this date
        - a `distributions_from_start` float column with accumulated distributions per unit
        - a `yield_from_start` float column with the total percentage yield on this date
        - a `returns` float column with the returns (as percentage) considering appreciations and distributions
    volatility -- Standard deviation of the price changes (that is, of the `change` series)
    """

    def price(self, day):
        """Report the closing price for this ticker on the requested day."""
        try:
            return self.values['price'][day]
        except KeyError:
            return None

    def change(self, day):
        """Report the percentage price change from the date before."""
        try:
            return self.values['change'][day]
        except KeyError:
            return None

    def change_from_start(self, day):
        """Report the percentage price change from the initial value."""
        try:
            return self.values['change_from_start'][day]
        except KeyError:
            return None

    def distribution(self, day):
        """Report the per-unit cash distribution on the requested date."""
        try:
            return self.values['distribution'][day]
        except KeyError:
            return None

    def distributions_from_start(self, day):
        """Report the accumulated per-unit cash distribution up to the requested date."""
        try:
            return self.values['distributions_from_start'][day]
        except KeyError:
            return None

    def yield_from_start(self, day):
        """Report the per-unit yield on the requested date."""
        try:
            return self.values['yield_from_start'][day]
        except KeyError:
            return None

    def returns(self, day):
        """Report the per-unit total returns as a percentage, considering appreciation and distributions."""
        try:
            return self.values['returns'][day]
        except KeyError:
            return None

    def __init__(self, ticker_name, from_day=None):
        """Instantiate a Ticker object."""
        self.ticker_name = ticker_name
        self.from_day = from_day
        _days = MarketDays(from_day).market_days
        self.values = pd.DataFrame(_days, index=_days.index, columns=['open'])
        self.values['price'] = self._get_prices()
        self.values['change'] = (self.values['price'] / self.values['price'].shift(1)) - 1.0
        first_price_index = self.values['price'].first_valid_index()
        if first_price_index:
            self.values['change_from_start'] = (self.values['price'] / self.values['price'][first_price_index]) - 1.0
        else:  # there are no valid prices!
            self.values['change_from_start'] = 0.0
        self.values['distribution'] = self._get_distributions()
        self.values['distributions_from_start'] = self.values['distribution'].cumsum()
        self.values['yield_from_start'] = self.values['distributions_from_start'] / self.values['price']
        self.values['returns'] = self.values['change_from_start'] + self.values['yield_from_start']
        self.volatility = self._get_volatility()

    def __repr__(self):
        return str(self.values.head())

    def __str__(self):
        return str(self.values.head())

    def _get_prices(self):
        """Create a Series with the ticker's closing prices."""
        db.ensure_connected()
        with db.conn.cursor() as cur:
            cur.execute('''
                SELECT p.close::double precision AS price, m.day
                FROM marketdays m LEFT JOIN assetprices p USING (day)
                WHERE (p.ticker IS NULL OR p.ticker = %(ticker_name)s)
                AND (%(from_day)s IS NULL OR m.day >= %(from_day)s) AND m.day < %(today)s
                ORDER BY m.day ASC;''',
                {'ticker_name': self.ticker_name, 'from_day': self.from_day, 'today': date.today()})

            if cur.rowcount > 0:
                records = list(zip(*cur.fetchall()))  # two sublists: the first of prices, the second of days
            else:
                records = [None, None]

        _prices = pd.Series(records[0], index=records[1], dtype=float)
        _prices.fillna(method='ffill', inplace=True)

        return _prices

    def _get_distributions(self):
        """Create a dataframe with the ticker's per-unit cash distributions."""
        db.ensure_connected()
        with db.conn.cursor() as cur:
            cur.execute('''
                WITH tickerdistributions AS
                    (SELECT day, amount::double precision FROM distributions WHERE ticker = %(ticker_name)s)
                SELECT COALESCE(d.amount, 0) AS distribution, m.day
                FROM marketdays m LEFT JOIN tickerdistributions d USING (day)
                WHERE (%(from_day)s IS NULL OR m.day >= %(from_day)s) AND m.day < %(today)s
                ORDER BY m.day ASC;''',
                {'ticker_name': self.ticker_name, 'from_day': self.from_day, 'today': date.today()})

            if cur.rowcount > 0:
                records = list(zip(*cur.fetchall()))  # two sublists: the first of distributions, the second of days
            else:
                records = [None, None]

        return pd.Series(records[0], index=records[1], dtype=float)

    def _get_volatility(self):
        """Calculate ticker price volatility."""
        if self.values.empty:
            return None
        else:
            return self.values[(self.values.open)]['change'].std(axis=0)
