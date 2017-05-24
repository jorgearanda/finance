from datetime import date
import pandas as pd

from components.days import Days
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
            return self.values.loc[day]['price']
        except KeyError:
            return None

    def change(self, day):
        """Report the percentage price change from the date before."""
        try:
            return self.values.loc[day]['change']
        except KeyError:
            return None

    def change_from_start(self, day):
        """Report the percentage price change from the initial value."""
        try:
            return self.values.loc[day]['change_from_start']
        except KeyError:
            return None

    def distribution(self, day):
        """Report the per-unit cash distribution on the requested date."""
        try:
            return self.values.loc[day]['distribution']
        except KeyError:
            return None

    def distributions_from_start(self, day):
        """Report the accumulated per-unit cash distribution up to the requested date."""
        try:
            return self.values.loc[day]['distributions_from_start']
        except KeyError:
            return None

    def yield_from_start(self, day):
        """Report the per-unit yield on the requested date."""
        try:
            return self.values.loc[day]['yield_from_start']
        except KeyError:
            return None

    def returns(self, day):
        """Report the per-unit total returns as a percentage, considering appreciation and distributions."""
        try:
            return self.values.loc[day]['returns']
        except KeyError:
            return None

    def __init__(self, ticker_name, from_day=None):
        """Instantiate a Ticker object."""
        self.ticker_name = ticker_name
        _prices = self._get_prices(ticker_name, from_day)
        _days = Days(from_day).days
        _changes = self._calc_changes(_prices)
        _changes_from_start = self._calc_changes_from_start(_prices)
        _distributions = self._get_distributions(ticker_name, from_day)
        _distributions_from_start = self._calc_distributions_from_start(_distributions)
        _yield_from_start = self._calc_yield_from_start(_prices, _distributions_from_start)
        _returns = self._calc_returns(_changes_from_start, _yield_from_start)
        self.values = pd.concat(
            [_prices, _days, _changes, _changes_from_start,
            _distributions, _distributions_from_start, _yield_from_start, _returns],
            axis=1)
        self.volatility = self._get_volatility()

    def __repr__(self):
        return str(self.values.head())

    def __str__(self):
        return str(self.values.head())

    def _get_prices(self, ticker_name, from_day):
        """Create a dataframe with the ticker's closing prices."""
        db.ensure_connected()
        with db.conn.cursor() as cur:
            cur.execute('''
                SELECT m.day, p.close AS price
                FROM marketdays m LEFT JOIN assetprices p USING (day)
                WHERE (p.ticker IS NULL OR p.ticker = %(ticker_name)s)
                AND (%(from_day)s IS NULL OR m.day >= %(from_day)s) AND m.day < %(today)s
                ORDER BY m.day ASC;''',
                {'ticker_name': ticker_name, 'from_day': from_day, 'today': date.today()})

            _prices = pd.DataFrame(cur.fetchall())
            if not _prices.empty:
                _prices = _prices.set_index('day')
                _prices = _prices.astype('float')

        _prices = self._fill_price_gaps(_prices)
        return _prices

    def _fill_price_gaps(self, _prices):
        """Use the last available price on days where we do not have a closing price."""
        last_price = None
        for _, row in _prices.iterrows():
            if not pd.isnull(row['price']):
                last_price = row['price']
            else:
                row['price'] = last_price

        return _prices

    def _calc_changes(self, _prices):
        """Calculate the price changes from the date before."""
        _changes = pd.DataFrame(_prices, columns=['change'])

        prev_price = None
        for day, row in _prices.iterrows():
            if not pd.isnull(row['price']):
                if prev_price is not None:
                    _changes.loc[day]['change'] = (row['price'] / prev_price) - 1
                prev_price = row['price']

        return _changes

    def _calc_changes_from_start(self, _prices):
        """Calculate the price changes from the first available price."""
        _changes_from_start = pd.DataFrame(_prices, columns=['change_from_start'])

        start_price = None
        for day, row in _prices.iterrows():
            if not pd.isnull(row['price']):
                if start_price is None:
                    start_price = row['price']
                _changes_from_start.loc[day]['change_from_start'] = (row['price'] / start_price) - 1

        return _changes_from_start

    def _get_distributions(self, ticker_name, from_day):
        """Create a dataframe with the ticker's per-unit cash distributions."""
        db.ensure_connected()
        with db.conn.cursor() as cur:
            cur.execute('''
                WITH tickerdistributions AS
                    (SELECT day, amount FROM distributions WHERE ticker = %(ticker_name)s)
                SELECT m.day, COALESCE(d.amount, 0) AS distribution
                FROM marketdays m LEFT JOIN tickerdistributions d USING (day)
                WHERE (%(from_day)s IS NULL OR m.day >= %(from_day)s) AND m.day < %(today)s
                ORDER BY m.day ASC;''',
                {'ticker_name': ticker_name, 'from_day': from_day, 'today': date.today()})

            _distributions = pd.DataFrame(cur.fetchall())
            if not _distributions.empty:
                _distributions = _distributions.set_index('day')
                _distributions = _distributions.astype('float')

        return _distributions

    def _calc_distributions_from_start(self, _distributions):
        """Calculate per-unit accumulated cash distributions."""
        _distributions_from_start = pd.DataFrame(_distributions, columns=['distributions_from_start'])
        amount = 0.0
        for day, row in _distributions.iterrows():
            amount += row['distribution']
            _distributions_from_start.loc[day]['distributions_from_start'] = amount

        return _distributions_from_start

    def _calc_yield_from_start(self, _prices, _d):
        """Calculate per-unit yields from the start date."""
        _yield_from_start = pd.DataFrame(_prices, columns=['yield_from_start'])
        for day, row in _prices.iterrows():
            _yield_from_start.loc[day]['yield_from_start'] = _d.loc[day]['distributions_from_start'] / row['price']

        return _yield_from_start

    def _calc_returns(self, _changes, _yields):
        """Calculate per-unit returns from the start date."""
        _returns = pd.DataFrame(_changes, columns=['returns'])
        for day, row in _changes.iterrows():
            _returns.loc[day]['returns'] = row['change_from_start'] + _yields.loc[day]['yield_from_start']

        return _returns

    def _get_volatility(self):
        """Calculate ticker price volatility."""
        if self.values.empty:
            return None
        else:
            return self.values[(self.values.open)]['change'].std(axis=0)
