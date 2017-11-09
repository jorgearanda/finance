from datetime import date
import numpy as np
import pandas as pd

from components.ticker import Ticker
from db import db


class Position():
    """DataFrame-based structure of portfolio positions.

    Public methods:
    units(date) -- Return the number of units held in this position for this day
    cost(date) -- Return the total cost of the units held in this position for this day
    cost_per_unit(date) -- Return the average cost of the units held in this position on this day
    current_price(date) -- Return this position ticker's closing price on this day
    market_value(date) -- Return the market value of the units held in this position on this day
    open_profit(date) -- Return the unrealized appreciation profits from the units held in this position on this day
    distributions(date) -- Return the cash value received from distributions on this position up to this day
    distribution_returns(date) -- Return the accumulated distributions over the cost of the position to this day
    appreciation_returns(date) -- Return the unrealized appreciation profits over the cost of the position to this day
    total_returns(date) -- Return the total returns (distribution and appreciation) of the position to this day

    Instance variables:
    account -- Name of the account for this position. All accounts, if None
    ticker_name -- Name of the ticker
    values -- DataFrame indexed by day, with:
        - a `units` float column with the held units of this ticker on this date
        - a `cost` float column with the price paid in total for the position
        - a `cost_per_unit` float column with the price paid per unit on average
        - a `current_price` float column with the ticker's closing price on this date
        - a `market_value` float column with the market value of the position on this date
        - an `open_profit' float column with the unrealized appreciation profits from this position
        - a `distributions` float column with the cash value received from distributions on this position
        - a `distribution_returns` float column representing distributions / cost
        - an `appreciation_returns` float column representing open_profit / cost
        - a `total_returns` float column representing distribution_returns + appreciation_returns
    """

    def units(self, day):
        """Return the number of units held in this position for this day."""
        try:
            return self.values['units'][day]
        except KeyError:
            return None

    def cost(self, day):
        """Return the total cost of the units held in this position for this day."""
        try:
            return self.values['cost'][day]
        except KeyError:
            return None

    def cost_per_unit(self, day):
        """Return the average cost of the units held in this position on this day."""
        try:
            return self.values['cost_per_unit'][day]
        except KeyError:
            return None

    def current_price(self, day):
        """Return the position ticker's closing price on this day."""
        try:
            return self.values['current_price'][day]
        except KeyError:
            return None

    def market_value(self, day):
        """Return the market value of the units held in this position on this day."""
        try:
            return self.values['market_value'][day]
        except KeyError:
            return None

    def open_profit(self, day):
        """Return the unrealized appreciation profits from the units held in this position on this day."""
        try:
            return self.values['open_profit'][day]
        except KeyError:
            return None

    def distributions(self, day):
        """Return the cash value received from distributions on this position up to this day."""
        try:
            return self.values['distributions'][day]
        except KeyError:
            return None

    def distribution_returns(self, day):
        """Return the accumulated distributions over the cost of the position to this day."""
        try:
            return self.values['distribution_returns'][day]
        except KeyError:
            return None

    def appreciation_returns(self, day):
        """Return the unrealized appreciation profits over the cost of the position to this day."""
        try:
            return self.values['appreciation_returns'][day]
        except KeyError:
            return None

    def total_returns(self, day):
        """Return the total returns (distribution and appreciation) of the position to this day."""
        try:
            return self.values['total_returns'][day]
        except KeyError:
            return None

    def __init__(self, ticker_name, account=None, from_day=None, ticker=None):
        """Instantiate a Position object."""
        self.ticker_name = ticker_name
        self.account = account
        self.from_day = from_day
        if ticker:
            self._ticker = ticker
        else:
            self._ticker = Ticker(ticker_name, from_day)

        self.values = self._get_daily_values()

    def __repr__(self):
        return str(self.values.head())

    def __str__(self):
        return str(self.values.head())

    def _get_daily_values(self):
        """Create a DataFrame with daily position data."""
        db.ensure_connected()
        position_data = pd.read_sql_query('''
            WITH buys AS
                (SELECT SUM(units)::int AS units, SUM(total)::double precision AS total, day
                FROM transactions
                WHERE (%(account)s IS NULL OR account = %(account)s)
                    AND (%(from_day)s IS NULL OR day >= %(from_day)s)
                    AND txtype = 'buy'
                    AND target = %(ticker_name)s
                GROUP BY day
                ORDER BY day ASC),
            dividends AS
                (SELECT SUM(total)::double precision AS total, day
                FROM transactions
                WHERE (%(account)s IS NULL OR account = %(account)s)
                    AND (%(from_day)s IS NULL OR day >= %(from_day)s)
                    AND txtype = 'dividend'
                    AND source = %(ticker_name)s
                GROUP BY day
                ORDER BY day ASC)
            SELECT m.day, buys.units, buys.total AS cost, dividends.total AS distributions
            FROM marketdays m LEFT JOIN buys USING (day)
            LEFT JOIN dividends USING (day)
            WHERE (%(from_day)s IS NULL OR m.day >= %(from_day)s) AND m.day < %(today)s
            ORDER BY m.day ASC;''',
            con=db.conn,
            params={
                'account': self.account,
                'ticker_name': self.ticker_name,
                'from_day': self.from_day,
                'today': date.today()
            },
            index_col='day',
            parse_dates=['day'])

        position_data['units'] = position_data['units'].fillna(0).cumsum()
        position_data['cost'] = position_data['cost'].fillna(0).cumsum()
        position_data['distributions'] = position_data['distributions'].fillna(0).cumsum()
        position_data['current_price'] = self._ticker.values['price']
        position_data['cost_per_unit'] = position_data['cost'] / position_data['units']
        position_data['market_value'] = position_data['units'] * position_data['current_price']
        position_data['open_profit'] = position_data['market_value'] - position_data['cost']
        position_data['appreciation_returns'] = position_data['open_profit'] / position_data['cost']
        position_data['distribution_returns'] = position_data['distributions'] / position_data['cost']
        position_data['total_returns'] = \
            position_data['distribution_returns'].fillna(0) + position_data['appreciation_returns'].fillna(0)

        return position_data
