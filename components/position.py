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
            return self.values.loc[day]['units']
        except KeyError:
            return None

    def cost(self, day):
        """Return the total cost of the units held in this position for this day."""
        try:
            return self.values.loc[day]['cost']
        except KeyError:
            return None

    def cost_per_unit(self, day):
        """Return the average cost of the units held in this position on this day."""
        try:
            return self.values.loc[day]['cost_per_unit']
        except KeyError:
            return None

    def current_price(self, day):
        """Return the position ticker's closing price on this day."""
        try:
            return self.values.loc[day]['current_price']
        except KeyError:
            return None

    def market_value(self, day):
        """Return the market value of the units held in this position on this day."""
        try:
            return self.values.loc[day]['market_value']
        except KeyError:
            return None

    def open_profit(self, day):
        """Return the unrealized appreciation profits from the units held in this position on this day."""
        try:
            return self.values.loc[day]['open_profit']
        except KeyError:
            return None

    def distributions(self, day):
        """Return the cash value received from distributions on this position up to this day."""
        try:
            return self.values.loc[day]['distributions']
        except KeyError:
            return None

    def distribution_returns(self, day):
        """Return the accumulated distributions over the cost of the position to this day."""
        try:
            return self.values.loc[day]['distribution_returns']
        except KeyError:
            return None

    def appreciation_returns(self, day):
        """Return the unrealized appreciation profits over the cost of the position to this day."""
        try:
            return self.values.loc[day]['appreciation_returns']
        except KeyError:
            return None

    def total_returns(self, day):
        """Return the total returns (distribution and appreciation) of the position to this day."""
        try:
            return self.values.loc[day]['total_returns']
        except KeyError:
            return None

    def __init__(self, ticker_name, account=None, from_day=None, ticker=None):
        """Instantiate a Position object."""
        self.ticker_name = ticker_name
        self.account = account
        self._from_day = from_day
        if ticker:
            self._ticker = ticker
        else:
            self._ticker = Ticker(ticker_name, from_day)
        self.values = pd.DataFrame(
            index=self._ticker.values.index,
            columns=['units', 'cost', 'cost_per_unit', 'current_price', 'market_value', 'open_profit', 'distributions',
                'distribution_returns', 'appreciation_returns', 'total_returns'])

        self.values['current_price'] = self._ticker.values['price']
        self._get_units_and_costs()
        self._calc_appreciations()
        self._get_distributions()
        self._calc_total_returns()

    def __repr__(self):
        return str(self.values.head())

    def __str__(self):
        return str(self.values.head())

    def _get_units_and_costs(self):
        db.ensure_connected()
        with db.conn.cursor() as buys:
            buys.execute('''
                SELECT day, units, total
                FROM transactions
                WHERE (%(account)s IS NULL OR account = %(account)s)
                    AND (%(from_day)s IS NULL OR day >= %(from_day)s)
                    AND txtype = 'buy'
                    AND target = %(ticker_name)s
                ORDER BY day;''',
                {
                    'account': self.account,
                    'from_day': self._from_day,
                    'ticker_name': self.ticker_name
                })

            next_buy = buys.fetchone()
            last_units = 0.0
            last_cost = 0.0

            for day, row in self.values.iterrows():
                self.values.loc[day, 'units'] = last_units
                self.values.loc[day, 'cost'] = last_cost
                while next_buy is not None and day == next_buy.day:
                    self.values.loc[day, 'units'] += float(next_buy.units)
                    self.values.loc[day, 'cost'] += float(next_buy.total)
                    next_buy = buys.fetchone()

                last_units = self.values.loc[day, 'units']
                last_cost = self.values.loc[day, 'cost']
                self.values.loc[day, 'cost_per_unit'] = float('nan') if last_units == 0 else last_cost / last_units

            self.values['cost'] = self.values['cost'].astype('float')

    def _calc_appreciations(self):
        self.values['market_value'] = self.values['units'] * self.values['current_price']
        self.values['open_profit'] = self.values['market_value'] - self.values['cost']
        self.values['open_profit'] = self.values['open_profit'].astype('float')
        self.values['appreciation_returns'] = self.values['open_profit'] / self.values['cost']

    def _get_distributions(self):
        db.ensure_connected()
        with db.conn.cursor() as dividends:
            dividends.execute('''
                SELECT day, total
                FROM transactions
                WHERE (%(account)s IS NULL OR account = %(account)s)
                    AND (%(from_day)s IS NULL OR day >= %(from_day)s)
                    AND txtype = 'dividend'
                    AND source = %(ticker_name)s
                ORDER BY day;''',
                {
                    'account': self.account,
                    'from_day': self._from_day,
                    'ticker_name': self.ticker_name
                })

            next_dividend = dividends.fetchone()
            last_distribution = 0.0

            for day, row in self.values.iterrows():
                self.values.loc[day, 'distributions'] = last_distribution
                while next_dividend is not None and day == next_dividend.day:
                    self.values.loc[day, 'distributions'] += float(next_dividend.total)
                    next_dividend = dividends.fetchone()

                last_distribution = self.values.loc[day, 'distributions']
                if self.values.loc[day, 'cost'] == 0:
                    self.values.loc[day, 'distribution_returns'] = 0
                else:
                    self.values.loc[day, 'distribution_returns'] = last_distribution / self.values.loc[day, 'cost']

    def _calc_total_returns(self):
        self.values['total_returns'] = \
            self.values.fillna(0)['distribution_returns'] + self.values.fillna(0)['appreciation_returns']
