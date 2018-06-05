from datetime import date
import pandas as pd

from components.ticker import Ticker
from db import db
from util.determine_accounts import determine_accounts


class Position():
    """DataFrame-based structure of portfolio positions.

    Public methods:
    units(date) -- Return the number of units held
    cost(date) -- Return the total cost of the units held
    cost_per_unit(date) -- Return the average cost of the units held
    current_price(date) -- Return this position ticker's closing price
    market_value(date) -- Return the market value of the units held
    open_profit(date) -- Return the unrealized appreciation profits
    distributions(date) -- Return the cash value received from distributions
    distribution_returns(date) -- Return the returns from accumulated
                                  distributions over the cost of the position
    appreciation_returns(date) -- Return the returns from
                                  unrealized appreciation profits
    total_returns(date) -- Return the total returns
    weight(date) -- Return the weight of this position on the whole portfolio

    Instance variables:
    accounts -- Name of the accounts for this position. All accounts, if None
    ticker_name -- Name of the ticker
    values -- DataFrame indexed by day, with:
        - a `units` float column, held units of this ticker
        - a `cost` float column, price paid in total for the position
        - a `cost_per_unit` float column, price paid per unit on average
        - a `current_price` float column, ticker's closing price
        - a `market_value` float column, market value of the position
        - an `open_profit' float column, unrealized appreciation profits
        - a `distributions` float column, cash received from distributions
        - a `distribution_returns` float column, distributions / cost
        - an `appreciation_returns` float column, open_profit / cost
        - a `total_returns` float column, distribution and appreciation returns
        - a `weight` float column, weight of this position on the portfolio
    """

    def calc_weight(self, daily_totals):
        """Calculate the weight of this position as a part of the portfolio."""
        self.values['weight'] = \
            (self.values['market_value'] / daily_totals).fillna(0.00)

    def units(self, day):
        """Return the number of units held."""
        try:
            return self.values['units'][day]
        except KeyError:
            return None

    def cost(self, day):
        """Return the total cost of the units held."""
        try:
            return self.values['cost'][day]
        except KeyError:
            return None

    def cost_per_unit(self, day):
        """Return the average cost of the units held."""
        try:
            return self.values['cost_per_unit'][day]
        except KeyError:
            return None

    def current_price(self, day):
        """Return the position ticker's closing price."""
        try:
            return self.values['current_price'][day]
        except KeyError:
            return None

    def market_value(self, day):
        """Return the market value of the units held."""
        try:
            return self.values['market_value'][day]
        except KeyError:
            return None

    def open_profit(self, day):
        """Return the unrealized appreciation profits from the units held."""
        try:
            return self.values['open_profit'][day]
        except KeyError:
            return None

    def distributions(self, day):
        """Return the cash received from distributions on this position."""
        try:
            return self.values['distributions'][day]
        except KeyError:
            return None

    def distribution_returns(self, day):
        """Return the returns from accumulated distributions."""
        try:
            return self.values['distribution_returns'][day]
        except KeyError:
            return None

    def appreciation_returns(self, day):
        """Return the returns from unrealized appreciation profits."""
        try:
            return self.values['appreciation_returns'][day]
        except KeyError:
            return None

    def total_returns(self, day):
        """Return the total returns (distribution and appreciation)."""
        try:
            return self.values['total_returns'][day]
        except KeyError:
            return None

    def weight(self, day):
        """Return the weight of the position as a percentage of portfolio."""
        try:
            return self.values['weight'][day]
        except KeyError:
            return None

    def __init__(self, ticker_name, accounts=None, from_day=None, ticker=None):
        """Instantiate a Position object."""
        self.ticker_name = ticker_name
        self.accounts = determine_accounts(accounts)
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
        df = pd.read_sql_query(
            '''WITH buys AS
                (SELECT SUM(units)::int AS units,
                    SUM(total)::double precision AS total, day
                FROM transactions
                WHERE account = ANY(%(accounts)s)
                    AND (%(from_day)s IS NULL OR day >= %(from_day)s)
                    AND txtype = 'buy'
                    AND target = %(ticker_name)s
                GROUP BY day
                ORDER BY day ASC),
            dividends AS
                (SELECT SUM(total)::double precision AS total, day
                FROM transactions
                WHERE account = ANY(%(accounts)s)
                    AND (%(from_day)s IS NULL OR day >= %(from_day)s)
                    AND txtype = 'dividend'
                    AND source = %(ticker_name)s
                GROUP BY day
                ORDER BY day ASC)
            SELECT m.day, buys.units, buys.total AS cost,
                dividends.total AS distributions
            FROM marketdays m LEFT JOIN buys USING (day)
            LEFT JOIN dividends USING (day)
            WHERE (%(from_day)s IS NULL OR m.day >= %(from_day)s)
                AND m.day <= %(today)s
            ORDER BY m.day ASC;''',
            con=db.conn,
            params={
                'accounts': self.accounts,
                'ticker_name': self.ticker_name,
                'from_day': self.from_day,
                'today': date.today()
            },
            index_col='day',
            parse_dates=['day'])

        df['units'] = df['units'].fillna(0).cumsum()
        df['cost'] = df['cost'].fillna(0).cumsum()
        df['distributions'] = df['distributions'].fillna(0).cumsum()
        df['current_price'] = self._ticker.values['price']
        df['cost_per_unit'] = df['cost'] / df['units']
        df['market_value'] = df['units'] * df['current_price']
        df['open_profit'] = df['market_value'] - df['cost']
        df['appreciation_returns'] = df['open_profit'] / df['cost']
        df['distribution_returns'] = df['distributions'] / df['cost']
        df['total_returns'] = \
            df['distribution_returns'].fillna(0) + \
            df['appreciation_returns'].fillna(0)

        return df
