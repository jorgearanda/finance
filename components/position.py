import pandas as pd

from components.ticker import Ticker
from db import db


class Position():
    """DataFrame-based structure of portfolio positions.

    Public methods:
    --none yet--

    Instance variables:
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

    def __init__(self, ticker_name, account=None, from_day=None):
        """Instantiate a Position object."""
        self.ticker_name = ticker_name
        self.account = account
        self._from_day = from_day
        _ticker = Ticker(ticker_name, from_day)
        _current_prices = _ticker.values['price']
        _units_and_costs = self._get_units_and_costs(_ticker.values.index)
        _costs_per_unit = self._calc_costs_per_unit(_units_and_costs)
        _appreciations = self._calc_appreciations(_ticker, _units_and_costs)
        _distributions = self._get_distributions(_units_and_costs)

        self.values = pd.DataFrame({
            'current_price': _current_prices,
            'units': _units_and_costs['units'],
            'cost': _units_and_costs['cost'],
            'cost_per_unit': _costs_per_unit['cost_per_unit'],
            'market_value': _appreciations['market_value'],
            'open_profit': _appreciations['open_profit'],
            'distributions': _distributions['distributions'],
            'distribution_returns': _distributions['distribution_returns'],
            'appreciation_returns': _appreciations['appreciation_returns'],
            'total_returns': _distributions.fillna(0)['distribution_returns'] + _appreciations['appreciation_returns']
        })

    def __repr__(self):
        return str(self.values.head())

    def __str__(self):
        return str(self.values.head())

    def _say_hi(self):
        print('hi')

    def _get_units_and_costs(self, by_day):
        frame = pd.DataFrame(index=by_day, columns=['units', 'cost'])
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

            for day, row in frame.iterrows():
                row['units'] = last_units
                row['cost'] = last_cost
                while next_buy is not None and day == next_buy.day:
                    row['units'] += float(next_buy.units)
                    row['cost'] += float(next_buy.total)
                    next_buy = buys.fetchone()

                last_units = row['units']
                last_cost = row['cost']
                row['cost_per_unit'] = float('nan') if last_units == 0 else last_cost / last_units

        return frame

    def _calc_costs_per_unit(self, _units_and_costs):
        _costs_per_unit = pd.DataFrame(_units_and_costs, columns=['cost_per_unit'])
        for day, row in _units_and_costs.iterrows():
            _costs_per_unit.loc[day]['cost_per_unit'] = \
                float('nan') if row['units'] == 0 else row['cost'] / row['units']

        return _costs_per_unit

    def _calc_appreciations(self, _ticker, _units_and_costs):
        _appreciations = pd.DataFrame(
            index=_units_and_costs.index,
            columns=['market_value', 'open_profit', 'appreciation_returns'])
        for day, row in _units_and_costs.iterrows():
            _appreciations.loc[day]['market_value'] = row['units'] * _ticker.price(day)
            _appreciations.loc[day]['open_profit'] = _appreciations.loc[day]['market_value'] - row['cost']
            _appreciations.loc[day]['appreciation_returns'] = \
                float('nan') if row['cost'] == 0 else _appreciations.loc[day]['open_profit'] / row['cost']

        return _appreciations

    def _get_distributions(self, _units_and_costs):
        _distributions = pd.DataFrame(index=_units_and_costs.index, columns=['distributions', 'distribution_returns'])

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

            for day, row in _distributions.iterrows():
                row['distributions'] = last_distribution
                while next_dividend is not None and day == next_dividend.day:
                    row['distributions'] += float(next_dividend.total)
                    next_dividend = dividends.fetchone()

                last_distribution = row['distributions']
                row['distribution_returns'] = \
                    float('nan') if last_distribution == 0 else last_distribution / _units_and_costs.loc[day]['cost']

        return _distributions
