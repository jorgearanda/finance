from datetime import date
import pandas as pd

from components.position import Position
from components.tickers import Tickers
from db import db


class Positions():
    """DataFrame-based structure that wraps several Position objects

    Public methods:
    calc_weights -- Trigger weight calculations for all positions in this object

    Instance variables:
    account -- Name of the account for these positions. All accounts, if None
    ticker_names -- List with the ticker names contained in the object
    positions -- Dict with ticker names as keys and Position objects as values
    units -- DataFrame, day-indexed, one position per column. Daily number of units held in the position
    costs -- DataFrame, day-indexed, one position per column. Total cost of the units held in the position this day
    costs_per_unit -- DataFrame, day-indexed, one position per column. Average cost of the units held in the position
    current_prices -- DataFrame, day-indexed, one position per column. The position's ticker closing price this day
    market_values -- DataFrame, day-indexed, one position per column. The market value of the position this day
    open_profits -- DataFrame, day-indexed, one position per column. Unrealized appreciation profits this day
    distributions -- DataFrame, day-indexed, one position per column. Cash values received for the position to this day
    distribution_returns -- DataFrame, day-indexed, one position per column. Cumulative distributions over position cost
    appreciation_returns -- DataFrame, day-indexed, one position per column. Appreciation returns over position cost
    total_returns -- DataFrame, day-indexed, one position per column. Appreciation and distribution returns
    weight -- DataFrame, day-indexed, one position per column, plus cash. Weight of the position
    """

    def calc_weights(self, total_values):
        """Trigger weight calculations for all positions held in this object."""
        for _, position in self.positions.items():
            position.calc_weight(total_values)
        self.weights = self._collect_feature('weight')
        self.weights['Cash'] = 1 - self.weights.sum(axis=1)

    def __init__(self, account=None, from_day=None, tickers=None):
        """Instantiate a Positions object, with dates starting on from_day."""
        self.account = account
        if not tickers:
            tickers = Tickers(from_day)
        self.ticker_names = tickers.ticker_names
        self.positions = {
            name: Position(name, account=account, from_day=from_day, ticker=tickers.tickers[name])
            for name in self.ticker_names
        }
        if len(self.ticker_names) > 0:
            self.units = self._collect_feature('units')
            self.costs = self._collect_feature('cost')
            self.costs_per_unit = self._collect_feature('cost_per_unit')
            self.current_prices = self._collect_feature('current_price')
            self.market_values = self._collect_feature('market_value')
            self.open_profits = self._collect_feature('open_profit')
            self.distributions = self._collect_feature('distributions')
            self.distribution_returns = self._collect_feature('distribution_returns')
            self.appreciation_returns = self._collect_feature('appreciation_returns')
            self.total_returns = self._collect_feature('total_returns')

    def __repr__(self):
        return str(self.positions)

    def __str__(self):
        return str(self.positions)

    def _collect_feature(self, feature):
        """Extract a feature (a column) from several position objects and collect them in a single DataFrame."""
        _feature = pd.concat([self.positions[name].values[feature] for name in self.ticker_names], axis=1)
        _feature.columns = self.ticker_names
        return _feature
