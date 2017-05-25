import pandas as pd


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

    def __init__(self, ticker_name, from_day=None):
        """Instantiate a Position object."""
        self.ticker_name = ticker_name
        # TODO - the rest of the initialization
        self.values = pd.DataFrame()

    def __repr__(self):
        return str(self.values.head())

    def __str__(self):
        return str(self.values.head())
