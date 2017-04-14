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
        - a `price` float column with the ticker's closing price
        - a `market_value` float column with the market value of the position on this date
    """

    def __init__(self, ticker_name, from_day=None):
        """Instantiate a Position object.

        The Position object will contain data on a portfolio's position of `ticker_name`
        for all dates since `from_day` until yesterday.

        Keyword arguments:
        ticker_name -- name of the ticker
        from_day -- the first day in the sequence, in datetime.date format (default None)
        """
        self.ticker_name = ticker_name
        # TODO - the rest of the initialization
        self.values = pd.DataFrame()

    def __repr__(self):
        return str(self.values.head())

    def __str__(self):
        return str(self.values.head())
