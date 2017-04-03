from datetime import date
import pandas as pd

from db import db


class Ticker():
    """DataFrame-based structure of ticker pricing data.

    Public methods:
    price(date) -- Return the closing price of the ticker on this date
    change(date) -- Return the percentage price change from the date before

    Instance variables:
    ticker -- Name of the ticker
    prices -- DataFrame indexed by day, with:
        - a `price` Decimal column with the ticker's closing price
        - a `change` Decimal column with the percentage price change from the day before
    """

    def price(self, day):
        """Report the closing price for this ticker on the requested day.

        Keyword arguments:
        day -- the requested day, in datetime.date format

        Returns:
        Decimal -- the closing price on the requested day, or None if out of scope
        """
        try:
            return self.prices.loc[day]['price']
        except KeyError:
            return None

    def change(self, day):
        """Report the percentage price change from the date before.

        Keyword arguments:
        day -- the requested day, in datetime.date format

        Returns:
        Float -- the percentage price change, or NaN if one could not be calculated
        """
        try:
            return self.prices.loc[day]['change']
        except KeyError:
            return None

    def __init__(self, ticker_name, from_day=None):
        """Instantiate a Ticker object.

        The Ticker object will contain the closing prices of `ticker_name`
        for all dates since `from_day` until yesterday.
        As is convention elsewhere in the code, a `from_day` value of None
        will extract all available dates and prices for this ticker until yesterday.
        This class will fill in prices for dates where the market is closed,
        using the last available closing price.

        Keyword arguments:
        ticker_name -- name of the ticker
        from_day -- the first day in the sequence, in datetime.date format (default None)
        """
        self.ticker_name = ticker_name
        _prices = self._get_prices(ticker_name, from_day)
        _changes = self._calc_changes(_prices)
        self.prices = pd.concat([_prices, _changes], axis=1)

    def __repr__(self):
        return str(self.prices.head())

    def __str__(self):
        return str(self.prices.head())

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

        _prices = self._fill_price_gaps(_prices)
        return _prices

    def _fill_price_gaps(self, _prices):
        """Use the last available price on days where we do not have a closing price."""
        last_price = None
        for _, row in _prices.iterrows():
            if row['price'] is not None:
                last_price = row['price']
            else:
                row['price'] = last_price

        return _prices

    def _calc_changes(self, _prices):
        """Calculate the price changes from the date before."""
        _changes = pd.DataFrame(_prices, columns=['change'])

        prev_price = None
        for day, row in _prices.iterrows():
            if row['price'] is not None:
                if prev_price is not None:
                    _changes.loc[day]['change'] = (row['price'] / prev_price) - 1
                prev_price = row['price']

        return _changes
