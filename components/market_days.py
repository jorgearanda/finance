from datetime import date
import pandas as pd

from db import db


class MarketDays():
    """Series-based structure to keep track of open and closed market days.

    Public methods:
    open(date) -- Report whether the market was open on this date

    Instance variables:
    market_days -- Series, indexed by day, representing which days had markets open
    """

    def open(self, day):
        """Report whether the market was open on the requested day.

        Keyword arguments:
        day -- the requested day, in datetime.date format

        Returns:
        bool -- True if the market was open, False if closed, None if out of scope
        """
        try:
            return self.market_days[day]
        except KeyError:
            return None

    def __init__(self, from_day=None):
        """Instantiate a Days object."""
        self.market_days = self._get_market_days(from_day)

    def __repr__(self):
        return str(self.market_days)

    def __str__(self):
        return str(self.market_days)

    def _get_market_days(self, from_day):
        """Create a Series from the data on the marketdays table."""
        db.ensure_connected()
        with db.conn.cursor() as cur:
            cur.execute('''
                SELECT open, day
                FROM marketdays
                WHERE (%(from_day)s IS NULL OR day >= %(from_day)s) AND day < %(today)s
                ORDER BY day;''',
                {'from_day': from_day, 'today': date.today()})

            if cur.rowcount > 0:
                _records = list(zip(*cur.fetchall()))  # two sublists: the first of open values, the second of days
            else:
                _records = [None, None]

            return pd.Series(_records[0], index=_records[1], dtype=bool)
