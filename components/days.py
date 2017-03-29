from datetime import date
import pandas as pd

from db import db


class Days():
    """DataFrame-based structure to keep track of open and closed market days.

    Public methods:
    open(date) -- Report whether the market was open on this date

    Instance variables:
    days -- DataFrame indexed by day, with an `open` boolean column
    """

    def open(self, day):
        """Report whether the market was open on the requested day.

        Keyword arguments:
        day -- the requested day, in datetime.date format

        Returns:
        bool -- True if the market was open, False if closed, None if out of scope
        """
        try:
            return self.days.loc[day]['open']
        except KeyError:
            return None

    def __init__(self, from_day=None):
        """Instantiate a Days object.

        The Days object will contain all days from the `from_day` until yesterday.
        If the `from_day` argument is None, the object will contain all days in the database until yesterday.

        Keyword arguments:
        from_day -- the first day in the sequence, in datetime.date format (default None)
        """
        self.days = self._get_days(from_day)

    def __repr__(self):
        return str(self.days)

    def __str__(self):
        return str(self.days)

    def _get_days(self, from_day):
        """Create a dataframe from the data on the marketdays table."""
        db.ensure_connected()
        with db.conn.cursor() as cur:
            cur.execute('''
                SELECT day, open
                FROM marketdays
                WHERE (%(from_day)s IS NULL OR day >= %(from_day)s) AND day < %(today)s
                ORDER BY day;''',
                {'from_day': from_day, 'today': date.today()})

            _days = pd.DataFrame(cur.fetchall())
            if not _days.empty:
                _days = _days.set_index('day')

            return _days
