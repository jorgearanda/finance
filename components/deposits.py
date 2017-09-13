from datetime import date
import pandas as pd

from db import db


class Deposits():
    """DataFrame-based structure to keep track of investment deposits.

    Public methods:
    amount(date) -- Return the amount deposited on this day

    Instance variables:
    account -- str, the account to which the deposits were made
    deposits -- DataFrame, day-indexed, with the total sum deposited on each day
    """

    def amount(self, day):
        """Return the amount deposited on the requested day. If no deposits on a date, return None rather than zero."""
        try:
            return self.deposits['amount'][day]
        except KeyError:
            return None

    def __init__(self, account=None, from_day=None):
        """Instantiate a Deposits object."""
        self.account = account
        self.deposits = self._get_deposits(account, from_day)

    def __repr__(self):
        return str(self.deposits)

    def __str__(self):
        return str(self.deposits)

    def _get_deposits(self, account, from_day):
        db.ensure_connected()
        with db.conn.cursor() as cur:
            cur.execute('''
                SELECT SUM(total)::double precision AS amount, day
                FROM transactions
                WHERE (%(account)s IS NULL OR account = %(account)s)
                    AND (%(from_day)s IS NULL OR day >= %(from_day)s)
                    AND day < %(today)s
                    AND txtype = 'deposit'
                GROUP BY day
                ORDER BY day ASC;''',
                {'account': account, 'from_day': from_day, 'today': date.today()})

            _deposits = pd.DataFrame(cur.fetchall())
            if not _deposits.empty:
                _deposits = _deposits.set_index('day')
                _deposits = _deposits.astype('float')

        return _deposits


