from datetime import date
import pandas as pd

from db.db import df_from_sql
from util.determine_accounts import determine_accounts


class Deposits:
    """DataFrame-based structure to keep track of investment deposits.

    Public methods:
    amount(date) -- Return the amount deposited on this day

    Instance variables:
    account -- str, the account to which the deposits were made
    deposits -- DataFrame, day-indexed, with the total sum deposited on each day
    """

    def amount(self, day):
        """Return the amount deposited on the requested day.

        If no deposits on a date, return None rather than zero.
        """
        try:
            return self.deposits["amount"][day]
        except KeyError:
            return None

    def __init__(self, accounts=None, from_day=None):
        """Instantiate a Deposits object."""
        self.accounts = determine_accounts(accounts)
        self.deposits = self._get_deposits(self.accounts, from_day)

    def __repr__(self):
        return str(self.deposits)

    def __str__(self):
        return str(self.deposits)

    def _get_deposits(self, accounts, from_day):
        return df_from_sql(
            """SELECT SUM(total)::double precision AS amount, day
            FROM transactions
            WHERE account = ANY(%(accounts)s)
                AND (%(from_day)s IS NULL OR day >= %(from_day)s)
                AND day <= %(today)s
                AND txtype = 'deposit'
            GROUP BY day
            ORDER BY day ASC;""",
            params={"accounts": accounts, "from_day": from_day, "today": date.today()},
            index_col="day",
            parse_dates=["day"],
        )
