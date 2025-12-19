from datetime import date
from sqlalchemy import bindparam

from db.data import Data
from util.determine_accounts import determine_accounts


class Deposits:
    """DataFrame-based structure to keep track of investment deposits.

    Public methods:
    amount(date) -- Return the total sum deposited on this day to all accounts requested

    Instance variables:
    deposits -- DataFrame, day-indexed, with the total sum deposited on each day into
                any of the accounts requested
    """

    def amount(self, day):
        """Return the amount deposited on the requested day."""
        return self.deposits["amount"].get(day, 0)

    def __init__(self, accounts=None, from_day=None, data=None):
        """Instantiate a Deposits object."""
        self._data = Data() if data is None else data
        self._accounts = determine_accounts(accounts)
        self._from_day = from_day
        self.deposits = self._get_deposits()

    def __str__(self):
        return str(self.deposits)

    def _get_deposits(self):
        return self._data.df_from_sql(
            """SELECT SUM(total) AS amount, day
            FROM transactions
            WHERE account IN :accounts
                AND (:from_day IS NULL OR day >= :from_day)
                AND day <= :today
                AND txtype = 'deposit'
            GROUP BY day
            ORDER BY day ASC;""",
            params={
                "accounts": self._accounts,
                "from_day": self._from_day,
                "today": date.today(),
            },
            index_col="day",
            parse_dates=["day"],
            bindparams=[bindparam("accounts", expanding=True)],
        )
