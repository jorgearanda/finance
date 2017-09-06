from datetime import date
import pandas as pd

from components.deposits import Deposits
from components.positions import Positions
from components.tickers import Tickers
from db import db


class Portfolio():
    """Hold performance information for an investment account or portfolio.

    Public methods:
    --none yet--

    Instance variables:
    account -- str, the account for this portfolio. If None, the portfolio represents all accounts in the database
    from_day -- date, the start date for accounting. If None, data is not filtered by date
    deposits -- Deposits object, with all deposits relevant to the portfolio
    tickers -- Tickers object, with all tickers relevant to the portfolio
    positions -- Positions object, with performance data for all positions in the portfolio
    """

    def __init__(self, account=None, from_day=None):
        """Instantiate a Portfolio object."""
        self.account = account
        self.from_day = from_day
        self.deposits = Deposits(account, from_day)
        self.tickers = Tickers(from_day)
        self.positions = Positions(account, from_day, self.tickers)
