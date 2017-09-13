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
    daily -- DataFrame indexed by day, with the following Series:
        - `days_from_start` int, days from the date the account was opened
        - `daily_deposits` float, deposits made on this date
        - `capital` float, sum of deposits made to this date
        - `avg_capital` float, average capital held over the lifetime of the portfolio
        - `dividends` float, sum of dividends received to this date
        - `cash` float, cash held by the end of this date
        - `market_value` float, sum of the market value of all the positions *and* cash by the end of this date
        - `daily_profit` float, difference between the market value on this date and the day before
        - `daily_returns` float, percentage of the daily profit over yesterday's market value
        - `profit` float, difference between the market value on this date and the capital provided
        - `distribution_returns` float, percentage returns obtained by dividends
        - `appreciation_returns` float, percentage returns obtained by unrealized market value appreciations
        - `total_returns` float, percentage returns, both by dividends and appreciation
        - `twrr` float, time-weighted real returns
        - `twrr_annualized` float, annualized time-weighted real returns
        - `mwrr` float, money-weighted real returns
        - `mwrr_annualized` float, annualized money-weighted real returns
        - `volatility` float, volatility of the portfolio as a whole
        - `10k_equivalent` float, value of a hypothetical 10k dollars invested with this strategy from the start
        - `last_peak_twrr` float, time-weighted returns at the highest point in the lifetime of the portfolio
        - `current_drawdown` float, percentage drop in returns from the last peak
        - `current_drawdown_start` date, date when the current drawdown began
        - `greatest_drawdown` float, greatest percentage drop in returns in the lifetime of the portfolio
        - `greatest_drawdown_start` date, day when the greatest drawdown began
        - `greatest_drawdown_end` date, day when the greatest drawdown ended
        - `sharpe` float, Sharpe ratio of the portfolio as a whole
    """

    def __init__(self, account=None, from_day=None):
        """Instantiate a Portfolio object."""
        self.account = account
        self.from_day = from_day
        self.deposits = Deposits(account, from_day)
        self.tickers = Tickers(from_day)
        self.positions = Positions(account, from_day, self.tickers)
        self.daily = pd.DataFrame(index=self.tickers.prices.index)
        self.daily['days_from_start'] = range(1, len(self.daily) + 1)
        self.daily['daily_deposits'] = self.deposits.deposits
        self.daily['daily_deposits'].fillna(0.00, inplace=True)
        self.daily['capital'] = self.daily['daily_deposits'].cumsum()
        self.daily['avg_capital'] = self.daily['capital'].expanding().mean()
        self.daily['dividends'] = self.positions.distributions.sum(axis=1)
        self.daily['cash'] = self.daily['capital'] + self.daily['dividends'] - self.positions.costs.sum(axis=1)
        self.daily['market_value'] = self.daily['cash'] + self.positions.market_values.sum(axis=1)
