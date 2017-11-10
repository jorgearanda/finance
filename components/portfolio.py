from datetime import date
import pandas as pd

from components.deposits import Deposits
from components.positions import Positions
from components.tickers import Tickers
from db import db


class Portfolio():
    """Hold performance information for an investment account or portfolio.

    Public methods:
    latest() -- Return the latest metrics for this portfolio

    Instance variables:
    account -- str, the account for this portfolio. If None, the portfolio represents all accounts in the database
    from_day -- date, the start date for accounting. If None, data is not filtered by date
    deposits -- Deposits object, with all deposits relevant to the portfolio
    tickers -- Tickers object, with all tickers relevant to the portfolio
    positions -- Positions object, with performance data for all positions in the portfolio
    daily -- DataFrame indexed by day, with the following Series:
        - `days_from_start` int, days from the date the account was opened
        - `market_open` bool, whether the market was open on this date
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
        - `last_peak_twrr` float, time-weighted returns at the highest point in the lifetime of the portfolio to date
        - `current_drawdown` float, percentage drop in returns from the last peak
        - `greatest_drawdown` float, greatest percentage drop in returns in the lifetime of the portfolio
        - `sharpe` float, Sharpe ratio of the portfolio as a whole
    """

    def latest(self):
        return self.daily.ix[-1]

    def __init__(self, account=None, from_day=None):
        """Instantiate a Portfolio object."""
        self.account = account
        self.from_day = from_day
        self.deposits = Deposits(account, from_day)
        self.tickers = Tickers(from_day)
        self.positions = Positions(account, from_day, self.tickers)
        if len(self.tickers.ticker_names) == 0:
            self.daily = pd.DataFrame()
            return
        self.daily = pd.DataFrame(index=self.tickers.prices.index)
        self.daily['days_from_start'] = range(1, len(self.daily) + 1)
        self.daily['market_open'] = self.tickers.market_open
        self.daily['daily_deposits'] = self.deposits.deposits
        self.daily['daily_deposits'].fillna(0.00, inplace=True)
        self.daily['capital'] = self.daily['daily_deposits'].cumsum()
        self.daily['avg_capital'] = self.daily['capital'].expanding().mean()
        self.daily['dividends'] = self.positions.distributions.sum(axis=1)
        self.daily['cash'] = self.daily['capital'] + self.daily['dividends'] - self.positions.costs.sum(axis=1)
        self.daily['market_value'] = self.daily['cash'] + self.positions.market_values.sum(axis=1)
        self.daily['daily_profit'] = self.daily['market_value'] - self.daily['market_value'].shift(1) - self.daily['daily_deposits']
        self.daily['daily_returns'] = self.daily['daily_profit'] / self.daily['market_value'].shift(1)
        self.daily['profit'] = self.daily['market_value'] - self.daily['capital']
        self.daily['distribution_returns'] = self.daily['dividends'] / self.daily['capital']
        self.daily['appreciation_returns'] = self.positions.market_values.sum(axis=1) / self.positions.costs.sum(axis=1) - 1
        self.daily['total_returns'] = self.daily['profit'] / self.daily['capital']
        self.daily['twrr'] = ((self.daily['daily_returns'] + 1).cumprod() - 1).fillna(0.00)
        self.daily['twrr_annualized'] = (1.0 + self.daily['twrr']) ** (365.0 / (self.daily['days_from_start'])) - 1
        self.daily['mwrr'] = self.daily['profit'] / self.daily['avg_capital']
        self.daily['mwrr_annualized'] = (1.0 + self.daily['mwrr']) ** (365.0 / (self.daily['days_from_start'])) - 1
        self.daily['volatility'] = self.daily[(self.daily['market_open'])]['daily_returns'].expanding().std()
        self.daily['10k_equivalent'] = 10000 * (self.daily['twrr'] + 1)
        self.daily['last_peak_twrr'] = self.daily['twrr'].expanding().max()
        self.daily['last_peak'] = self.daily['last_peak_twrr'].groupby(self.daily['last_peak_twrr']).transform('idxmax').astype('datetime64[ns]')
        self.daily['current_drawdown'] = (self.daily['twrr'] - self.daily['last_peak_twrr']) / (1 + self.daily['last_peak_twrr'])
        self.daily['greatest_drawdown'] = self.daily['current_drawdown'].expanding().min()
