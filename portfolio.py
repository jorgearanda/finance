import pandas as pd

from components.deposits import Deposits
from components.positions import Positions
from components.tickers import Tickers
import config
from db import db


class Portfolio():
    """Hold performance information for an investment account or portfolio.

    Public methods:
    latest() -- Return the latest metrics for this portfolio
    allocations() -- Return the latest asset allocations for this portfolio

    Instance variables:
    account -- str, the account for this portfolio. If None, the portfolio represents all accounts in the database
    from_day -- date, the start date for accounting. If None, data is not filtered by date
    deposits -- Deposits object, with all deposits relevant to the portfolio
    tickers -- Tickers object, with all tickers relevant to the portfolio
    positions -- Positions object, with performance data for all positions in the portfolio
    by_day -- DataFrame indexed by day, with the following Series:
        - `days_from_start` int, days from the date the account was opened
        - `years_from_start` float, years from the date the account was opened
        - `market_day` bool, whether the market was open on this date
        - `day_deposits` float, deposits made on this date
        - `capital` float, sum of deposits made to this date
        - `avg_capital` float, average capital held over the lifetime of the portfolio
        - `positions_cost` float, sum of the cost of all positions in the portfolio on this date
        - `positions_value`, sum of the current market valuations of all positions in the portfolio on this date
        - `appreciation` float, the current market valuation minus the position cost
        - `dividends` float, sum of dividends received to this date
        - `cash` float, cash held by the end of this date
        - `total_value` float, sum of the market value of all the positions *and* cash by the end of this date
        - `day_profit` float, difference between the market value on this date and the day before
        - `day_returns` float, percentage of the daily profit over yesterday's market value
        - `profit` float, difference between the market value on this date and the capital provided
        - `appreciation_returns` float, percentage returns obtained by unrealized market value appreciations
        - `distribution_returns` float, percentage returns obtained by dividends
        - `returns` float, percentage returns, both by dividends and appreciation
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
        return self.by_day.ix[-1]

    def allocations(self):
        return self.positions.weights.ix[-1]

    def __init__(self, account=None, from_day=None):
        """Instantiate a Portfolio object."""
        self.account = account
        self.from_day = from_day
        self.deposits = Deposits(account, from_day)
        self.tickers = Tickers(from_day)
        self.positions = Positions(account, from_day, self.tickers)
        if len(self.tickers.ticker_names) == 0:
            self.by_day = pd.DataFrame()
            return
        df = pd.DataFrame(index=self.tickers.prices.index)
        df['days_from_start'] = range(1, len(df) + 1)
        df['years_from_start'] = df['days_from_start'] / 365.0
        df['market_day'] = self.tickers.market_day
        df['day_deposits'] = self.deposits.deposits
        df['day_deposits'].fillna(0.00, inplace=True)
        df['capital'] = df['day_deposits'].cumsum()
        df['avg_capital'] = df['capital'].expanding().mean()
        df['positions_cost'] = self.positions.costs.sum(axis=1)
        df['positions_value'] = self.positions.market_values.sum(axis=1)
        df['appreciation'] = df['positions_value'] - df['positions_cost']
        df['dividends'] = self.positions.distributions.sum(axis=1)
        df['cash'] = df['capital'] + df['dividends'] - df['positions_cost']
        df['total_value'] = df['cash'] + df['positions_value']
        df['day_profit'] = (df['total_value'] - df['total_value'].shift(1) - df['day_deposits']).fillna(0.00)
        df['day_returns'] = (df['day_profit'] / df['total_value'].shift(1)).fillna(0.00)
        df['profit'] = df['total_value'] - df['capital']
        df['appreciation_returns'] = df['appreciation'] / df['capital']
        df['distribution_returns'] = df['dividends'] / df['capital']
        df['returns'] = df['profit'] / df['capital']
        df['twrr'] = ((df['day_returns'] + 1).cumprod() - 1).fillna(0.00)
        df['twrr_annualized'] = (1.0 + df['twrr']) ** (1 / df['years_from_start']) - 1
        df['mwrr'] = df['profit'] / df['avg_capital']
        df['mwrr_annualized'] = (1.0 + df['mwrr']) ** (1 / df['years_from_start']) - 1
        df['volatility'] = df[(df['market_day'])]['day_returns'].expanding().std()
        df['10k_equivalent'] = 10000 * (df['twrr'] + 1)
        df['last_peak_twrr'] = df['twrr'].expanding().max()
        df['last_peak'] = \
            df['last_peak_twrr'].groupby(df['last_peak_twrr']).transform('idxmax').astype('datetime64[ns]')
        df['current_drawdown'] = (df['twrr'] - df['last_peak_twrr']) / (1 + df['last_peak_twrr'])
        df['greatest_drawdown'] = df['current_drawdown'].expanding().min()
        df['sharpe'] = (df['twrr'] - config.sharpe * df['years_from_start']) / df['volatility']
        self.by_day = df
        self.positions.calc_weights(df['total_value'])
