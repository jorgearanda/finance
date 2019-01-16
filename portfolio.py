from datetime import timedelta
import numpy as np
import pandas as pd

from components.deposits import Deposits
from components.positions import Positions
from components.tickers import Tickers
import config
from db import db
from util.determine_accounts import determine_accounts
from util.update_prices import update_prices


class Portfolio:
    """Hold performance information for an investment account or portfolio.

    Public methods:
    val(prop, day) -- Return the value of property `prop` on date `day`
    latest() -- Return the latest daily metrics
    current_month() -- Return the current month's metrics
    previous_month() -- Return last month's metrics
    allocations() -- Return the latest asset allocations

    Instance variables:
    accounts -- list, the accounts for this portfolio. If None, the portfolio represents all accounts
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
    by_month -- DataFrame indexed by end of month, similar to by_day but
                with the following additional Series:
        - `month_deposits` float, mirroring `day_deposits` in `by_day`
        - `month_profits` float, mirroring `day_profits` in `by_day`
        - `month_returns` float, mirroring `day_returns` in `by_day`
    """

    def val(self, prop, day):
        """Return the value of property `prop` on day `day`."""
        return self.by_day.ix[day][prop]

    def latest(self):
        if len(self.by_day.index) > 0:
            return self.by_day.ix[-1]
        else:
            return None

    def current_month(self):
        if len(self.by_month.index) > 0:
            return self.by_month.ix[-1]
        else:
            return None

    def previous_month(self):
        if len(self.by_month.index) > 1:
            return self.by_month.ix[-2]
        else:
            return None

    def allocations(self):
        return self.positions.weights.ix[-1]

    def __init__(self, accounts=None, from_day=None, update=True, verbose=True):
        """Instantiate a Portfolio object."""
        self.accounts = determine_accounts(accounts)
        if from_day is not None:
            self.from_day = from_day
        else:
            self.from_day = self._get_start_date(self.accounts)
        if update:
            update_prices(verbose)
        self.deposits = Deposits(self.accounts, self.from_day)
        self.tickers = Tickers(self.accounts, self.from_day)
        self.positions = Positions(self.accounts, self.from_day, self.tickers)
        self.by_day = self._calc_daily()
        self.by_month = self._calc_monthly()
        self.by_year = self._calc_yearly()
        if len(self.by_day.index) > 0:
            self.positions.calc_weights(self.by_day["total_value"])

    def _get_start_date(self, accounts):
        db.ensure_connected()
        with db.conn.cursor() as cur:
            cur.execute(
                """SELECT MIN(datecreated) AS datecreated
                FROM accounts
                WHERE name = ANY(%(accounts)s);""",
                {"accounts": accounts},
            )
            date_created = cur.fetchone().datecreated

        if date_created is None:
            return None
        return date_created - timedelta(days=1)

    def _calc_daily(self):
        if self._no_tickers():
            return pd.DataFrame()
        df = pd.DataFrame(index=self.tickers.prices.index)
        df["days_from_start"] = range(1, len(df) + 1)
        df["years_from_start"] = df["days_from_start"] / 365.0
        df["market_day"] = self.tickers.market_day
        df["day_deposits"] = self.deposits.deposits
        df["day_deposits"].fillna(0.00, inplace=True)
        df["capital"] = df["day_deposits"].cumsum()
        df["avg_capital"] = df["capital"].expanding().mean()
        df["positions_cost"] = self.positions.costs.sum(axis=1)
        df["positions_value"] = self.positions.market_values.sum(axis=1)
        df["appreciation"] = df["positions_value"] - df["positions_cost"]
        df["dividends"] = self.positions.distributions.sum(axis=1)
        df["cash"] = df["capital"] + df["dividends"] - df["positions_cost"]
        df["total_value"] = df["cash"] + df["positions_value"]
        df["day_profit"] = (
            df["total_value"] - df["total_value"].shift(1) - df["day_deposits"]
        ).fillna(0.00)
        df["day_returns"] = (df["day_profit"] / df["total_value"].shift(1)).fillna(0.00)
        df["profit"] = df["total_value"] - df["capital"]
        df["appreciation_returns"] = df["appreciation"] / df["capital"]
        df["distribution_returns"] = df["dividends"] / df["capital"]
        df["returns"] = df["profit"] / df["capital"]
        df["twrr"] = ((df["day_returns"] + 1).cumprod() - 1).fillna(0.00)
        df["twrr_annualized"] = np.where(
            df["years_from_start"] > 1.0,
            (1.0 + df["twrr"]) ** (1 / df["years_from_start"]) - 1,
            df["twrr"],
        )
        df["mwrr"] = df["profit"] / df["avg_capital"]
        df["mwrr_annualized"] = np.where(
            df["years_from_start"] > 1.0,
            (1.0 + df["mwrr"]) ** (1 / df["years_from_start"]) - 1,
            df["mwrr"],
        )
        df["volatility"] = df[(df["market_day"])]["day_returns"].expanding().std()
        df["volatility"].fillna(method="ffill", inplace=True)
        df["10k_equivalent"] = 10000 * (df["twrr"] + 1)
        df["last_peak_twrr"] = df["twrr"].expanding().max()
        df["last_peak"] = (
            df["last_peak_twrr"]
            .groupby(df["last_peak_twrr"])
            .transform("idxmax")
            .astype("datetime64[ns]")
        )
        df["current_drawdown"] = (df["twrr"] - df["last_peak_twrr"]) / (
            1 + df["last_peak_twrr"]
        )
        df["greatest_drawdown"] = df["current_drawdown"].expanding().min()
        df["sharpe"] = (df["twrr"] - config.sharpe * df["years_from_start"]) / df[
            "volatility"
        ]

        return df

    def _calc_monthly(self):
        if self._no_tickers():
            return pd.DataFrame()
        df = self.by_day.asfreq("M")
        if df.empty or df.index.values[-1] != self.by_day.index.values[-1]:
            df = df.append(self.by_day.ix[-1])
        df = df.drop(
            ["market_day", "day_deposits", "day_profit", "day_returns"], axis=1
        )
        df["month_deposits"] = df["capital"] - df["capital"].shift(1).fillna(0.00)
        df["month_profit"] = (
            df["total_value"]
            - df["total_value"].shift(1).fillna(0.00)
            - df["month_deposits"]
        )
        df["month_returns"] = df["month_profit"] / df["total_value"].shift(1).fillna(
            df["month_deposits"]
        )

        return df

    def _calc_yearly(self):
        if self._no_tickers():
            return pd.DataFrame()
        df = self.by_day.asfreq("A")
        if df.empty or df.index.values[-1] != self.by_day.index.values[-1]:
            df = df.append(self.by_day.ix[-1])
        df = df.drop(
            ["market_day", "day_deposits", "day_profit", "day_returns"], axis=1
        )
        df["year_deposits"] = df["capital"] - df["capital"].shift(1).fillna(0)
        df["year_profit"] = df["profit"] - df["profit"].shift(1).fillna(0)
        df["year_returns"] = (1 + df["returns"]) / (
            1 + df["returns"].shift(1).fillna(0)
        ) - 1
        df["year_twrr"] = (1 + df["twrr"]) / (1 + df["twrr"].shift(1).fillna(0)) - 1
        df["year_mwrr"] = (1 + df["mwrr"]) / (1 + df["mwrr"].shift(1).fillna(0)) - 1

        return df

    def _no_tickers(self):
        return len(self.tickers.ticker_names) == 0
