from datetime import timedelta
import numpy as np
import pandas as pd

pd.set_option("future.no_silent_downcasting", True)

from sqlalchemy import text

from components.deposits import Deposits
from components.positions import Positions
from components.tickers import Tickers
import config
from db import db
from db.data import Data
from util.determine_accounts import determine_accounts
from util.relative_rate import relative_rate
from util.price_updater import PriceUpdater


class Portfolio:
    """Hold performance information for an investment account or portfolio.

    Public methods:
    val(prop, day) -- Return the value of property `prop` on date `day`
    latest() -- Return the latest daily metrics
    current_month() -- Return the current month's metrics
    previous_month() -- Return last month's metrics
    current_year() -- Return the current year's metrics
    previous_year() -- Return last year's metrics
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
        return self.by_day[prop][day]

    def latest(self):
        return self.by_day.iloc[-1] if len(self.by_day.index) > 0 else None

    def current_month(self):
        return self.by_month.iloc[-1] if len(self.by_month.index) > 0 else None

    def previous_month(self):
        return self.by_month.iloc[-2] if len(self.by_month.index) > 1 else None

    def current_year(self):
        return self.by_year.iloc[-1] if len(self.by_year.index) > 0 else None

    def previous_year(self):
        return self.by_year.iloc[-2] if len(self.by_year.index) > 1 else None

    def allocations(self):
        return self.positions.weights.iloc[-1]

    def __init__(self, accounts=None, from_day=None, update=True, verbose=True):
        """Instantiate a Portfolio object."""
        self._data = Data()
        self.accounts = determine_accounts(accounts)
        if from_day is not None:
            self.from_day = from_day
        else:
            self.from_day = self._get_start_date(self.accounts)
        if update:
            PriceUpdater(verbose).update()
        self.deposits = Deposits(self.accounts, self.from_day, self._data)
        self.tickers = Tickers(self.accounts, self.from_day, data=self._data)
        self.positions = Positions(
            self.accounts, self.from_day, self.tickers, data=self._data
        )
        self.by_day = self._calc_daily()
        self.by_month = self._summarize_by("month")
        self.by_year = self._summarize_by("year")
        if len(self.by_day.index) > 0:
            self.positions.calc_weights(self.by_day["total_value"])

    def _get_start_date(self, accounts):
        db.ensure_connected()
        cur = db.conn.execute(
            text(
                """SELECT MIN(datecreated) AS datecreated
            FROM accounts
            WHERE name = ANY(:accounts);"""
            ),
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
        df["volatility"] = df["volatility"].ffill()
        df["10k_equivalent"] = 10000 * (df["twrr"] + 1)
        df["last_peak_twrr"] = df["twrr"].expanding().max()
        # Map each peak value to the first date it occurred
        peak_first_occurrence = df.groupby("last_peak_twrr", sort=False)[
            "last_peak_twrr"
        ].apply(lambda x: x.index[0])
        df["last_peak"] = df["last_peak_twrr"].map(peak_first_occurrence).astype(
            "datetime64[ns]"
        )
        df["current_drawdown"] = (df["twrr"] - df["last_peak_twrr"]) / (
            1 + df["last_peak_twrr"]
        )
        df["greatest_drawdown"] = df["current_drawdown"].expanding().min()
        df["sharpe"] = (df["twrr"] - config.sharpe * df["years_from_start"]) / df[
            "volatility"
        ]

        return df

    def _summarize_by(self, freq):
        """Valid frequencies are `month` and `year`."""
        if self._no_tickers():
            return pd.DataFrame()
        df = self.by_day.asfreq("ME" if freq == "month" else "YE")
        if df.empty or df.index.values[-1] != self.by_day.index.values[-1]:
            df.loc[self.by_day.index.values[-1]] = self.by_day.iloc[-1]
        df = df.drop(
            ["market_day", "day_deposits", "day_profit", "day_returns"], axis=1
        )
        df[f"{freq}_deposits"] = df["capital"] - df["capital"].shift(1).fillna(0.00).infer_objects(copy=False)
        df[f"{freq}_profit"] = df["profit"] - df["profit"].shift(1).fillna(0).infer_objects(copy=False)
        df[f"{freq}_returns"] = relative_rate(df["returns"])
        df[f"{freq}_twrr"] = relative_rate(df["twrr"])
        df[f"{freq}_mwrr"] = relative_rate(df["mwrr"])
        return df

    def _no_tickers(self):
        return len(self.tickers.ticker_names) == 0
