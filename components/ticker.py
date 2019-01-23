from datetime import date
import pandas as pd

from db.db import df_from_sql


class Ticker:
    """DataFrame-based structure of ticker pricing data.

    Public methods:
    price(date) -- Return the closing price of the ticker on this date
    change(date) -- Return the percentage price change from the date before
    change_from_start(date) -- Return the percentage price change from the initial value
    distribution(date) -- Return the per-unit distribution on the requested date
    distributions_from_start(date) -- Return the accumulated per-unit cash distributions until the requested date
    yield_from_start(date) -- Return the yield (distributions over price) from the start date until the requested one
    returns(date) -- Return the percentage of returns to this date, considering appreciation and distributions

    Instance variables:
    ticker_name -- Name of the ticker
    values -- DataFrame indexed by day, with:
        - a `price` float column with the ticker's closing price
        - a `change` float column with the percentage price change from the day before
        - a `change_from_start` float column with the percentage price change from the initial value
        - a `distributions` float column with per-unit distributions on this date
        - a `distributions_from_start` float column with accumulated distributions per unit
        - a `yield_from_start` float column with the total percentage yield on this date
        - a `returns` float column with the returns (as percentage) considering appreciations and distributions
    volatility -- Standard deviation of the price changes (that is, of the `change` series)
    """

    def price(self, day):
        """Report the closing price for this ticker on the requested day."""
        try:
            return self.values["price"][day]
        except KeyError:
            return None

    def change(self, day):
        """Report the percentage price change from the date before."""
        try:
            return self.values["change"][day]
        except KeyError:
            return None

    def change_from_start(self, day):
        """Report the percentage price change from the initial value."""
        try:
            return self.values["change_from_start"][day]
        except KeyError:
            return None

    def distribution(self, day):
        """Report the per-unit cash distribution on the requested date."""
        try:
            return self.values["distribution"][day]
        except KeyError:
            return None

    def distributions_from_start(self, day):
        """Report the accumulated per-unit cash distribution up to the date."""
        try:
            return self.values["distributions_from_start"][day]
        except KeyError:
            return None

    def yield_from_start(self, day):
        """Report the per-unit yield on the requested date."""
        try:
            return self.values["yield_from_start"][day]
        except KeyError:
            return None

    def returns(self, day):
        """Report the per-unit total returns as a percentage."""
        try:
            return self.values["returns"][day]
        except KeyError:
            return None

    def __init__(self, ticker_name, from_day=None):
        """Instantiate a Ticker object."""
        self.ticker_name = ticker_name
        self.from_day = from_day
        self.values = self._get_daily_values()
        self.volatility = self._get_volatility()

    def __repr__(self):
        return str(self.values.head())

    def __str__(self):
        return str(self.values.head())

    def _get_daily_values(self):
        """Create a DataFrame with daily ticker data."""
        df = df_from_sql(
            """WITH tickerdistributions AS
                (SELECT day, amount::double precision
                FROM distributions
                WHERE ticker = %(ticker_name)s)
            SELECT m.day, m.open, p.close::double precision AS price,
                COALESCE(d.amount, 0) AS distribution
            FROM marketdays m LEFT JOIN assetprices p USING (day)
            LEFT JOIN tickerdistributions d USING (day)
            WHERE (p.ticker IS NULL OR p.ticker = %(ticker_name)s)
            AND (%(from_day)s IS NULL OR m.day >= %(from_day)s)
            AND m.day <= %(today)s
            ORDER BY m.day ASC;""",
            params={
                "ticker_name": self.ticker_name,
                "from_day": self.from_day,
                "today": date.today(),
            },
            index_col="day",
            parse_dates=["day"],
        )

        df["price"].fillna(method="ffill", inplace=True)
        df["change"] = (df["price"] / df["price"].shift(1)) - 1.0
        first_price_idx = df["price"].first_valid_index()
        df["distributions_from_start"] = df["distribution"].cumsum()
        if first_price_idx:
            df["change_from_start"] = (df["price"] / df["price"][first_price_idx]) - 1.0
            df["yield_from_start"] = (
                df["distributions_from_start"] / df["price"][first_price_idx]
            )
        else:  # there are no valid prices
            df["change_from_start"] = 0.0
            df["yield_from_start"] = 0.0
        df["returns"] = df["change_from_start"] + df["yield_from_start"]

        return df

    def _get_volatility(self):
        """Calculate ticker price volatility."""
        if self.values.empty:
            return None
        else:
            return self.values[(self.values.open)]["change"].std(axis=0)
