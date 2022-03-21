import math

from db import db
from util.yahoo_scraper import YahooScraper


class PriceUpdater:
    def __init__(self, verbose=False):
        self.verbose = verbose
        self.scraper = YahooScraper()

    def update(self):
        self.new_quotes = self.updated_quotes = 0
        if self.verbose:
            print("===Updating prices===")
        self._record_quotes(self.scraper.get_quotes(symbols=self._ticker_symbols()))
        if self.verbose:
            print(f"{self.new_quotes} new quote(s), {self.updated_quotes} update(s)")
            print("===Finished update===\n")

    def _ticker_symbols(self):
        """Get all the tickers to poll from the database."""
        db.ensure_connected()
        cur = db.conn.execute(
            """
            SELECT ticker AS name
            FROM assets
            WHERE class != 'Domestic Cash';"""
        )

        return [ticker.name for ticker in cur.fetchall()]

    def _record_quotes(self, quotes):
        for quote in quotes:
            self._record_quote(quote)

    def _record_quote(self, quote):
        if quote.price is None:
            if self.verbose:
                print(f"x {quote.symbol:6} {quote.day}: -null- (skipping)")
            return

        cur = db.conn.execute(
            """SELECT close FROM assetprices
            WHERE ticker = %(ticker)s AND day = %(day)s;""",
            {"ticker": quote.symbol, "day": quote.day},
        )

        if cur.rowcount == 0:
            cur = db.conn.execute(
                """
                INSERT INTO assetprices (ticker, day, close)
                VALUES (%(ticker)s, %(day)s, %(close)s)
                ON CONFLICT DO NOTHING;""",
                {
                    "ticker": quote.symbol,
                    "day": quote.day,
                    "close": quote.price,
                },
            )
            self.new_quotes += 1
            if self.verbose:
                print(f"+ {quote.symbol:6} {quote.day}:  -.-- -> {quote.price:.2f}")
        else:
            prev_price = cur.fetchone().close
            if not self._is_same_price(prev_price, quote.price):
                cur = db.conn.execute(
                    """UPDATE assetprices
                    SET close = %(close)s
                    WHERE ticker = %(ticker)s AND day = %(day)s;""",
                    {
                        "ticker": quote.symbol,
                        "day": quote.day,
                        "close": quote.price,
                    },
                )
                self.updated_quotes += 1
                if self.verbose:
                    print(
                        f"* {quote.symbol:6} {quote.day}: {prev_price} -> {quote.price:.2f}"
                    )

    def _is_same_price(self, prev, new):
        return math.isclose(float(prev), float(new), abs_tol=0.0051)
