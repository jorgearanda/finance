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
            print(f"New quotes: {self.new_quotes}")
            print(f"Updated quotes: {self.updated_quotes}")
            print("===Finished updating prices===\n")

    def _ticker_symbols(self):
        """Get all the tickers to poll from the database."""
        db.ensure_connected()
        with db.conn.cursor() as cur:
            cur.execute(
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
                print(f"  x {quote.symbol:6} {quote.day}: -null- (skipping)")
            return

        with db.conn.cursor() as cur:
            cur.execute(
                """SELECT close FROM assetprices
                WHERE ticker = %(ticker)s AND day = %(day)s;""",
                {"ticker": quote.symbol, "day": quote.day},
            )

            if cur.rowcount == 0:
                cur.execute(
                    """
                    INSERT INTO assetprices (ticker, day, ask, bid, close)
                    VALUES (%(ticker)s, %(day)s,
                        %(close)s, %(close)s, %(close)s)
                    ON CONFLICT DO NOTHING;""",
                    {
                        "ticker": quote.symbol,
                        "day": quote.day,
                        "close": quote.price,
                    },
                )
                self.new_quotes += 1
                if self.verbose:
                    print(
                        f"  + {quote.symbol:6} {quote.day}:  -.-- -> {quote.price:.2f}"
                    )
            else:
                old_price = cur.fetchone().close
                if not math.isclose(float(old_price), quote.price, abs_tol=0.0051):
                    cur.execute(
                        """UPDATE assetprices
                        SET ask = %(close)s, bid = %(close)s, close = %(close)s
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
                            f"  + {quote.symbol:6} {quote.day}: {old_price} -> {quote.price:.2f}"
                        )
