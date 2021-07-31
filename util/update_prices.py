import math

from db import db
from util.yahoo_scraper import YahooScraper

verbose = False


def update_prices(verbosity=False):
    global verbose
    verbose = verbosity
    if verbose:
        print("===Updating prices===")
    record_quotes(YahooScraper(verbose=verbose).get_quotes(symbols=ticker_symbols()))
    if verbose:
        print("===Finished updating prices===\n")


def ticker_symbols():
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


def record_quotes(quotes):
    symbol = None
    for quote in quotes:
        if symbol != quote.symbol and verbose:
            print(f"* Updating {quote.symbol}")
        symbol = quote.symbol
        day = quote.day
        close = quote.price
        if close is None:
            if verbose:
                print(f"  x {day}: -null- (skipping)")
            continue

        with db.conn.cursor() as cur:
            cur.execute(
                """SELECT close FROM assetprices
                WHERE ticker = %(ticker)s AND day = %(day)s;""",
                {"ticker": symbol, "day": day},
            )

            if cur.rowcount == 0:
                cur.execute(
                    """
                    INSERT INTO assetprices (ticker, day, ask, bid, close)
                    VALUES (%(ticker)s, %(day)s,
                        %(close)s, %(close)s, %(close)s)
                    ON CONFLICT DO NOTHING;""",
                    {"ticker": symbol, "day": day, "close": close},
                )
                if verbose:
                    print(f"  + {day}:  -.-- -> {close:.2f}")
            else:
                old = cur.fetchone().close
                if not math.isclose(float(old), close, abs_tol=0.0051):
                    cur.execute(
                        """UPDATE assetprices
                        SET ask = %(close)s, bid = %(close)s, close = %(close)s
                        WHERE ticker = %(ticker)s AND day = %(day)s;""",
                        {"ticker": symbol, "day": day, "close": close},
                    )
                    if verbose:
                        print(f"  + {day}: {old} -> {close:.2f}")
