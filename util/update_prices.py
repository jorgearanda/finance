import calendar
import grequests
import math
import re
import requests

from datetime import date, datetime as dt, timedelta

from db import db

verbose = False


def update_prices(verbosity=False):
    global verbose
    verbose = verbosity
    updater = YahooTickerScraper(verbose=verbose)
    if verbose:
        print("===Updating prices===")
    for res in updater.get_ticker_requests(symbols=ticker_symbols()):
        symbol, price_lines = updater.extract_price_lines(res)
        _update_prices_for_ticker(symbol, price_lines, updater)
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


def _update_prices_for_ticker(symbol, lines, updater):
    """Use historical data in `lines` to populate prices table."""
    if verbose:
        print(f"* Updating {symbol}")
    for line in lines:
        if len(line) == 0:
            continue
        values = line.split(",")
        day = updater.day_from_price_line(line)
        close = values[4]
        if close == "null":
            if verbose:
                print(f"  x {day}: -null- (skipping)")
            continue
        close = float(close)

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


class YahooTickerScraper:
    def __init__(self, verbose=False):
        self.verbose = verbose
        self.auth_url = "https://finance.yahoo.com/quote/VAB.TO/history?p=VAB.TO"
        self.user_agent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36"
        self.timeout = 5  # seconds
        self.cookie, self.crumb = self.get_cookie_and_crumb()

    def get_cookie_and_crumb(self):
        if self.verbose:
            print("* Getting cookie and crumb")
        req = requests.get(
            self.auth_url,
            headers={"User-Agent": self.user_agent},
            timeout=self.timeout,
        )
        cookie = {"B": req.cookies["B"]}
        content = req.content.decode("unicode-escape")
        match = re.search(r'CrumbStore":{"crumb":"(.*?)"}', content)
        crumb = match.group(1)

        return cookie, crumb

    def get_ticker_requests(self, symbols):
        if verbose:
            print("* Getting tickers")
        ts_from = calendar.timegm((date.today() - timedelta(days=30)).timetuple())
        ts_to = calendar.timegm((date.today() + timedelta(days=1)).timetuple())
        base = "https://query1.finance.yahoo.com/v7/finance/download/"
        params = (
            f"?period1={ts_from}&period2={ts_to}&"
            + f"interval=1d&events=historical&crumb={self.crumb}"
        )

        return grequests.map(
            grequests.get(
                base + symbol + params,
                headers={"User-Agent": self.user_agent},
                cookies=self.cookie,
                timeout=self.timeout,
            )
            for symbol in symbols
        )

    def extract_price_lines(self, res):
        symbol = re.search(r"download/(.*)\?", res.request.url).group(1)
        price_lines = res.text.split("\n")[1:]
        return symbol, price_lines

    def day_from_price_line(self, line):
        return dt.strptime(line.split(",")[0], "%Y-%m-%d").date()
