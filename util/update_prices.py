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
    if verbose:
        print("===Updating prices===")
    updater = YahooTickerScraper(verbose=verbose)
    for symbol, prices in updater.get_ticker_prices(symbols=ticker_symbols()).items():
        record_prices(symbol, prices)
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


def record_prices(symbol, prices):
    if verbose:
        print(f"* Updating {symbol}")
    for day, close in prices.items():
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


class YahooTickerScraper:
    auth_url = "https://finance.yahoo.com/quote/VAB.TO/history?p=VAB.TO"
    user_agent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36"
    timeout = 5  # seconds

    def __init__(self, verbose=False, look_back_days=30):
        self.verbose = verbose
        self.look_back_days = look_back_days
        self.cookie, self.crumb = self._get_cookie_and_crumb()

    def get_ticker_prices(self, symbols):
        """
        Fetch price data from Yahoo for the symbols requested.

        Returns a dictionary with symbol keys and price values
        (in the form of nested day: price dictionaries).
        """
        responses = self._get_ticker_requests(symbols)
        tickers = {}
        for res in responses:
            symbol, prices = self._extract_prices(res)
            tickers[symbol] = prices
        return tickers

    def _get_cookie_and_crumb(self):
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

    def _get_ticker_requests(self, symbols):
        if verbose:
            print("* Getting tickers")
        return grequests.map(
            grequests.get(
                f"https://query1.finance.yahoo.com/v7/finance/download/{symbol}"
                f"?period1={self._ts_from()}&period2={self._ts_to()}&"
                f"interval=1d&events=historical&crumb={self.crumb}",
                headers={"User-Agent": self.user_agent},
                cookies=self.cookie,
                timeout=self.timeout,
            )
            for symbol in symbols
        )

    def _ts_from(self):
        return calendar.timegm(
            (date.today() - timedelta(days=self.look_back_days)).timetuple()
        )

    def _ts_to(self):
        return calendar.timegm((date.today() + timedelta(days=1)).timetuple())

    def _extract_prices(self, res):
        symbol, price_lines = self._extract_price_lines(res)
        prices = {
            self._day_from_price_line(line): self._closing_price_from_price_line(line)
            for line in price_lines
            if len(line) > 0
        }
        return symbol, prices

    def _extract_price_lines(self, res):
        symbol = re.search(r"download/(.*)\?", res.request.url).group(1)
        price_lines = res.text.split("\n")[1:]
        return symbol, price_lines

    def _day_from_price_line(self, line):
        return dt.strptime(line.split(",")[0], "%Y-%m-%d").date()

    def _closing_price_from_price_line(self, line):
        closing_price = line.split(",")[4]
        return float(closing_price) if closing_price != "null" else None
