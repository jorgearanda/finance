import calendar
import grequests
import re
import requests

from datetime import date, datetime as dt, timedelta


class YahooScraper:
    auth_url = "https://finance.yahoo.com/quote/VAB.TO/history?p=VAB.TO"
    user_agent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36"
    timeout = 5  # seconds

    def __init__(self, verbose=False, look_back_days=30):
        self.verbose = verbose
        self.look_back_days = look_back_days
        self.cookie = self.crumb = None

    def get_quotes(self, symbols):
        """
        Fetch price quotes from Yahoo for the symbols requested.

        Returns a list of Quote objects with symbol, day, and price attributes.
        """
        if self.cookie is None:
            self.cookie, self.crumb = self._get_cookie_and_crumb()
        responses = self._fetch_quotes(symbols)
        quotes = []
        for res in responses:
            quotes.extend(self._parse_quotes(res))
        return quotes

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

    def _fetch_quotes(self, symbols):
        if self.verbose:
            print("* Fetching quotes")
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

    def _parse_quotes(self, res):
        symbol = self._symbol_from_url(res)
        return [
            Quote(symbol, line) for line in res.text.split("\n")[1:] if len(line) > 0
        ]

    def _symbol_from_url(self, res):
        return re.search(r"download/(.*)\?", res.request.url).group(1)


class Quote:
    def __init__(self, symbol, line):
        """
        Yahoo result lines are CSVs with the format:
        Date,Open,High,Low,Close,Adj Close,Volume
        """
        self.symbol = symbol
        self.day = dt.strptime(line.split(",")[0], "%Y-%m-%d").date()
        price = line.split(",")[4]
        self.price = float(price) if price != "null" else None
