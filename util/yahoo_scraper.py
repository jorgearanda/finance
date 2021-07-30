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

    def get_ticker_prices(self, symbols):
        """
        Fetch price data from Yahoo for the symbols requested.

        Returns a dictionary with symbol keys and price values
        (in the form of nested day: price dictionaries).
        """
        if self.cookie is None:
            self.cookie, self.crumb = self._get_cookie_and_crumb()
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
        if self.verbose:
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
        prices = {line.day: line.price for line in price_lines}
        return symbol, prices

    def _extract_price_lines(self, res):
        symbol = re.search(r"download/(.*)\?", res.request.url).group(1)
        price_lines = [
            Quote(symbol, line) for line in res.text.split("\n")[1:] if len(line) > 0
        ]
        return symbol, price_lines


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
