import calendar
import grequests
import json
import re
import requests

from datetime import date, datetime as dt, timedelta


class YahooScraper:
    auth_url = "https://finance.yahoo.com/quote/VAB.TO/history/?p=VAB.TO"
    user_agent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15"
    timeout = 5  # seconds

    def __init__(self, look_back_days=30):
        self.look_back_days = look_back_days
        self.crumb = None

    def get_quotes(self, symbols):
        """
        Fetch price quotes from Yahoo for the symbols requested.

        Returns a list of Quote objects with symbol, day, and price attributes.
        """
        if self.crumb is None:
            self.crumb = self._get_crumb()
        responses = self._fetch_quotes(symbols)
        quotes = []
        for res in responses:
            quotes.extend(self._parse_quotes(res))
        return quotes

    def _get_crumb(self):
        req = requests.get(
            self.auth_url,
            headers={
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Encoding": "gzip, deflate, br",
                "Accept-Language": "en-CA,en-US;q=0.9,en;q=0.8",
                "Connection": "keep-alive",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "User-Agent": self.user_agent,
            },
            timeout=self.timeout,
        )
        content = req.content.decode("unicode-escape")
        try:
            crumb = re.search(r"crumb=([A-Za-z0-9]+)", content).group(1)
        except AttributeError:
            crumb = re.search(r'"crumb":"(.*?)"', content).group(1)
        return crumb

    def _fetch_quotes(self, symbols):
        return grequests.map(
            grequests.get(
                f"https://query2.finance.yahoo.com/v8/finance/chart/{symbol}"
                f"?period1={self._ts_from()}&period2={self._ts_to()}&"
                f"interval=1d&events=historical&crumb={self.crumb}",
                headers={"User-Agent": self.user_agent},
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
        payload = json.loads(res.text)
        symbol = payload["chart"]["result"][0]["meta"]["symbol"]
        timestamps = payload["chart"]["result"][0]["timestamp"]
        closes = payload["chart"]["result"][0]["indicators"]["quote"][0]["close"]
        return [Quote(symbol, entry) for entry in zip(timestamps, closes)]

    def _symbol_from_url(self, res):
        return re.search(r"download/(.*)\?", res.request.url).group(1)


class Quote:
    def __init__(self, symbol, entry):
        """
        Entries have two fields, date as a timestamp and closing price.
        """
        self.symbol = symbol
        self.day = date.fromtimestamp(entry[0])
        price = entry[1]
        self.price = float(price) if price != "null" else None
