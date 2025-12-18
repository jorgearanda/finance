import calendar
import grequests
import json

from datetime import date, timedelta


class YahooScraper:
    user_agent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15"
    timeout = 5  # seconds

    def __init__(self, look_back_days=30):
        self.look_back_days = look_back_days

    def get_quotes(self, symbols):
        """
        Fetch price quotes from Yahoo for the symbols requested.

        Returns a list of Quote objects with symbol, day, and price attributes.
        """
        responses = self._fetch_quotes(symbols)
        quotes = []
        for res in responses:
            quotes.extend(self._parse_quotes(res))
        return quotes

    def _fetch_quotes(self, symbols):
        return grequests.map(
            grequests.get(
                f"https://query2.finance.yahoo.com/v8/finance/chart/{symbol}"
                f"?period1={self._ts_from()}&period2={self._ts_to()}&interval=1d",
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


class Quote:
    def __init__(self, symbol, entry):
        """
        Entries have two fields, date as a timestamp and closing price.
        """
        self.symbol = symbol
        self.day = date.fromtimestamp(entry[0])
        price = entry[1]
        self.price = float(price) if price != "null" and price is not None else None
