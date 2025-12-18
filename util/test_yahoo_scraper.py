from datetime import date

from util.yahoo_scraper import Quote
from util.yahoo_scraper import YahooScraper


def test_yahoo_scraper_initializes():
    y = YahooScraper()
    assert y.look_back_days == 30
    assert y.crumb is None


def test_quote_class():
    # Entry is a tuple of (timestamp, closing_price) from Yahoo JSON API
    timestamp = 1627560000  # 2021-07-29
    q = Quote("SYM", (timestamp, 77.15))
    assert q.symbol == "SYM"
    assert q.day == date(2021, 7, 29)
    assert q.price == 77.15
