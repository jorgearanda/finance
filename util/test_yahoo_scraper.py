from datetime import date

from util.yahoo_scraper import Quote
from util.yahoo_scraper import YahooScraper


def test_yahoo_scraper_initializes():
    y = YahooScraper()
    assert y.look_back_days == 30
    assert y.crumb is None


def test_quote_class():
    q = Quote("SYM", "2021-07-29,77.22,77.42,77.15,77.15,77.14,56026")
    assert q.symbol == "SYM"
    assert q.day == date(2021, 7, 29)
    assert q.price == 77.15
