from util.yahoo_scraper import YahooScraper


def test_yahoo_scraper_initializes():
    y = YahooScraper()
    assert y.look_back_days == 30
    assert y.cookie is None
