from datetime import date

from util.price_updater import PriceUpdater


def test_price_updater_initializes():
    updater = PriceUpdater()
    assert updater.verbose is False
    assert updater.scraper is not None
