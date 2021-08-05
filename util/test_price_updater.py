from datetime import date

from util.price_updater import PriceUpdater


def test_price_updater_initializes():
    updater = PriceUpdater()
    assert updater.verbose is False
    assert updater.scraper is not None


def test_is_same_price():
    updater = PriceUpdater()
    assert updater._is_same_price(0, 0)
    assert updater._is_same_price(0.000, 0.001)
    assert updater._is_same_price(10.50, 10.4951)
    assert updater._is_same_price(1_000_000.50, 1_000_000.4951)
    assert not updater._is_same_price(0.00, 0.01)
    assert not updater._is_same_price(1_000_000.50, 1_000_000.49)
