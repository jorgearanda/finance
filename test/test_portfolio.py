from datetime import date, timedelta
from freezegun import freeze_time
from nose.tools import with_setup, assert_raises

from portfolio import Portfolio, DataError
from test import unit_utils


def create_account():
    cur = unit_utils.conn.cursor()
    cur.execute('''INSERT INTO accountTypes (name, tax) VALUES ('RRSP', 'deferred');''')
    cur.execute('''INSERT INTO investors (name) VALUES ('First Last');''')
    cur.execute('''
                INSERT INTO accounts (name, accountType, investor, dateCreated)
                VALUES ('RRSP1', 'RRSP', 'First Last', '2016-10-10');''')
    unit_utils.conn.commit()
    cur.close()


def populate_market_days(from_date=date(2016, 10, 10), to_date=date(2016, 12, 31)):
    holidays = [date(2016, 10, 10), date(2016, 12, 26), date(2016, 12, 27)]
    cur = unit_utils.conn.cursor()
    day = from_date
    while day <= to_date:
        open_market = True
        if day.weekday() in [5, 6] or day in holidays:
            open_market = False
        cur.execute('''
                    INSERT INTO marketDays (day, open)
                    VALUES (%s, %s)
                    ON CONFLICT DO NOTHING;''',
                    (str(day), open_market))
        day += timedelta(days=1)
    unit_utils.conn.commit()
    cur.close()


@with_setup(unit_utils.setup, unit_utils.teardown)
def test_simple_portfolio_creation():
    with freeze_time(date(2016, 10, 15)):
        create_account()
        populate_market_days()

        pf = Portfolio(env='test', conn=unit_utils.conn)

    assert pf is not None
    assert pf.performance is not None
    assert len(pf.performance) == 6
    assert pf.performance[0]['date'] == date(2016, 10, 10)
    assert pf.performance[-1]['date'] == date(2016, 10, 15)


@with_setup(unit_utils.setup, unit_utils.teardown)
def test_single_account_portfolio_creation():
    with freeze_time(date(2016, 10, 15)):
        create_account()
        populate_market_days()

        pf = Portfolio(account='RRSP1', env='test', conn=unit_utils.conn)

    assert pf is not None
    assert pf.performance is not None
    assert len(pf.performance) == 6
    assert pf.performance[0]['date'] == date(2016, 10, 10)
    assert pf.performance[-1]['date'] == date(2016, 10, 15)


@with_setup(unit_utils.setup, unit_utils.teardown)
def test_portfolio_class_connects_if_not_given_connection():
    with freeze_time(date(2016, 10, 15)):
        create_account()
        populate_market_days()

        pf = Portfolio(env='test')

    assert pf is not None


@with_setup(unit_utils.setup, unit_utils.teardown)
def test_portfolio_creation_raises_on_no_accounts():
    assert_raises(DataError, Portfolio, None, 'test', unit_utils.conn)


@with_setup(unit_utils.setup, unit_utils.teardown)
def test_portfolio_creation_raises_on_no_accounts_with_specific_account_selected():
    assert_raises(DataError, Portfolio, 'RRSP1', 'test', unit_utils.conn)


@with_setup(unit_utils.setup, unit_utils.teardown)
def test_portfolio_class_raises_when_account_starts_before_market_days():
    with freeze_time(date(2016, 10, 15)):
        create_account()
        populate_market_days(from_date=date(2016, 10, 11))

        assert_raises(DataError, Portfolio, None, 'test', unit_utils.conn)


@with_setup(unit_utils.setup, unit_utils.teardown)
def test_portfolio_class_raises_when_market_days_end_before_today():
    with freeze_time(date(2016, 10, 16)):
        create_account()
        populate_market_days(from_date=date(2016, 10, 10), to_date=date(2016, 10, 15))

        assert_raises(DataError, Portfolio, None, 'test', unit_utils.conn)
