from datetime import date, timedelta
from decimal import Decimal
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
    cur.execute('''INSERT INTO assetClasses (name, domesticCurrency) VALUES ('Cash', true);''')
    cur.execute('''INSERT INTO assetClasses (name, domesticCurrency) VALUES ('Domestic Equity', true);''')
    cur.execute('''INSERT INTO assets (ticker, class) VALUES ('Cash', 'Cash');''')
    cur.execute('''INSERT INTO assets (ticker, class) VALUES ('VVV.TO', 'Domestic Equity');''')
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


def populate_deposits():
    cur = unit_utils.conn.cursor()
    cur.execute('''
                INSERT INTO transactions (day, txType, account, source, target, units, unitPrice, commission, total)
                VALUES ('2016-10-11', 'deposit', 'RRSP1', null, 'Cash', null, null, null, 1000);''')
    unit_utils.conn.commit()
    cur.close()


def populate_buys():
    cur = unit_utils.conn.cursor()
    cur.execute('''
                INSERT INTO transactions (day, txType, account, source, target, units, unitPrice, commission, total)
                VALUES ('2016-10-12', 'buy', 'RRSP1', 'Cash', 'VVV.TO', 10, 12.34, 0.03, 123.43);''')
    cur.execute('''
                INSERT INTO transactions (day, txType, account, source, target, units, unitPrice, commission, total)
                VALUES ('2016-10-14', 'buy', 'RRSP1', 'Cash', 'VVV.TO', 20, 12.34, 0.03, 246.86);''')
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
    dates = list(pf.performance.items())
    assert dates[0][0] == date(2016, 10, 10)
    assert dates[-1][0] == date(2016, 10, 15)


@with_setup(unit_utils.setup, unit_utils.teardown)
def test_single_account_portfolio_creation():
    with freeze_time(date(2016, 10, 15)):
        create_account()
        populate_market_days()

        pf = Portfolio(account='RRSP1', env='test', conn=unit_utils.conn)

    assert pf is not None
    assert pf.performance is not None
    assert len(pf.performance) == 6
    dates = list(pf.performance.items())
    assert dates[0][0] == date(2016, 10, 10)
    assert dates[-1][0] == date(2016, 10, 15)


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


@with_setup(unit_utils.setup, unit_utils.teardown)
def test_day_deposits():
    with freeze_time(date(2016, 10, 15)):
        create_account()
        populate_market_days()
        populate_deposits()

        pf = Portfolio(env='test', conn=unit_utils.conn)

    assert pf.performance[date(2016, 10, 10)]['dayDeposits'] == 0
    assert pf.performance[date(2016, 10, 11)]['dayDeposits'] == 1000
    assert pf.performance[date(2016, 10, 12)]['dayDeposits'] == 0


@with_setup(unit_utils.setup, unit_utils.teardown)
def test_total_deposits():
    with freeze_time(date(2016, 10, 15)):
        create_account()
        populate_market_days()
        populate_deposits()

        pf = Portfolio(env='test', conn=unit_utils.conn)

    assert pf.performance[date(2016, 10, 10)]['totalDeposits'] == 0
    assert pf.performance[date(2016, 10, 11)]['totalDeposits'] == 1000
    assert pf.performance[date(2016, 10, 12)]['totalDeposits'] == 1000


@with_setup(unit_utils.setup, unit_utils.teardown)
def test_buys_propagate_to_assets_subdictionary():
    with freeze_time(date(2016, 10, 15)):
        create_account()
        populate_market_days()
        populate_buys()

        pf = Portfolio(env='test', conn=unit_utils.conn)

    assert len(pf.performance[date(2016, 10, 11)]['assets']) == 0
    assert len(pf.performance[date(2016, 10, 12)]['assets']) == 1
    assert pf.performance[date(2016, 10, 12)]['assets']['VVV.TO']['units'] == 10
    assert pf.performance[date(2016, 10, 12)]['assets']['VVV.TO']['positionCost'] == Decimal('123.43')
    assert pf.performance[date(2016, 10, 13)]['assets']['VVV.TO']['units'] == 10
    assert pf.performance[date(2016, 10, 13)]['assets']['VVV.TO']['positionCost'] == Decimal('123.43')
    assert pf.performance[date(2016, 10, 14)]['assets']['VVV.TO']['units'] == 30
    assert pf.performance[date(2016, 10, 14)]['assets']['VVV.TO']['positionCost'] == Decimal('370.29')
