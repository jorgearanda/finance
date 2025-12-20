from sqlalchemy import text

from db import db
from populate_market_days import populate_market_days


def test_populate_market_days_creates_correct_entries():
    # Populate a specific date range that includes weekdays, weekends, and a known holiday
    # March 2-8, 2017: Includes weekend (Mar 4-5 Sat/Sun) and weekdays
    populate_market_days("2017-03-02", "2017-03-08")

    # Query all inserted market days
    result = db.conn.execute(
        text("SELECT day, open FROM marketDays ORDER BY day")
    ).fetchall()

    assert len(result) == 7  # 7 days total

    # Verify specific days
    days_dict = {str(row[0]): row[1] for row in result}

    # Weekdays should be open (1)
    assert days_dict["2017-03-02"] == 1  # Thursday
    assert days_dict["2017-03-03"] == 1  # Friday
    assert days_dict["2017-03-06"] == 1  # Monday
    assert days_dict["2017-03-07"] == 1  # Tuesday
    assert days_dict["2017-03-08"] == 1  # Wednesday

    # Weekend should be closed (0)
    assert days_dict["2017-03-04"] == 0  # Saturday
    assert days_dict["2017-03-05"] == 0  # Sunday


def test_populate_market_days_marks_holidays_as_closed():
    # Test with a date range that includes a known holiday
    # December 25-27, 2023: includes Christmas (Dec 25) and Boxing Day (Dec 26)
    populate_market_days("2023-12-25", "2023-12-27")

    result = db.conn.execute(
        text("SELECT day, open FROM marketDays WHERE day >= '2023-12-25' ORDER BY day")
    ).fetchall()

    days_dict = {str(row[0]): row[1] for row in result}

    # Christmas and Boxing Day should be closed (holidays)
    assert days_dict["2023-12-25"] == 0  # Monday, Christmas
    assert days_dict["2023-12-26"] == 0  # Tuesday, Boxing Day
    assert days_dict["2023-12-27"] == 1  # Wednesday, regular day


def test_populate_market_days_handles_duplicates():
    # Populate the same date range twice
    populate_market_days("2017-04-10", "2017-04-12")
    initial_count = db.conn.execute(text("SELECT COUNT(*) FROM marketDays")).fetchone()[
        0
    ]

    # Run again with overlapping range
    populate_market_days("2017-04-11", "2017-04-13")
    final_count = db.conn.execute(text("SELECT COUNT(*) FROM marketDays")).fetchone()[0]

    # Should have 4 unique days total (Apr 10, 11, 12, 13)
    # Not 3 + 3 = 6 days
    assert final_count == 4
    assert final_count > initial_count  # One new day added (Apr 13)


def test_populate_market_days_weekend_check():
    # Test a week with Saturday and Sunday
    # January 1-7, 2023: Sun Jan 1 (holiday), Mon 2 (holiday), Tue-Fri (weekdays), Sat-Sun (weekend)
    populate_market_days("2023-01-01", "2023-01-08")

    result = db.conn.execute(
        text(
            "SELECT day, open FROM marketDays WHERE day >= '2023-01-01' AND day <= '2023-01-08' ORDER BY day"
        )
    ).fetchall()

    days_dict = {str(row[0]): row[1] for row in result}

    # Jan 1 (Sunday) - weekend AND holiday
    assert days_dict["2023-01-01"] == 0
    # Jan 2 (Monday) - holiday (from holidays list)
    assert days_dict["2023-01-02"] == 0
    # Jan 3-5 (Tue-Thu) - regular weekdays
    assert days_dict["2023-01-03"] == 1
    assert days_dict["2023-01-04"] == 1
    assert days_dict["2023-01-05"] == 1
    # Jan 6 (Friday) - regular weekday
    assert days_dict["2023-01-06"] == 1
    # Jan 7 (Saturday) - weekend
    assert days_dict["2023-01-07"] == 0
    # Jan 8 (Sunday) - weekend
    assert days_dict["2023-01-08"] == 0
