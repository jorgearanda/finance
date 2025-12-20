from datetime import date, datetime, timedelta
from sqlalchemy import text

from db import db


holidays = [
    date(2016, 10, 10),
    date(2016, 12, 26),
    date(2016, 12, 27),
    date(2017, 1, 2),
    date(2017, 2, 20),
    date(2017, 4, 14),
    date(2017, 5, 22),
    date(2017, 7, 3),
    date(2017, 8, 7),
    date(2017, 9, 4),
    date(2017, 10, 9),
    date(2017, 12, 25),
    date(2017, 12, 26),
    date(2018, 1, 1),
    date(2018, 2, 19),
    date(2018, 3, 30),
    date(2018, 5, 21),
    date(2018, 7, 2),
    date(2018, 8, 6),
    date(2018, 9, 3),
    date(2018, 10, 8),
    date(2018, 12, 25),
    date(2018, 12, 26),
    date(2019, 1, 1),
    date(2019, 2, 18),
    date(2019, 4, 19),
    date(2019, 5, 20),
    date(2019, 7, 1),
    date(2019, 8, 5),
    date(2019, 9, 2),
    date(2019, 10, 14),
    date(2019, 12, 25),
    date(2019, 12, 26),
    date(2020, 1, 1),
    date(2020, 2, 17),
    date(2020, 4, 10),
    date(2020, 5, 18),
    date(2020, 7, 1),
    date(2020, 8, 3),
    date(2020, 9, 7),
    date(2020, 10, 12),
    date(2020, 12, 25),
    date(2020, 12, 28),
    date(2021, 1, 1),
    date(2021, 2, 15),
    date(2021, 4, 2),
    date(2021, 5, 24),
    date(2021, 7, 1),
    date(2021, 8, 2),
    date(2021, 9, 6),
    date(2021, 10, 11),
    date(2021, 12, 27),
    date(2021, 12, 28),
    date(2022, 1, 3),
    date(2022, 2, 21),
    date(2022, 4, 15),
    date(2022, 5, 23),
    date(2022, 7, 1),
    date(2022, 8, 1),
    date(2022, 9, 5),
    date(2022, 10, 10),
    date(2022, 12, 26),
    date(2022, 12, 27),
    date(2023, 1, 2),
    date(2023, 2, 20),
    date(2023, 4, 7),
    date(2023, 5, 22),
    date(2023, 7, 3),
    date(2023, 8, 7),
    date(2023, 9, 4),
    date(2023, 10, 9),
    date(2023, 12, 25),
    date(2023, 12, 26),
    date(2024, 1, 1),
    date(2024, 2, 19),
    date(2024, 3, 29),
    date(2024, 5, 20),
    date(2024, 7, 1),
    date(2024, 8, 5),
    date(2024, 9, 2),
    date(2024, 10, 14),
    date(2024, 12, 25),
    date(2024, 12, 26),
    date(2025, 1, 1),
    date(2025, 2, 17),
    date(2025, 4, 18),
    date(2025, 5, 19),
    date(2025, 7, 1),
    date(2025, 8, 4),
    date(2025, 9, 1),
    date(2025, 10, 13),
    date(2025, 12, 25),
    date(2025, 12, 26),
    date(2026, 1, 1),
    date(2026, 2, 16),
    date(2026, 4, 3),
    date(2026, 5, 18),
    date(2026, 7, 1),
    date(2026, 8, 3),
    date(2026, 9, 7),
    date(2026, 12, 12),
    date(2026, 12, 25),
    date(2026, 12, 26),
    date(2026, 12, 28),
]


def populate_market_days(from_date_str="2016-09-07", to_date_str="2026-12-31"):
    db.ensure_connected()
    day = datetime.strptime(from_date_str, "%Y-%m-%d").date()
    to_date = datetime.strptime(to_date_str, "%Y-%m-%d").date()
    while day <= to_date:
        open_market = (
            1  # Market open (SQLite uses INTEGER for booleans: 1=true, 0=false)
        )
        if day.weekday() in [5, 6] or day in holidays:
            open_market = 0  # Market closed
        db.conn.execute(
            text(
                """
                INSERT INTO marketDays (day, open)
                VALUES (:day, :open)
                ON CONFLICT (day) DO NOTHING;
                """
            ),
            {"day": day, "open": open_market},
        )
        day += timedelta(days=1)


if __name__ == "__main__":
    populate_market_days()
