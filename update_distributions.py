from docopt import docopt
import pandas as pd
from sqlalchemy import text

from db import db

usage = """
Load distributions from CSV file into the database.

Usage:
    update_distributions.py [-h] [--env <env>]

Options:
    -h --help               Show this
    -e <env> --env <env>    Environment to load prices into [default: dev]
"""

def connect(env):
    db.ensure_connected(env)


def main(args):
    env = args["--env"]
    connect(env)
    table = pd.read_csv("populate/distributions.csv")

    # Filter data using vectorized operation
    filtered = table[table["Date"] >= "2016-09-08"]  # TODO: so hacky; fix

    # Prepare data for batch insert
    data = [
        {
            "ticker": row.Ticker,
            "day": row.Date,
            "type": row.Type,
            "amount": row.Amount,
        }
        for row in filtered.itertuples()
    ]

    # Insert records using SQLAlchemy
    for record in data:
        db.conn.execute(
            text("""
                INSERT INTO distributions (ticker, day, type, amount)
                VALUES (:ticker, :day, :type, :amount) ON CONFLICT DO NOTHING;
            """),
            record
        )


if __name__ == "__main__":
    args = docopt(usage, argv=None, help=True, version=None, options_first=False)
    main(args)
