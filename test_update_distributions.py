import os
import tempfile
from unittest.mock import patch, MagicMock
import pandas as pd
import pytest
from sqlalchemy import text

import update_distributions
from conftest import simple_fixture, simple_fixture_teardown
from db import db


def test_update_distributions_filters_and_inserts():
    """Test that update_distributions correctly filters dates and inserts records."""
    simple_fixture()

    # Add marketdays for test dates
    db.ensure_connected("test")
    db.conn.execute(
        text(
            """INSERT INTO marketdays (day, open)
               VALUES ('2016-09-07', true), ('2016-09-08', true),
                      ('2016-09-09', true), ('2016-09-10', true)"""
        )
    )

    # Create a temporary CSV file with test data
    test_data = pd.DataFrame(
        {
            "Ticker": ["VCN.TO", "VEE.TO", "VCN.TO", "VEE.TO"],
            "Date": ["2016-09-07", "2016-09-08", "2016-09-09", "2016-09-10"],
            "Type": ["income", "income", "capital gains", "income"],
            "Amount": [0.10, 0.20, 0.30, 0.40],
        }
    )

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        test_data.to_csv(f.name, index=False)
        temp_csv = f.name

    try:
        # Patch the CSV file path
        with patch("pandas.read_csv") as mock_read_csv:
            mock_read_csv.return_value = test_data

            # Run the main function
            args = {"--env": "test"}
            update_distributions.main(args)

        # Verify the data was inserted correctly
        db.ensure_connected("test")
        result = db.conn.execute(
            text(
                """SELECT ticker, day, type, amount FROM distributions
               WHERE day BETWEEN '2016-09-07' AND '2016-09-10'
               ORDER BY day, ticker"""
            )
        )
        rows = result.fetchall()

        # Should have 3 rows (filtered out 2016-09-07)
        assert len(rows) == 3
        assert rows[0][0] == "VEE.TO"  # ticker
        assert str(rows[0][1]) == "2016-09-08"  # day
        assert rows[0][2] == "income"  # type
        assert float(rows[0][3]) == 0.20  # amount

        assert rows[1][0] == "VCN.TO"
        assert str(rows[1][1]) == "2016-09-09"
        assert float(rows[1][3]) == 0.30

        assert rows[2][0] == "VEE.TO"
        assert str(rows[2][1]) == "2016-09-10"
        assert float(rows[2][3]) == 0.40

    finally:
        os.unlink(temp_csv)


def test_update_distributions_handles_duplicates():
    """Test that ON CONFLICT DO NOTHING works correctly."""
    simple_fixture()

    # Add marketdays for test dates
    db.ensure_connected("test")
    db.conn.execute(
        text("""INSERT INTO marketdays (day, open) VALUES ('2016-09-08', true)""")
    )

    # Create test data with a duplicate entry
    test_data = pd.DataFrame(
        {
            "Ticker": ["VCN.TO", "VCN.TO"],
            "Date": ["2016-09-08", "2016-09-08"],
            "Type": ["income", "income"],
            "Amount": [0.10, 0.10],
        }
    )

    with patch("pandas.read_csv") as mock_read_csv:
        mock_read_csv.return_value = test_data

        # Run the main function twice
        args = {"--env": "test"}
        update_distributions.main(args)
        update_distributions.main(args)

    # Verify only one record exists
    db.ensure_connected("test")
    result = db.conn.execute(
        text(
            """SELECT COUNT(*) FROM distributions WHERE ticker = 'VCN.TO' AND day = '2016-09-08'"""
        )
    )
    count = result.fetchone()[0]
    assert count == 1


def test_update_distributions_empty_csv():
    """Test that empty CSV after filtering doesn't cause errors."""
    simple_fixture()

    # Create test data where all dates are before cutoff
    test_data = pd.DataFrame(
        {
            "Ticker": ["VCN.TO", "VEE.TO"],
            "Date": ["2016-09-01", "2016-09-07"],
            "Type": ["income", "income"],
            "Amount": [0.10, 0.20],
        }
    )

    with patch("pandas.read_csv") as mock_read_csv:
        mock_read_csv.return_value = test_data

        # Run the main function
        args = {"--env": "test"}
        update_distributions.main(args)

    # Verify no records were inserted for the test date range
    db.ensure_connected("test")
    result = db.conn.execute(
        text(
            """SELECT COUNT(*) FROM distributions
               WHERE day BETWEEN '2016-09-01' AND '2016-09-07'"""
        )
    )
    count = result.fetchone()[0]
    assert count == 0
