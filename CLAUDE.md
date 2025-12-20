# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Personal finance portfolio tracking application. Tracks investment positions, calculates returns (TWRR, MWRR), and generates performance snapshots. Uses SQLite for data storage and pandas for calculations.

## Commands

### Install dependencies
```bash
pip install -r requirements.txt
```

### Run tests
```bash
pytest --cov-report term-missing --cov
```

### Run a single test file
```bash
pytest path/to/test_file.py
```

### Run a specific test
```bash
pytest path/to/test_file.py::test_function_name
```

### Run tests in parallel
```bash
pytest -n auto  # Use all CPU cores
pytest -n 4     # Use 4 workers
```

### Generate portfolio snapshot
```bash
python snapshot.py -u -v -r -p  # update prices, verbose, recent periods, positions
python snapshot.py -a "RRSP1"   # specific account
```

## Architecture

### Core Components

**Portfolio** (`portfolio.py`): Main entry point. Aggregates deposits, tickers, and positions to calculate daily/monthly/yearly metrics (returns, drawdowns, Sharpe ratio, etc.) in pandas DataFrames (`by_day`, `by_month`, `by_year`).

**Positions** (`components/positions.py`): Collection of Position objects. Tracks units, costs, market values, and returns for each position.

**Tickers** (`components/tickers.py`): Collection of Ticker objects. Stores price history, daily changes, distributions, and correlations.

**Deposits** (`components/deposits.py`): Tracks capital contributions by account.

### Data Layer

**db/db.py**: Database connection singleton. Uses SQLAlchemy with SQLite. Supports both file-based (production: `data/finance.db`) and in-memory (tests: `:memory:`) databases. Environment controlled by `_env` variable (`dev`/`test`).

**db/data.py**: `Data` class wraps database queries for dependency injection in tests.

**db/schemas.sql**: SQLite schema with CHECK constraints for data validation. Uses INTEGER for booleans (0=false, 1=true). Archived PostgreSQL schema available at `db/schemas_postgresql.sql`.

### Price Updates

**util/yahoo_scraper.py**: `YahooScraper` fetches historical quotes from Yahoo Finance API using httpx with async/await for parallel requests.

**util/price_updater.py**: `PriceUpdater` orchestrates fetching new quotes and upserting them to the database.

### Configuration

**config.py**: Database connection settings (dev/test) and Sharpe ratio risk-free rate. Both dev and test environments use SQLite.

### Testing

Tests use in-memory SQLite databases (`:memory:`). Each test automatically gets a fresh isolated database with schema created by an autouse fixture in `conftest.py`. The `simple_fixture()` function inserts sample test data (accounts, tickers, prices, transactions).

Tests can be run in parallel using `pytest -n <workers>` (requires pytest-xdist), though sequential execution is currently faster for this small test suite. Use `pytest -n auto` to automatically use all available CPU cores.
