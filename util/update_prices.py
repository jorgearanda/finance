import calendar
import grequests
import math
import re
import requests

from datetime import date, datetime as dt, timedelta

from db import db

verbose = False
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) "
"AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36"
TIMEOUT = 5.0  # seconds


def update_prices(verbosity=False):
    global verbose
    verbose = verbosity
    if verbose:
        print("===Updating prices===")
    for res in _ticker_data():
        symbol = re.search(r"download/(.*)\?", res.request.url).group(1)
        quote_lines = res.text.split("\n")[1:]
        _update_prices_for_ticker(symbol, quote_lines)
    if verbose:
        print("===Finished updating prices===\n")


def _get_tickers():
    """Get all the tickers to poll from the database."""
    db.ensure_connected()
    with db.conn.cursor() as cur:
        cur.execute(
            """
            SELECT ticker AS name
            FROM assets
            WHERE class != 'Domestic Cash';"""
        )

        return cur.fetchall()


def _get_cookie_and_crumb(symbol):
    """Get cookie and crumb for further calls."""
    url = f"https://finance.yahoo.com/quote/{symbol}/history?p={symbol}"
    req = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT)
    cookie = {"B": req.cookies["B"]}
    content = req.content.decode("unicode-escape")
    match = re.search(r'CrumbStore":{"crumb":"(.*?)"}', content)
    crumb = match.group(1)

    return cookie, crumb


def _update_prices_for_ticker(symbol, lines):
    """Use historical data in `lines` to populate prices table."""
    if verbose:
        print(f"* Updating {symbol}")
    for line in lines:
        if len(line) == 0:
            continue
        values = line.split(",")
        day = dt.strptime(values[0], "%Y-%m-%d").date()
        close = values[4]
        if close == "null":
            if verbose:
                print(f"  x {day}: -null- (skipping)")
            continue
        close = float(close)

        with db.conn.cursor() as cur:
            cur.execute(
                """SELECT close FROM assetprices
                WHERE ticker = %(ticker)s AND day = %(day)s;""",
                {"ticker": symbol, "day": day},
            )

            if cur.rowcount == 0:
                cur.execute(
                    """
                    INSERT INTO assetprices (ticker, day, ask, bid, close)
                    VALUES (%(ticker)s, %(day)s,
                        %(close)s, %(close)s, %(close)s)
                    ON CONFLICT DO NOTHING;""",
                    {"ticker": symbol, "day": day, "close": close},
                )
                if verbose:
                    print(f"  + {day}:  -.-- -> {close:.2f}")
            else:
                old = cur.fetchone().close
                if not math.isclose(float(old), close, abs_tol=0.0051):
                    cur.execute(
                        """UPDATE assetprices
                        SET ask = %(close)s, bid = %(close)s, close = %(close)s
                        WHERE ticker = %(ticker)s AND day = %(day)s;""",
                        {"ticker": symbol, "day": day, "close": close},
                    )
                    if verbose:
                        print(f"  + {day}: {old} -> {close:.2f}")


def _ticker_data():
    if verbose:
        print("* Preparing queries")
    tickers = _get_tickers()
    cookie, crumb = _get_cookie_and_crumb(tickers[0].name)
    ts_from = calendar.timegm((date.today() - timedelta(days=30)).timetuple())
    ts_to = calendar.timegm((date.today() + timedelta(days=1)).timetuple())
    base = "https://query1.finance.yahoo.com/v7/finance/download/"
    params = (
        f"?period1={ts_from}&period2={ts_to}&"
        + f"interval=1d&events=historical&crumb={crumb}"
    )

    return grequests.map(
        grequests.get(
            base + ticker.name + params,
            headers={"User-Agent": USER_AGENT},
            cookies=cookie,
            timeout=TIMEOUT,
        )
        for ticker in tickers
    )
