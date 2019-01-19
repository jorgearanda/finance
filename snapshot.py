from docopt import docopt

from portfolio import Portfolio


usage = """
Provide a snapshot of the state of a portfolio.

Usage:
    snapshot.py [-h] [-a <accts>] [-u] [-v] [-p] [-r] [-m] [-y]

Options:
    -h --help                      Show this
    -a <accts> --accounts <accts>  Include only data for the account(s) listed
    -m --months                    Include a tabular report of monthly performance
    -p --positions                 Include position data
    -r --recent                    Include recent month and year performance
    -u --update                    Fetch new prices from external server
    -v --verbose                   Use verbose mode
    -y --years                     Include a tabular report of yearly performance
"""


def snapshot(args):
    p = Portfolio(
        accounts=args["--accounts"], update=args["--update"], verbose=args["--verbose"]
    )
    latest = p.latest()

    rep = []
    rep.append("Portfolio Snapshot")
    rep.append("===========================================================")
    rep.append(f'Total Value:  {latest["total_value"]:12,.2f}')
    rep.append(f'Profit:       {latest["profit"]:12,.2f}')
    rep.append("")
    rep.append(
        f'TWRR:         {latest["twrr"] * 100:12,.2f}%     '
        + f'TWRR Annual:  {latest["twrr_annualized"] * 100:12,.2f}%'
    )
    rep.append(
        f'MWRR:         {latest["mwrr"] * 100:12,.2f}%     '
        + f'MWRR Annual:  {latest["mwrr_annualized"] * 100:12,.2f}%'
    )
    rep.append(
        f'Total Returns:{latest["returns"] * 100:12,.2f}%     '
        + f'Drawdown:     {latest["current_drawdown"] * 100:12,.2f}%'
    )
    rep.append("")
    rep.append(f'Day Profit:   {latest["day_profit"]:12,.2f}')
    rep.append(f'Day Returns:  {latest["day_returns"] * 100:12,.2f}%')

    if args["--recent"]:
        prev_month = p.previous_month()
        if prev_month is None:
            prev_month_profit, prev_month_twrr, prev_month_mwrr = 0, 0, 0
        else:
            prev_month_profit = prev_month["month_profit"]
            prev_month_twrr = prev_month["month_twrr"]
            prev_month_mwrr = prev_month["month_mwrr"]

        current_month = p.current_month()
        if current_month is None:
            current_month_profit, current_month_twrr, current_month_mwrr = 0, 0, 0
        else:
            current_month_profit = current_month["month_profit"]
            current_month_twrr = current_month["month_twrr"]
            current_month_mwrr = current_month["month_mwrr"]

        prev_year = p.previous_year()
        if prev_year is None:
            prev_year_profit, prev_year_twrr, prev_year_mwrr = 0, 0, 0
        else:
            prev_year_profit = prev_year["year_profit"]
            prev_year_twrr = prev_year["year_twrr"]
            prev_year_mwrr = prev_year["year_mwrr"]

        current_year = p.current_year()
        if current_year is None:
            current_year_profit, current_year_twrr, current_year_mwrr = 0, 0, 0
        else:
            current_year_profit = current_year["year_profit"]
            current_year_twrr = current_year["year_twrr"]
            current_year_mwrr = current_year["year_mwrr"]
        rep.append("")
        rep.append(
            f"Month Profit: {current_month_profit:12,.2f}      "
            + f"Last Month:   {prev_month_profit:12,.2f}"
        )
        rep.append(
            f"Month TWRR:   {current_month_twrr * 100:12,.2f}%     "
            + f"Last Month:   {prev_month_twrr * 100:12,.2f}%"
        )
        rep.append(
            f"Month MWRR:   {current_month_mwrr * 100:12,.2f}%     "
            + f"Last Month:   {prev_month_mwrr * 100:12,.2f}%"
        )
        rep.append("")
        rep.append(
            f"Year Profit:  {current_year_profit:12,.2f}      "
            + f"Last Year:    {prev_year_profit:12,.2f}"
        )
        rep.append(
            f"Year TWRR:    {current_year_twrr * 100:12,.2f}%     "
            + f"Last Year:    {prev_year_twrr * 100:12,.2f}%"
        )
        rep.append(
            f"Year MWRR:    {current_year_mwrr * 100:12,.2f}%     "
            + f"Last Year:    {prev_year_mwrr * 100:12,.2f}%"
        )

    if args["--years"]:
        rep.append("")
        rep.append("                         Total                        Yearly")
        rep.append(
            "Year         Value      Profit    TWRR     MWRR       "
            + "Profit    TWRR     MWRR"
        )
        years = p.by_year
        for key in years.index:
            rep.append(
                f"{str(key)[:4]}  "
                + f"{years.ix[key]['total_value']:12,.2f}"
                + f"{years.ix[key]['profit']:12,.2f}"
                + f"{years.ix[key]['twrr'] * 100:8,.2f}%"
                + f"{years.ix[key]['mwrr'] * 100:8,.2f}%"
                + f"{years.ix[key]['year_profit']:12,.2f}"
                + f"{years.ix[key]['year_twrr'] * 100:8,.2f}%"
                + f"{years.ix[key]['year_mwrr'] * 100:8,.2f}%"
            )

    if args["--months"]:
        rep.append("")
        rep.append("                            Total                       Monthly")
        rep.append(
            "Month           Value      Profit    TWRR     MWRR       "
            + "Profit    TWRR     MWRR"
        )
        months = p.by_month
        for key in months.index:
            rep.append(
                f"{str(key)[:7]}  "
                + f"{months.ix[key]['total_value']:12,.2f}"
                + f"{months.ix[key]['profit']:12,.2f}"
                + f"{months.ix[key]['twrr'] * 100:8,.2f}%"
                + f"{months.ix[key]['mwrr'] * 100:8,.2f}%"
                + f"{months.ix[key]['month_profit']:12,.2f}"
                + f"{months.ix[key]['month_twrr'] * 100:8,.2f}%"
                + f"{months.ix[key]['month_mwrr'] * 100:8,.2f}%"
            )

    if args["--positions"]:
        rep.append("")
        rep.append("Ticker                Price           Value      Weight")
        weights = p.positions.weights.ix[-1]
        values = p.positions.market_values.ix[-1]
        prices = p.tickers.prices.ix[-1]
        day_returns = p.tickers.changes.ix[-1]
        for position in weights.index:
            if position != "Cash":
                rep.append(
                    f"{position:6}:      "
                    + f"{prices[position]:6,.2f} "
                    + f"({day_returns[position] * 100:5,.2f}%)   "
                    + f"{values[position]:12,.2f}   "
                    + f"{weights[position] * 100:8,.2f}%"
                )
            else:
                rep.append(
                    f"{position:6}:      "
                    + "     -            "
                    + f'{latest["cash"]:12,.2f}   '
                    + f"{weights[position] * 100:8,.2f}%"
                )

    return rep


if __name__ == "__main__":
    args = docopt(usage, argv=None, help=True, options_first=False)
    report = snapshot(args)
    print("\n".join(report))
