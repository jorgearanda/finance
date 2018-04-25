from docopt import docopt

from portfolio import Portfolio


usage = """
Provide a snapshot of the state of a portfolio.

Usage:
    snapshot.py [-h] [--accounts <accounts>] [--update] [--verbose]

Options:
    -h --help                      Show this
    -a <accts> --accounts <accts>  Include only data for the account(s) listed
    -u --update                    Fetch new prices from external server
    -v --verbose                   Use verbose mode
"""


def snapshot(args):
    p = Portfolio(
        accounts=args['--accounts'],
        update=args['--update'],
        verbose=args['--verbose'])
    latest = p.latest()
    prev_month = p.last_month()
    if prev_month is not None:
        prev_month_profit = prev_month['month_profit']
        prev_month_returns = prev_month['month_returns']
        month_profit = latest['profit'] - prev_month['profit']
        month_returns = month_profit / prev_month['total_value']
    else:
        prev_month_profit = 0
        prev_month_returns = 0
        month_profit = latest['profit']
        month_returns = latest['returns']

    print('Portfolio Snapshot')
    print('===========================================================')
    print(f'Total Value:  {latest["total_value"]:12,.2f}')
    print(f'Profit:       {latest["profit"]:12,.2f}')
    print()
    print(
        f'TWRR:         {latest["twrr"] * 100:12,.2f}%     ' +
        f'TWRR Annual:  {latest["twrr_annualized"] * 100:12,.2f}%')
    print(
        f'MWRR:         {latest["mwrr"] * 100:12,.2f}%     ' +
        f'MWRR Annual:  {latest["mwrr_annualized"] * 100:12,.2f}%')
    print(f'Total Returns:{latest["returns"] * 100:12,.2f}%')
    print()
    print(f'Day Profit:   {latest["day_profit"]:12,.2f}')
    print(f'Day Returns:  {latest["day_returns"] * 100:12,.2f}%')
    print()
    print(
        f'Month Profit: {month_profit:12,.2f}      ' +
        f'Last Month:   {prev_month_profit:12,.2f}')
    print(
        f'Month Returns:{month_returns * 100:12,.2f}%     ' +
        f'Last Month:   {prev_month_returns * 100:12,.2f}%')


if __name__ == '__main__':
    args = docopt(usage, argv=None, help=True, options_first=False)
    snapshot(args)
