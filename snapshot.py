from docopt import docopt

from portfolio import Portfolio


usage = """
Provide a snapshot of the state of a portfolio.

Usage:
    snapshot.py [-h] [--accounts <accts>] [--update] [--verbose] [--positions]

Options:
    -h --help                      Show this
    -a <accts> --accounts <accts>  Include only data for the account(s) listed
    -p --positions                 Include position data
    -u --update                    Fetch new prices from external server
    -v --verbose                   Use verbose mode
"""


def snapshot(args):
    p = Portfolio(
        accounts=args['--accounts'],
        update=args['--update'],
        verbose=args['--verbose'])
    latest = p.latest()
    prev_month = p.previous_month()
    current_month = p.current_month()
    if prev_month is not None:
        prev_month_profit = prev_month['month_profit']
        prev_month_returns = prev_month['month_returns']
    else:
        prev_month_profit = 0
        prev_month_returns = 0

    if current_month is not None:
        current_month_profit = current_month['month_profit']
        current_month_returns = current_month['month_returns']
    else:
        current_month_profit = 0
        current_month_returns = 0

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
        f'Month Profit: {current_month_profit:12,.2f}      ' +
        f'Last Month:   {prev_month_profit:12,.2f}')
    print(
        f'Month Returns:{current_month_returns * 100:12,.2f}%     ' +
        f'Last Month:   {prev_month_returns * 100:12,.2f}%')

    if args['--positions']:
        print()
        print('Ticker               Weight')
        weights = p.positions.weights.ix[-1]
        for position in weights.index:
            print(f'{position:6}:       {weights[position] * 100:12,.2f}%')


if __name__ == '__main__':
    args = docopt(usage, argv=None, help=True, options_first=False)
    snapshot(args)
