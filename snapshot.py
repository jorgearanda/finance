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

    rep = []
    rep.append('Portfolio Snapshot')
    rep.append('===========================================================')
    rep.append(f'Total Value:  {latest["total_value"]:12,.2f}')
    rep.append(f'Profit:       {latest["profit"]:12,.2f}')
    rep.append('')
    rep.append(
        f'TWRR:         {latest["twrr"] * 100:12,.2f}%     ' +
        f'TWRR Annual:  {latest["twrr_annualized"] * 100:12,.2f}%')
    rep.append(
        f'MWRR:         {latest["mwrr"] * 100:12,.2f}%     ' +
        f'MWRR Annual:  {latest["mwrr_annualized"] * 100:12,.2f}%')
    rep.append(
        f'Total Returns:{latest["returns"] * 100:12,.2f}%     ' +
        f'Drawdown:     {latest["current_drawdown"] * 100:12,.2f}%')
    rep.append('')
    rep.append(f'Day Profit:   {latest["day_profit"]:12,.2f}')
    rep.append(f'Day Returns:  {latest["day_returns"] * 100:12,.2f}%')
    rep.append('')
    rep.append(
        f'Month Profit: {current_month_profit:12,.2f}      ' +
        f'Last Month:   {prev_month_profit:12,.2f}')
    rep.append(
        f'Month Returns:{current_month_returns * 100:12,.2f}%     ' +
        f'Last Month:   {prev_month_returns * 100:12,.2f}%')

    if args['--positions']:
        rep.append('')
        rep.append('Ticker                Price           Value      Weight')
        weights = p.positions.weights.ix[-1]
        values = p.positions.market_values.ix[-1]
        prices = p.tickers.prices.ix[-1]
        day_returns = p.tickers.changes.ix[-1]
        for position in weights.index:
            if position != 'Cash':
                rep.append(
                    f'{position:6}:      ' +
                    f'{prices[position]:6,.2f} ' +
                    f'({day_returns[position] * 100:5,.2f}%)   ' +
                    f'{values[position]:12,.2f}   ' +
                    f'{weights[position] * 100:8,.2f}%')
            else:
                rep.append(
                    f'{position:6}:      ' +
                    '     -            ' +
                    f'{latest["cash"]:12,.2f}   ' +
                    f'{weights[position] * 100:8,.2f}%')

    return rep


if __name__ == '__main__':
    args = docopt(usage, argv=None, help=True, options_first=False)
    report = snapshot(args)
    print('\n'.join(report))
