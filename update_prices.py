from docopt import docopt


usage = '''
Load missing asset prices from Yahoo Finance into the database.

Usage:
    update_prices.py [-h] [-v] [--env <env>]

Options:
    -h --help               Show this
    -e <env> --env <env>    Environment to load prices into [default: dev]
    -v --verbose            Verbose output
'''


def main(args):
    # Connect to database
    # Load assets data
    # For each asset,
    #   request prices from API
    #   load days with asset price currently missing
    #   for each market day with missing price,
    #       find the price from the API
    #       update the database
    pass


if __name__ == '__main__':
    args = docopt(usage, argv=None, help=True, version=None, options_first=False)
    main(args)
