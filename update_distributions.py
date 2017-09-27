from docopt import docopt
import pandas as pd
import psycopg2
from psycopg2.extras import NamedTupleCursor

import config

usage = '''
Load distributions from CSV file into the database.

Usage:
    update_distributions.py [-h] [--env <env>]

Options:
    -h --help               Show this
    -e <env> --env <env>    Environment to load prices into [default: dev]
'''

conn = None


def connect(env):
    global conn
    conn = psycopg2.connect(database=config.db[env]['db'], user=config.db[env]['user'], cursor_factory=NamedTupleCursor)


def main(args):
    env = args['--env']
    connect(env)
    cur = conn.cursor()
    table = pd.read_csv('populate/distributions.csv')
    for row in table.iterrows():
        if row[1]['Date'] >= '2016-09-08':  # TODO: so hacky; fix
            cur.execute('''
                INSERT INTO distributions (ticker, day, type, amount)
                VALUES (%(ticker)s, %(day)s, %(type)s, %(amount)s) ON CONFLICT DO NOTHING;''',
                {
                    'ticker': row[1]['Ticker'],
                    'day': row[1]['Date'],
                    'type': row[1]['Type'],
                    'amount': row[1]['Amount']
                })

    cur.close()
    conn.commit()


if __name__ == '__main__':
    args = docopt(usage, argv=None, help=True, version=None, options_first=False)
    main(args)
