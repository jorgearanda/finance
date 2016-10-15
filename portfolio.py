from datetime import date, datetime, timedelta
import psycopg2

import config


class Portfolio():
    def __init__(self, account=None, env='dev', conn=None):
        if conn is None:
            self.conn = psycopg2.connect(database=config.database[env])
        else:
            self.conn = conn
        self.account = account
        self.performance = []
        self.load()

    def load(self):
        self.load_market_days()

    def load_market_days(self):
        if self.account is None:
            self.date_created = self.get_earliest_account_start()
        else:
            self.date_created = self.get_account_start()

        cur = self.conn.cursor()
        cur.execute('''
                    SELECT day, open
                    FROM marketDays
                    WHERE day >= %s AND day <= %s
                    ORDER BY day;''',
                    (self.date_created, date.today()))

        self.performance = [{'date': row[0], 'open': row[1]} for row in cur.fetchall()]
        cur.close()

        if self.performance[0]['date'] > self.date_created:
            raise DataError('marketDays table starts after this account was opened')
        if self.performance[-1]['date'] < date.today():
            raise DataError('marketDays table ends before today')

    def get_earliest_account_start(self):
        cur = self.conn.cursor()
        cur.execute('''
                    SELECT MIN(dateCreated)
                    FROM accounts;''')

        start = cur.fetchone()[0]
        cur.close()

        if start is None:
            raise DataError('No account records found')

        return start

    def get_account_start(self):
        cur = self.conn.cursor()
        cur.execute('''
                    SELECT dateCreated
                    FROM accounts
                    WHERE name = %s;''',
                    (self.account,))

        if cur.rowcount != 1:
            raise DataError('No account record found for {name}'.format(name=self.account))

        account_start = cur.fetchone()[0]
        cur.close()

        return account_start


class DataError(Exception):
    pass
