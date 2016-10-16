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
        self.load_transactions()

    def load_market_days(self):
        self.date_created = self.get_date_created()

        cur = self.conn.cursor()
        cur.execute('''
                    SELECT day, open
                    FROM marketDays
                    WHERE day >= %(created)s AND day <= %(today)s
                    ORDER BY day;''',
                    {'created': self.date_created, 'today': date.today()})

        self.performance = [{'date': row[0], 'open': row[1]} for row in cur.fetchall()]
        cur.close()

        if self.performance[0]['date'] > self.date_created:
            raise DataError('marketDays table starts after this account was opened')
        if self.performance[-1]['date'] < date.today():
            raise DataError('marketDays table ends before today')

    def load_transactions(self):
        pass

    def get_date_created(self):
        cur = self.conn.cursor()
        cur.execute('''
                    SELECT MIN(dateCreated)
                    FROM accounts
                    WHERE %(name)s is null OR name = %(name)s;''',
                    {'name': self.account})

        start = cur.fetchone()[0]
        cur.close()

        if start is None:
            raise DataError('No account records found')

        return start


class DataError(Exception):
    pass
