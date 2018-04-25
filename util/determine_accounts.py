from db import db


def determine_accounts(accounts):
    """Parse `accounts` parameter to see which accounts to include."""
    if accounts is None:
        db.ensure_connected()
        with db.conn.cursor() as cur:
            cur.execute('''SELECT name FROM accounts;''')
            return [x.name for x in cur.fetchall()]
    if type(accounts) is str:
        if ',' in accounts:
            return accounts.split(',')
        else:
            return [accounts]
    return accounts
