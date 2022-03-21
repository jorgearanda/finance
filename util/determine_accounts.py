from db import db


def determine_accounts(accounts):
    """Parse `accounts` parameter to see which accounts to include."""
    if accounts is None:
        db.ensure_connected()
        cur = db.conn.execute("""SELECT name FROM accounts;""")
        return [x.name for x in cur.fetchall()]
    if type(accounts) is str:
        return accounts.split(",") if "," in accounts else [accounts]
    return accounts
