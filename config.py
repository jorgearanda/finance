db = {
    "dev": {
        "type": "postgresql",  # Keep PostgreSQL for now
        "db": "finance",
        "user": "jorge"
    },
    "test": {
        "type": "sqlite",  # Switch test to SQLite
        "path": "/tmp/financetest.db"
    },
}
sharpe = 0.017
