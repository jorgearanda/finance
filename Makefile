.PHONY: clean test all

all:
	pip install -r requirements.txt

freshtest: clean recreatedb test

test:
	rm -rf cover
	rm -f .coverage
	pytest --cov-report term-missing --cov

recreatedb:
	# PostgreSQL (keep for now)
	psql -h localhost finance -c "drop database financetest;"
	psql -h localhost finance -c "create database financetest;"
	psql -h localhost financetest < ./db/schemas.sql
	# SQLite (new)
	rm -f /tmp/financetest.db
	sqlite3 /tmp/financetest.db < db/schemas_sqlite.sql

clean:
	rm -rf __pycache__
