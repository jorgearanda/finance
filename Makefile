.PHONY: clean test all

all:
	pip install -r requirements.txt

freshtest: clean recreatedb test

test:
	rm -rf cover
	rm -f .coverage
	pytest --cov-report term-missing --cov

recreatedb:
	rm -f /tmp/financetest.db
	sqlite3 /tmp/financetest.db < db/schemas.sql

clean:
	rm -rf __pycache__
