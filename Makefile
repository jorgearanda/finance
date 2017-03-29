.PHONY: clean test all

all:
	pip install -r requirements.txt

freshtest: clean recreatedb test

test:
	rm -rf cover
	rm -f .coverage
	pytest --cov-report term-missing --cov

recreatedb:
	psql -h localhost finance -c "drop database financetest;"
	psql -h localhost finance -c "create database financetest;"
	psql -h localhost financetest < ./data/schemas.sql

clean:
	rm -rf __pycache__
