#!/usr/bin/env python3
"""
Dividend CSV to SQL converter.

Reads a dividend CSV file from a brokerage and generates SQL INSERT statements
for the transactions table, grouped by account.
"""

import argparse
import csv
import re
from collections import defaultdict
from datetime import datetime


def load_account_mapping(account_codes_file):
    """Load account code to account name mapping from CSV file."""
    mapping = {}
    with open(account_codes_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            code = row['account_code'].strip()
            name = row['account_name'].strip()
            if code and name:  # Skip empty rows
                mapping[code] = name
    return mapping


def extract_share_count(description):
    """Extract number of shares from description string.

    Example: "DIST ON 52 SHS REC 12/30/25" -> 52
    """
    match = re.search(r'DIST ON (\d+) SHS', description)
    if match:
        return int(match.group(1))
    raise ValueError(f"Could not extract share count from: {description}")


def parse_dividend_csv(dividend_file, account_mapping):
    """Parse dividend CSV and return transactions grouped by account."""
    transactions = defaultdict(list)

    with open(dividend_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Extract and convert data
            settlement_date = datetime.strptime(
                row['Settlement Date'],
                '%Y-%m-%d %I:%M:%S %p'
            ).strftime('%Y-%m-%d')

            symbol = row['Symbol'].strip() + '.TO'
            account_code = row['Account #'].strip()

            # Get account name from mapping
            if account_code not in account_mapping:
                raise ValueError(f"Unknown account code: {account_code}")
            account_name = account_mapping[account_code]

            # Extract share count from description
            shares = extract_share_count(row['Description'])

            # Get price and net amount
            price = float(row['Price'])
            net_amount = float(row['Net Amount'])

            # Create transaction tuple
            transaction = {
                'txType': 'dividend',
                'account': account_name,
                'source': symbol,
                'target': 'Cash',
                'day': settlement_date,
                'units': shares,
                'unitPrice': price,
                'total': net_amount
            }

            transactions[account_name].append(transaction)

    return transactions


def format_sql_value(value):
    """Format a value for SQL insertion."""
    if isinstance(value, str):
        return f"'{value}'"
    elif isinstance(value, (int, float)):
        return str(value)
    else:
        return str(value)


def generate_sql(transactions):
    """Generate SQL INSERT statements grouped by account."""
    sql_statements = []

    for account_name in sorted(transactions.keys()):
        txs = transactions[account_name]

        # Start INSERT statement
        sql = "INSERT INTO transactions (txType, account, source, target, day, units, unitPrice, total)\n"
        sql += "VALUES\n"

        # Add each transaction
        values = []
        for tx in txs:
            value_str = f"    ({format_sql_value(tx['txType'])}, " \
                       f"{format_sql_value(tx['account'])}, " \
                       f"{format_sql_value(tx['source'])}, " \
                       f"{format_sql_value(tx['target'])}, " \
                       f"{format_sql_value(tx['day'])}, " \
                       f"{format_sql_value(tx['units'])}, " \
                       f"{format_sql_value(tx['unitPrice'])}, " \
                       f"{format_sql_value(tx['total'])})"
            values.append(value_str)

        sql += ",\n".join(values) + ";\n"
        sql_statements.append(sql)

    return "\n".join(sql_statements)


def main():
    parser = argparse.ArgumentParser(
        description='Convert dividend CSV to SQL INSERT statements'
    )
    parser.add_argument('input_file', help='Input dividend CSV file')
    parser.add_argument('output_file', help='Output SQL file')
    parser.add_argument(
        '--account-codes',
        default='data/account_codes.csv',
        help='Account codes mapping file (default: data/account_codes.csv)'
    )

    args = parser.parse_args()

    # Load account mapping
    print(f"Loading account mapping from {args.account_codes}...")
    account_mapping = load_account_mapping(args.account_codes)
    print(f"Loaded {len(account_mapping)} account mappings")

    # Parse dividend CSV
    print(f"Parsing dividend data from {args.input_file}...")
    transactions = parse_dividend_csv(args.input_file, account_mapping)

    total_txs = sum(len(txs) for txs in transactions.values())
    print(f"Parsed {total_txs} transactions across {len(transactions)} accounts")

    # Generate SQL
    print(f"Generating SQL...")
    sql = generate_sql(transactions)

    # Write to output file
    with open(args.output_file, 'w') as f:
        f.write(sql)

    print(f"SQL statements written to {args.output_file}")
    print("\nSummary:")
    for account_name in sorted(transactions.keys()):
        print(f"  {account_name}: {len(transactions[account_name])} transactions")


if __name__ == '__main__':
    main()
