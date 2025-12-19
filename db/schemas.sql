CREATE TABLE IF NOT EXISTS accountTypes (
    name text unique not null primary key,
    tax TEXT NOT NULL CHECK(tax IN ('deferred', 'free', 'taxable', 'restricted')),
    margin INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS investors(
    name text unique not null primary key
);

CREATE TABLE IF NOT EXISTS accounts (
    name text unique not null primary key,
    accountType text not null references accountTypes(name),
    investor text not null references investors(name),
    dateCreated date not null
);

CREATE TABLE IF NOT EXISTS assetClasses (
    name text unique not null primary key,
    domesticCurrency INTEGER NOT NULL  -- domestic equity, bonds, and cash, as well as foreign currency-hedged
);

CREATE TABLE IF NOT EXISTS assets (
    ticker text unique not null primary key,
    class text not null references assetClasses(name)
);

CREATE TABLE IF NOT EXISTS marketDays (
    day date not null primary key,
    open INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS transactions (
    day date not null default current_date references marketDays(day),
    txType TEXT NOT NULL CHECK(txType IN ('deposit', 'buy', 'dividend', 'sale', 'withdrawal')),
    account text not null references accounts(name),
    source text references assets(ticker),
    target text references assets(ticker),
    units numeric(9, 2),
    unitPrice numeric(9, 2),
    commission numeric(9, 2),
    total numeric(12, 2) not null
);

CREATE TABLE IF NOT EXISTS assetPrices (
    ticker text not null references assets(ticker),
    day date not null default current_date references marketDays(day),
    close numeric(9, 2),
    ask numeric(9, 2),
    bid numeric(9, 2)
);

CREATE UNIQUE INDEX IF NOT EXISTS ticker_day ON assetPrices (
    ticker ASC,
    day ASC
);

CREATE TABLE IF NOT EXISTS distributions (
    ticker text not null references assets(ticker),
    day date not null default current_date references marketDays(day),
    type TEXT NOT NULL DEFAULT 'income' CHECK(type IN ('income', 'capital gains')),
    amount numeric(9, 6)
);

CREATE UNIQUE INDEX IF NOT EXISTS ticker_day_distrib ON distributions (
    ticker ASC,
    day ASC
);

CREATE TABLE IF NOT EXISTS inflationRates (
    month date not null primary key,
    rate numeric(9, 2)
);
