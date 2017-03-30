CREATE TYPE taxCategory AS ENUM ('deferred', 'free', 'taxable', 'restricted');
CREATE TYPE transactionType AS ENUM ('deposit', 'buy', 'dividend', 'sale', 'withdrawal');

CREATE TABLE IF NOT EXISTS accountTypes (
    name text unique not null primary key,
    tax taxCategory not null,
    margin boolean not null default false
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
    domesticCurrency boolean not null  -- domestic equity, bonds, and cash, as well as foreign currency-hedged
);

CREATE TABLE IF NOT EXISTS assets (
    ticker text unique not null primary key,
    class text not null references assetClasses(name)
);

CREATE TABLE IF NOT EXISTS marketDays (
    day date not null primary key,
    open boolean not null default true
);

CREATE TABLE IF NOT EXISTS transactions (
    day date not null default current_date references marketDays(day),
    txType transactionType not null,
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

CREATE TABLE IF NOT EXISTS inflationRates (
    month date not null primary key,
    rate numeric(9, 2)
);
