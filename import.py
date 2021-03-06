from __future__ import absolute_import

import argparse
import csv
import decimal
import sqlite3
from collections import namedtuple

parser = argparse.ArgumentParser(description="Import ETFmatic data into a database")
parser.add_argument('trades', 
                    type=argparse.FileType('r', encoding='UTF-8-SIG'),
                    help="Path to CSV file with trade history")
parser.add_argument('movements', 
                    type=argparse.FileType('r', encoding='UTF-8-SIG'),
                    help="Path to CSV file with cash movement history")
parser.add_argument('database',
                    help="Path to database")

Movement = namedtuple('Movement', ['date', 'type', 'amount'])
Trade = namedtuple('Trade', ['date', 'name', 'isin', 'symbol', 'exchange', 'type', 'price', 'quantity', 'total'])

QTY_FACTOR = 100000

SCHEMA = """
    create table if not exists trades (
        id integer primary key asc,
        date text not null,
        name text,
        isin text,
        symbol text,
        exchange text,
        type text check(type in ('Buy', 'Sell')) not null,
        price text not null,
        quantity integer not null,
        total text not null
    );

    create table if not exists movements (
        id integer primary key asc,
        date text not null,
        type text check(type in ('Dividends', 'Invoice', 'Funding', 'Divestment')) not null,
        amount text not null
    );

    create table if not exists reconciliation (
        id integer primary key asc,
        purchase_id integer not null,
        sale_id integer not null,
        quantity integer not null,
        foreign key(purchase_id) references trades(id),
        foreign key(sale_id) references trades(id)
    );
"""

def get_trades(tradesfp):
    reader = csv.reader(tradesfp)
    # skip header row
    next(reader)

    for line in reader:
        date, name, isin, symbol, exchange, type_, price, quantity, total = line
        t = Trade(date, name, isin, symbol, exchange, type_, price, int(decimal.Decimal(quantity) * QTY_FACTOR), total)
        yield t

def get_movements(movementsfp):
    reader = csv.reader(movementsfp)
    # skip header row
    next(reader)

    for line in reader:
        m = Movement(*line)
        yield m

def main():
    args = parser.parse_args()

    conn = sqlite3.connect(args.database)
    cur = conn.cursor()

    cur.executescript(SCHEMA)
    cur.executemany(
        'insert into trades(date, name, isin, symbol, exchange, type, price, quantity, total) values (?, ?, ?, ?, ?, ?, ?, ?, ?)', 
        get_trades(args.trades))
    cur.executemany(
        'insert into movements(date, type, amount) values (?, ?, ?)', 
        get_movements(args.movements))

    conn.commit()
    conn.close()

if __name__ == "__main__":
    main()