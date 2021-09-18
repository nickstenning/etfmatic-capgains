from __future__ import absolute_import

import argparse
import csv
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


SCHEMA = """
    create table if not exists trades (
        id integer primary key asc,
        date text not null,
        name text,
        isin text,
        symbol text,
        exchange text,
        type text check(type in ('Buy', 'Sell')) not null,
        price decimal(11, 4) not null,
        quantity decimal(12, 5) not null,
        total decimal(9, 2) not null
    );

    create table if not exists movements (
        id integer primary key asc,
        date text not null,
        type text check(type in ('Dividends', 'Invoice', 'Funding', 'Divestment')) not null,
        amount decimal(9, 2) not null
    );
"""

def get_trades(tradesfp):
    reader = csv.reader(tradesfp)
    # skip header row
    next(reader)

    for line in reader:
        t = Trade(*line)
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