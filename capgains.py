from __future__ import absolute_import

import argparse
import sqlite3

from decimal import *

parser = argparse.ArgumentParser(description="Print capital gains summary")
parser.add_argument('database',
                    help="Path to database")

QTY_FACTOR = 100000
                    
def iter_results(cur):
    while True:
        batch = cur.fetchmany()
        if not batch:
            break
        yield from batch


def find_year_dividends(cur, year):
    cur.execute("""
        select date, amount
        from movements
        where type = 'Dividends'
        and strftime('%Y', date) = ?
        order by date asc
    """, (str(year),))
    return iter_results(cur)

    
def find_year_sales(cur, year):
    cur.execute("""
        select
            sales.id as sale_id,
            sales.date as sale_date,
            sales.isin as sale_isin,
            sales.quantity as sale_quantity,
            sales.price as sale_price,
            sales.total as sale_total,
            reconciliation.quantity as reconciliation_quantity,
            purchases.id as purchase_id,
            purchases.date as purchase_date,
            purchases.isin as purchase_isin, 
            purchases.quantity as purchase_quantity,
            purchases.price as purchase_price,
            purchases.total as purchase_total
        from trades as sales
        inner join reconciliation on sales.id = reconciliation.sale_id
        inner join trades as purchases on reconciliation.purchase_id = purchases.id
        where sales.type = 'Sell'
        and strftime('%Y', sales.date) = ?
        order by sales.date asc
    """, (str(year),))
    return iter_results(cur)

def qty(quantity):
    return Decimal(quantity) / QTY_FACTOR

def capgains_summary(conn, year):
    cur = conn.cursor()

    year_profit = Decimal(0)
    purchase_total = None
    current_sale = None
    current_sale_total = None

    dividends = list(find_year_dividends(cur, year))
    sales = list(find_year_sales(cur, year))

    if not sales and not dividends:
        return

    print(f"YEAR {year}")
    print("#########")

    for d in dividends:
        print(f"Dividend ({d['date']}): {d['amount']}")
        year_profit += Decimal(d['amount'])
   

    if not sales:
        return

    for e in sales:
        if e['sale_id'] != current_sale:
            if current_sale is not None:
                sale_profit = current_sale_total - purchase_total
                print(f"    Purchase total: {purchase_total}")
                print(f"    Net profit: {current_sale_total} - {purchase_total} = {sale_profit}")
                year_profit += sale_profit

            purchase_total = Decimal(0)
            current_sale = e['sale_id']
            current_sale_total = -Decimal(e['sale_total'])

            print(f"Sell ({e['sale_date']}): {qty(e['sale_quantity'])} units of {e['sale_isin']} @ {e['sale_price']}, total {e['sale_total']}")

        reconciled_proportion = round(Decimal(e['reconciliation_quantity'])/Decimal(e['purchase_quantity']), 2)
        reconciled_total = reconciled_proportion * Decimal(e['purchase_total'])
        purchase_total += reconciled_total
        print(f"    Buy ({e['purchase_date']}): {qty(e['reconciliation_quantity'])}/{qty(e['purchase_quantity'])} units @ {e['purchase_price']}, total {e['purchase_total']} (reconciled total {reconciled_total})")
    
    sale_profit = current_sale_total - purchase_total
    print(f"    Purchase total: {purchase_total}")
    print(f"    Net profit on sale: {current_sale_total} - {purchase_total} = {sale_profit}")
    year_profit += sale_profit

    print()
    print(f"GRAND TOTAL PROFIT ({year}) = {year_profit}")
    print()
    print()

def main():
    args = parser.parse_args()

    conn = sqlite3.connect(args.database)
    conn.row_factory = sqlite3.Row

    cur = conn.cursor()
    cur.execute('select min(strftime("%Y", date)) as min, max(strftime("%Y", date)) as max from trades')
    start, end = cur.fetchone()

    getcontext().traps[FloatOperation] = True


    
    for year in range(int(start), int(end)+1):
        capgains_summary(conn, year)

if __name__ == "__main__":
    main()