# ETFmatic capital gains calculator

This repository contains a few scripts to help calculate capital gains for an 
ETFmatic investment account.

It assumes you have already downloaded a full list of trades and cash movements
from ETFmatic, in CSV format.

- `import.py`: processes the CSV files and imports data into a SQLite database
- `reconcile.py`: reconciles sale transactions against purchase transactions on a FIFO cost basis
- `capgains.py`: generates textual reports on capital gains for a given calendar year

**N.B.** There is at least one significant bug in this code at the moment. Use it
with caution.

NO WARRANTY OF ANY KIND IS OFFERED BY THE ORIGINAL AUTHOR OF THIS CODE