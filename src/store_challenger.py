"""Challenger variant of `store_csv` for the stock-ingestion duel.

This implementation deliberately differs structurally from `src/store.py`:
* It uses an explicit DuckDB sequence (`raw_csv_id_seq`) to generate
  monotonically increasing integer ids.
* It manages connections via `contextlib.closing` so each call opens, uses,
  and closes its own connection.
* It uses positional SQL parameter binding.
"""

import os
from contextlib import closing

import duckdb

_SEQUENCE_NAME = "raw_csv_id_seq"
_TABLE_NAME = "raw_csv"

# Build the sequence and table once per connection.  Both statements use
# IF NOT EXISTS so repeated calls are safe and no Python loops are needed.
_DDL = f"""
CREATE SEQUENCE IF NOT EXISTS {_SEQUENCE_NAME} START 1 INCREMENT 1;

CREATE TABLE IF NOT EXISTS {_TABLE_NAME} (
    id BIGINT PRIMARY KEY DEFAULT nextval('{_SEQUENCE_NAME}'),
    symbol VARCHAR,
    date VARCHAR,
    csv_bytes BLOB,
    created_at TIMESTAMP DEFAULT current_timestamp
);
"""

_INSERT_SQL = f"""
INSERT INTO {_TABLE_NAME} (symbol, date, csv_bytes)
VALUES (?, ?, ?)
RETURNING id
"""


def store_csv(symbol: str, date: str, csv_bytes: bytes) -> int:
    """Persist a raw CSV blob for (symbol, date) and return the generated id.

    Two calls with the same (symbol, date) append two distinct rows; the
    returned ids are strictly increasing.  The database path is read from
    the ``STORE_DB_PATH_CHALLENGER`` environment variable, defaulting to
    ``./stock_challenger.duckdb``.
    """
    raw_path = os.getenv("STORE_DB_PATH_CHALLENGER", "./stock_challenger.duckdb")
    db_path = os.path.expanduser(raw_path)

    with closing(duckdb.connect(db_path)) as conn:
        conn.execute(_DDL)
        row = conn.execute(_INSERT_SQL, (symbol, date, csv_bytes)).fetchone()

    return int(row[0])
