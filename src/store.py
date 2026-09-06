"""Local DuckDB persistence for raw stock CSV blobs."""

import os
import duckdb


def store_csv(symbol: str, date: str, csv_bytes: bytes) -> int:
    """Persist a raw CSV blob and return the generated row id.

    Uses a local DuckDB file whose path is configurable via the
    ``STORE_DB_PATH`` environment variable (default: ``./stock.duckdb``).
    Rows are always appended; duplicate (symbol, date) pairs produce
    distinct ids.
    """
    db_path = os.environ.get("STORE_DB_PATH", "./stock.duckdb")
    conn = duckdb.connect(db_path)
    try:
        conn.execute("CREATE SEQUENCE IF NOT EXISTS raw_csv_id_seq START 1")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS raw_csv (
                id BIGINT PRIMARY KEY DEFAULT nextval('raw_csv_id_seq'),
                symbol VARCHAR,
                date VARCHAR,
                csv_bytes BLOB,
                created_at TIMESTAMP
            )
            """
        )
        result = conn.execute(
            """
            INSERT INTO raw_csv (symbol, date, csv_bytes, created_at)
            VALUES (?, ?, ?, current_timestamp)
            RETURNING id
            """,
            (symbol, date, csv_bytes),
        ).fetchone()
        conn.commit()
        return int(result[0])
    finally:
        conn.close()
