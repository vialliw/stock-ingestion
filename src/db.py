"""DuckDB storage layer for the stock-ingestion pipeline.

Implements ``DatabaseManager`` per spec §4 (schema) and §10 (reference
implementation): thread-safe writes via a single write connection guarded by
``threading.Lock``, and concurrent reads via short-lived read-only
connections.

Spec reference: docs/tech_spec/gemini_tech_spec.1.0.2.md §4, §9, §10.
"""

from __future__ import annotations

import logging
import threading
from typing import Any, Dict, List, Optional, Set, Tuple

import duckdb
import pandas as pd
import pyarrow as pa

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 2


class DatabaseManager:
    """Thread-safe DuckDB manager: single write conn + read-only read conns."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._write_lock = threading.Lock()
        self.conn = duckdb.connect(self.db_path, read_only=False)
        self._init_schema()

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------

    def _init_schema(self):
        with self._write_lock:
            # --- Schema migration: v1 -> v2 (rename daily_quotes -> daily_close) ---
            try:
                row = self.conn.execute(
                    "SELECT MAX(version) FROM schema_version"
                ).fetchone()
                current_version = row[0] if row and row[0] is not None else 0
            except duckdb.Error:
                current_version = 0
            if current_version == 1:
                # Check if the old daily_quotes table still exists
                tables = {
                    r[0]
                    for r in self.conn.execute(
                        "SELECT table_name FROM information_schema.tables"
                    ).fetchall()
                }
                if "daily_quotes" in tables:
                    self.conn.execute(
                        "ALTER TABLE daily_quotes RENAME TO daily_close"
                    )
                self.conn.execute(
                    "INSERT INTO schema_version (version) VALUES (?)",
                    [SCHEMA_VERSION],
                )
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS daily_close (
                    Date DATE NOT NULL,
                    Ticker VARCHAR NOT NULL,
                    Open DOUBLE,
                    High DOUBLE,
                    Low DOUBLE,
                    Close DOUBLE,
                    Volume DOUBLE,
                    PRIMARY KEY (Date, Ticker)
                );
            """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS splits_processed (
                    Ticker VARCHAR NOT NULL,
                    SplitDate DATE NOT NULL,
                    SplitFactor DOUBLE NOT NULL,
                    ProcessedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (Ticker, SplitDate)
                );
            """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS schema_version (
                    version INTEGER NOT NULL,
                    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            self.conn.execute("""
                INSERT INTO schema_version (version)
                SELECT ?
                WHERE NOT EXISTS (
                    SELECT 1 FROM schema_version WHERE version = ?
                );
            """, [SCHEMA_VERSION, SCHEMA_VERSION])

    # ------------------------------------------------------------------
    # Read path (short-lived read-only connections)
    # ------------------------------------------------------------------

    def _read_only_conn(self):
        """Open a short-lived read-only connection for concurrent read queries.

        Spec §10 calls ``duckdb.connect(db_path, read_only=True)``. However
        DuckDB refuses to open a read-only connection to a file that already
        has a read-write connection in the same process ("different
        configuration than existing connections"). In that case we fall back
        to a cursor on the write connection wrapped in a ``READ ONLY``
        transaction, which enforces the same guarantee: any write statement
        raises TransactionException while snapshot reads proceed under MVCC.
        """
        try:
            return duckdb.connect(self.db_path, read_only=True)
        except duckdb.ConnectionException:
            cur = self.conn.cursor()
            cur.execute("BEGIN TRANSACTION READ ONLY")
            return cur

    def execute_query(self, query: str) -> pa.Table:
        conn = self._read_only_conn()
        try:
            return conn.execute(query).fetch_arrow_table()
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Write path (guarded by the write lock)
    # ------------------------------------------------------------------

    def upsert_dataframe(self, df: pd.DataFrame) -> int:
        """Upsert dataframe and return number of rows affected."""
        if df.empty:
            return 0
        # pandas 3.x uses a native 'str' dtype that duckdb 1.4.x cannot scan
        # ("Data type 'str' not recognized"); normalize to object dtype, and
        # coerce Date to datetime64 so MERGE compares DATE to DATE.
        work = df.copy()
        # Vectorized dtype fix: select str-dtype columns and cast all at once
        str_cols = [c for c in work.columns if str(work[c].dtype) in ("str", "string")]
        if str_cols:
            work[str_cols] = work[str_cols].astype(object)
        if not pd.api.types.is_datetime64_any_dtype(work["Date"]):
            work["Date"] = pd.to_datetime(work["Date"])
        with self._write_lock:
            self.conn.register("staging_df", work)
            self.conn.execute("""
                MERGE INTO daily_close AS target
                USING staging_df AS source
                ON target.Date = source.Date AND target.Ticker = source.Ticker
                WHEN MATCHED THEN UPDATE SET
                    Open = source.Open,
                    High = source.High,
                    Low = source.Low,
                    Close = source.Close,
                    Volume = source.Volume
                WHEN NOT MATCHED THEN INSERT
                    (Date, Ticker, Open, High, Low, Close, Volume)
                VALUES
                    (source.Date, source.Ticker, source.Open, source.High,
                     source.Low, source.Close, source.Volume);
            """)
            self.conn.unregister("staging_df")
            return len(df)

    def purge_ticker(self, ticker: str):
        with self._write_lock:
            self.conn.execute(
                "DELETE FROM daily_close WHERE Ticker = ?", [ticker]
            )
        logger.critical(
            "RUNTIME EVENT: Delisted ticker data completely purged for '%s'.",
            ticker,
        )

    def purge_tickers(self, tickers: List[str]) -> int:
        """Bulk-purge multiple tickers in a single SQL DELETE.

        Returns the number of rows deleted.
        """
        if not tickers:
            return 0
        placeholders = ", ".join("?" for _ in tickers)
        with self._write_lock:
            result = self.conn.execute(
                f"DELETE FROM daily_close WHERE Ticker IN ({placeholders})",
                tickers,
            )
            count = result.fetchone()[0] if result else 0
        logger.critical(
            "RUNTIME EVENT: Bulk-purged %s row(s) for tickers %s.",
            count,
            tickers,
        )
        return count

    # ------------------------------------------------------------------
    # Split tracking
    # ------------------------------------------------------------------

    def is_split_processed(self, ticker: str, split_date: str) -> bool:
        conn = self._read_only_conn()
        try:
            result = conn.execute(
                "SELECT 1 FROM splits_processed WHERE Ticker = ? AND SplitDate = ?",
                [ticker, split_date],
            ).fetchone()
            return result is not None
        finally:
            conn.close()

    def record_split(self, ticker: str, split_date: str, split_factor: float):
        """Record a split in splits_processed for idempotency."""
        with self._write_lock:
            self.conn.execute(
                """
                INSERT INTO splits_processed (Ticker, SplitDate, SplitFactor)
                VALUES (?, ?, ?)
                ON CONFLICT (Ticker, SplitDate) DO NOTHING;
                """,
                [ticker, split_date, split_factor],
            )

    def delete_ticker_data(self, ticker: str):
        """Delete all rows for a ticker (used before re-download after split)."""
        with self._write_lock:
            self.conn.execute(
                "DELETE FROM daily_close WHERE Ticker = ?", [ticker]
            )

    # ------------------------------------------------------------------
    # Ticker universe helpers
    # ------------------------------------------------------------------

    def get_existing_tickers(self) -> Set[str]:
        conn = self._read_only_conn()
        try:
            result = conn.execute(
                "SELECT DISTINCT Ticker FROM daily_close"
            ).fetchall()
            return {row[0] for row in result}
        finally:
            conn.close()

    def get_aligned_start_date(self) -> Optional[str]:
        """Return the earliest Date across all tickers as ISO string, or None."""
        conn = self._read_only_conn()
        try:
            result = conn.execute("SELECT MIN(Date) FROM daily_close").fetchone()
            if result and result[0]:
                return str(result[0])
            return None
        finally:
            conn.close()

    def detect_new_tickers(self, configured_tickers: List[str]) -> List[str]:
        existing = self.get_existing_tickers()
        return [t for t in configured_tickers if t not in existing]

    # ------------------------------------------------------------------
    # Reports (§9)
    # ------------------------------------------------------------------

    def generate_top_20_dollar_volume(
        self, target_date: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        conn = self._read_only_conn()
        try:
            if target_date:
                rel = conn.execute(
                    """SELECT Ticker, CAST(Date AS VARCHAR) AS Date,
                              Close, Volume,
                              (Close * Volume) AS dollar_volume
                       FROM daily_close
                       WHERE Date = ?
                       ORDER BY dollar_volume DESC
                       LIMIT 20;""",
                    [target_date],
                )
            else:
                rel = conn.execute(
                    """SELECT Ticker, CAST(Date AS VARCHAR) AS Date,
                              Close, Volume,
                              (Close * Volume) AS dollar_volume
                       FROM daily_close
                       WHERE Date = (SELECT MAX(Date) FROM daily_close)
                       ORDER BY dollar_volume DESC
                       LIMIT 20;"""
                )
            cols = [desc[0] for desc in rel.description]
            rows = rel.fetchall()
            # Date already comes back as a string via SQL CAST — no
            # Python stringify loop needed (SQL > loop hierarchy).
            result = [dict(zip(cols, row)) for row in rows]
            return result
        finally:
            conn.close()

    def batch_record_splits(
        self, splits: List[Tuple[str, str, float]]
    ) -> int:
        """Record multiple splits in a single SQL INSERT (idempotent).

        ``splits`` is a list of ``(ticker, split_date, split_factor)``
        tuples.  Uses ``ON CONFLICT (Ticker, SplitDate) DO NOTHING`` so
        re-recording existing splits is a no-op.  Returns the number of
        rows inserted.
        """
        if not splits:
            return 0
        placeholders = ", ".join("(?, ?, ?)" for _ in splits)
        params = [val for tup in splits for val in tup]
        with self._write_lock:
            self.conn.execute(
                f"INSERT INTO splits_processed "
                f"(Ticker, SplitDate, SplitFactor) "
                f"VALUES {placeholders} "
                f"ON CONFLICT (Ticker, SplitDate) DO NOTHING;",
                params,
            )
        return len(splits)

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------

    def close(self):
        with self._write_lock:
            logger.info("Executing DuckDB CHECKPOINT before closing handle...")
            try:
                self.conn.execute("CHECKPOINT;")
                self.conn.close()
                logger.info("DuckDB storage gracefully detached.")
            except Exception as e:
                logger.error("Error executing CHECKPOINT on close: %s", e)


__all__ = ["DatabaseManager", "SCHEMA_VERSION"]
