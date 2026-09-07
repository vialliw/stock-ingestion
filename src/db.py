"""DuckDB storage layer for the stock-ingestion pipeline.

Implements ``DatabaseManager`` per spec §4 (schema) and §10 (reference
implementation): thread-safe writes via a single write connection guarded by
``threading.Lock``, and concurrent reads via short-lived read-only
connections.

Spec reference: docs/tech_spec/gemini_tech_spec.1.0.2.md §4, §9, §10.
"""

from __future__ import annotations

import json
import logging
import threading
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

import duckdb
import pandas as pd
import pyarrow as pa

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 3


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
            # Ensure core tables exist before querying/migrating schema version.
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS schema_version (
                    version INTEGER NOT NULL,
                    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
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
                CREATE TABLE IF NOT EXISTS ingestion_runs (
                    run_id BIGINT PRIMARY KEY,
                    run_type VARCHAR NOT NULL,
                    started_at TIMESTAMP,
                    completed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    status VARCHAR,
                    rows_upserted INTEGER DEFAULT 0,
                    tickers_succeeded INTEGER DEFAULT 0,
                    tickers_failed INTEGER DEFAULT 0,
                    errors_json VARCHAR,
                    duration_ms INTEGER
                );
            """)

            # Determine current schema version (0 = fresh database).
            try:
                row = self.conn.execute(
                    "SELECT MAX(version) FROM schema_version"
                ).fetchone()
                current_version = row[0] if row and row[0] is not None else 0
            except duckdb.Error:
                current_version = 0

            # --- v1 -> v2: rename the legacy daily_quotes table to daily_close ---
            if current_version < 2:
                if current_version == 1:
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
                    "INSERT INTO schema_version (version) VALUES (?)", [2]
                )
                current_version = 2

            # --- v2 -> v3: ingestion_runs audit table (already created above) ---
            if current_version < 3:
                self.conn.execute(
                    "INSERT INTO schema_version (version) VALUES (?)", [3]
                )

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
    # Ingestion run audit
    # ------------------------------------------------------------------

    def record_ingestion_run(
        self,
        run_type: str,
        result: Dict[str, Any],
        started_at: Optional[datetime] = None,
    ) -> int:
        """Persist a single audit row summarising an ingestion run.

        ``result`` is the dict returned by one of the ``run_*`` entry
        points.  A single SQL INSERT is used (no Python row loops).
        Returns the generated ``run_id``.
        """
        if result is None:
            result = {}
        rows = int(result.get("rows_upserted", 0) or 0)
        succeeded = len(result.get("tickers_succeeded", []) or [])
        failed = len(result.get("tickers_failed", []) or [])
        status = result.get("status", "UNKNOWN")
        duration_ms = int(result.get("duration_ms", 0) or 0)
        errors = result.get("errors", {})
        if not isinstance(errors, dict):
            errors = {}
        errors_json = json.dumps(errors)
        completed_at = datetime.now()

        with self._write_lock:
            max_id_row = self.conn.execute(
                "SELECT COALESCE(MAX(run_id), 0) FROM ingestion_runs"
            ).fetchone()
            run_id = int(max_id_row[0]) + 1
            self.conn.execute(
                """
                INSERT INTO ingestion_runs
                (run_id, run_type, started_at, completed_at, status,
                 rows_upserted, tickers_succeeded, tickers_failed,
                 errors_json, duration_ms)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    run_id,
                    run_type,
                    started_at,
                    completed_at,
                    status,
                    rows,
                    succeeded,
                    failed,
                    errors_json,
                    duration_ms,
                ],
            )
        return run_id

    def get_ingestion_runs(self) -> pa.Table:
        """Return the full ingestion_runs audit table as an Arrow table."""
        return self.execute_query(
            "SELECT * FROM ingestion_runs ORDER BY completed_at DESC"
        )

    def get_health_info(self) -> Dict[str, Any]:
        """Return a snapshot of database health and row counts.

        All aggregates are computed in a single SQL query
        (SQL > vectorized > loop hierarchy).  If the database is
        unreachable, ``db_connected`` is False and the remaining fields
        fall back to safe defaults.
        """
        defaults: Dict[str, Any] = {
            "db_connected": False,
            "daily_close_count": 0,
            "splits_processed_count": 0,
            "ingestion_runs_count": 0,
            "ticker_count": 0,
            "last_run": None,
            "schema_version": SCHEMA_VERSION,
        }
        try:
            conn = self._read_only_conn()
            try:
                row = conn.execute(
                    """
                    SELECT
                        (SELECT COUNT(*) FROM daily_close) AS daily_close_count,
                        (SELECT COUNT(*) FROM splits_processed) AS splits_processed_count,
                        (SELECT COUNT(*) FROM ingestion_runs) AS ingestion_runs_count,
                        (SELECT COUNT(DISTINCT Ticker) FROM daily_close) AS ticker_count,
                        (SELECT MAX(version) FROM schema_version) AS schema_version,
                        (SELECT MAX(completed_at) FROM ingestion_runs) AS last_run
                    """
                ).fetchone()
            finally:
                conn.close()
        except Exception as e:
            logger.error("Health check query failed: %s", e)
            return defaults

        if row is None:
            return defaults

        return {
            "db_connected": True,
            "daily_close_count": row[0] or 0,
            "splits_processed_count": row[1] or 0,
            "ingestion_runs_count": row[2] or 0,
            "ticker_count": row[3] or 0,
            "last_run": str(row[4]) if row[4] is not None else None,
            "schema_version": row[5] if row[5] is not None else SCHEMA_VERSION,
        }

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
