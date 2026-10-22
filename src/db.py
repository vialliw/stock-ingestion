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
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple

import duckdb
import pandas as pd
import pandas_market_calendars as mcal
import pyarrow as pa

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 9

# F16-T1: CNN Fear & Greed sub-indicator payload keys — each becomes a
# ``<key> DOUBLE`` + ``<key>_rating VARCHAR`` column pair on
# ``fear_greed_history``.  These are the LIVE API key names (verified
# 2026-09-20); the reference doc docs/enhancements/2260920.md uses stale
# names (stock_price_momentum / market_volatility) that do not exist.
FEAR_GREED_SUB_INDICATORS: Tuple[str, ...] = (
    "stock_price_strength",
    "stock_price_breadth",
    "market_momentum_sp500",
    "market_momentum_sp125",
    "market_volatility_vix",
    "market_volatility_vix_50",
    "put_call_options",
    "junk_bond_demand",
    "safe_haven_demand",
)

# Static column list for the fear_greed_history upsert — every column
# except the ``ingested_at`` DEFAULT.  The engine materializes all of
# them on the staging frame so this list is always complete.
FEAR_GREED_UPSERT_COLUMNS: Tuple[str, ...] = (
    ("date", "fg_score", "fg_rating")
    + tuple(
        col for key in FEAR_GREED_SUB_INDICATORS for col in (key, f"{key}_rating")
    )
)

# F17-T1: valid fg_rating vocabulary (CNN lowercase labels).  NULL is
# allowed — this list is only used to flag non-NULL outliers.
FEAR_GREED_VALID_RATINGS: Tuple[str, ...] = (
    "extreme fear",
    "fear",
    "neutral",
    "greed",
    "extreme greed",
)


class DatabaseManager:
    """Thread-safe DuckDB manager: single write conn + read-only read conns."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._write_lock = threading.Lock()
        self.conn = duckdb.connect(self.db_path, read_only=False)
        # F26-T4: kill the extension auto-install/auto-load class
        # server-wide.  Calling an uninstalled-but-known function
        # (``stem`` via fts, ``sql_auto_complete`` via autocomplete,
        # ``host`` via inet, ``dbgen`` via tpch, ...) otherwise triggers
        # a network ``INSTALL`` + native-code ``LOAD`` at *bind* time —
        # egress + remote-code load reachable from client SQL.  Both
        # settings are GLOBAL scope on the database instance, so every
        # cursor/read-connection spawned from ``self.conn`` inherits
        # them (verified live on duckdb 1.5.5: the calls above all
        # raise CatalogException, ``SELECT 42`` is unaffected, and
        # unlike ``enable_external_access`` this is NOT a one-way
        # switch).  This also closes the identical pre-existing hole in
        # ``do_get``, which reads through the same DB instance.
        #
        # Caveat: already-installed extensions also stop auto-loading
        # by name — so every server code path was audited for extension
        # functions before flipping this: ``r2_sync`` uses boto3 (not
        # duckdb httpfs), Excel ingest uses pandas/openpyxl (not the
        # excel ext), weekly rebuilds and the signal/fear_greed
        # pipelines use pure built-in SQL + requests.  Nothing needs an
        # extension, so no explicit ``LOAD`` is issued here.  If a
        # future path ever does, ``LOAD <ext>`` immediately after these
        # SETs (only bundled/disk-local names — never client input).
        self.conn.execute("SET autoinstall_known_extensions=false")
        self.conn.execute("SET autoload_known_extensions=false")
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
                    RawClose DOUBLE,
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
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS company_metadata (
                    ticker VARCHAR PRIMARY KEY,
                    company_name VARCHAR,
                    sector VARCHAR,
                    industry VARCHAR,
                    quote_type VARCHAR
                );
            """)
            # F12-T1: weekly layer — Friday-dated weekly_close plus the
            # weekly_indicators _01 column set (ported verbatim from
            # libs/vw_pipe_data_duckdb.py).  Volume is DOUBLE to match
            # our daily_close (legacy used BIGINT; same values).
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS weekly_close (
                    Date DATE,
                    Ticker VARCHAR,
                    Open DOUBLE,
                    High DOUBLE,
                    Low DOUBLE,
                    Close DOUBLE,
                    Volume DOUBLE,
                    PRIMARY KEY (Date, Ticker)
                );
            """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS weekly_indicators (
                    Date DATE, Ticker VARCHAR, Slow_K DOUBLE, Slow_D DOUBLE,
                    Stoch_Crossover INTEGER, Slow_D_80_20_Crossover INTEGER,
                    BB_Upper DOUBLE, BB_Central DOUBLE, BB_Lower DOUBLE,
                    BB_Reversal_Signal INTEGER, PPO DOUBLE, PPO_Signal DOUBLE,
                    PPO_Hist_Crossover INTEGER, PPO_Zero_Crossover INTEGER,
                    Force_Index DOUBLE, Force_Zero_Crossover INTEGER,
                    Log_Close_EMA18 DOUBLE, Log_Close_EMA40 DOUBLE,
                    PRIMARY KEY (Date, Ticker)
                );
            """)
            # F16-T1: CNN Fear & Greed history — one row per date; the
            # composite score plus each of the 9 sub-indicators' raw
            # metric value and textual rating.
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS fear_greed_history (
                    date DATE PRIMARY KEY,
                    fg_score DOUBLE,
                    fg_rating VARCHAR,
                    stock_price_strength DOUBLE,
                    stock_price_strength_rating VARCHAR,
                    stock_price_breadth DOUBLE,
                    stock_price_breadth_rating VARCHAR,
                    market_momentum_sp500 DOUBLE,
                    market_momentum_sp500_rating VARCHAR,
                    market_momentum_sp125 DOUBLE,
                    market_momentum_sp125_rating VARCHAR,
                    market_volatility_vix DOUBLE,
                    market_volatility_vix_rating VARCHAR,
                    market_volatility_vix_50 DOUBLE,
                    market_volatility_vix_50_rating VARCHAR,
                    put_call_options DOUBLE,
                    put_call_options_rating VARCHAR,
                    junk_bond_demand DOUBLE,
                    junk_bond_demand_rating VARCHAR,
                    safe_haven_demand DOUBLE,
                    safe_haven_demand_rating VARCHAR,
                    ingested_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
                );
            """)
            # F24-T1: signal-pipeline tables — DDL ported verbatim from
            # quant_strategy_building/signal_input (NUMERIC types kept to
            # preserve the legacy rounding semantics).  The signal
            # pipeline runs server-side via the ``signal_pipeline``
            # Flight action and joins ``daily_close`` in-database.
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS signal_records (
                    "Signal_File" VARCHAR,
                    "Model" VARCHAR,
                    "Trading_Date" DATE,
                    "Ticker" VARCHAR,
                    "Open" NUMERIC(12,2),
                    "High" NUMERIC(12,2),
                    "Low" NUMERIC(12,2),
                    "Close" NUMERIC(12,2),
                    "Cluster" NUMERIC(1,0),
                    "Hist_WinRate" NUMERIC(12,2),
                    "ATR" NUMERIC(12,2),
                    "best_k" NUMERIC(12,2),
                    "Target" NUMERIC(12,2),
                    "Current_Price" NUMERIC(12,2),
                    "Stop_Price" NUMERIC(12,2),
                    "Target_Percent" NUMERIC(12,2),
                    "Stop_Percent" NUMERIC(12,2),
                    "Reward_Risk_Ratio" NUMERIC(12,2),
                    "Shares_to_buy" NUMERIC(5,0),
                    "Max_Drawdown" NUMERIC(5,0),
                    "Centroid_Dist" NUMERIC(12,2),
                    "Drift_Warning" BOOLEAN,
                    PRIMARY KEY (Ticker, Trading_Date, Cluster, Model, Signal_File)
                );
            """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS signal_performance (
                    "Ticker" VARCHAR,
                    "Trading_Date" DATE,
                    "Cluster" NUMERIC(1,0),
                    "Model" VARCHAR,
                    "Signal_File" VARCHAR,
                    "Hit_Target_Date" DATE,
                    "Hit_Target_T" INTEGER,
                    "Hit_Stop_Date" DATE,
                    "Hit_Stop_T" INTEGER,
                    "Time_Exit_Close" NUMERIC(12,2),
                    "T" INTEGER,
                    PRIMARY KEY (Ticker, Trading_Date, Cluster, Model, Signal_File)
                );
            """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS cluster_metadata (
                    Cluster INTEGER PRIMARY KEY,
                    Description VARCHAR
                );
            """)
            # F26-T1: server-owned client-DML bookkeeping.  The registry
            # is the source of truth for which client-created objects
            # (allowlisted namespaces such as ``pivot``) exist — DROP
            # guards, pin/TTL and ``list_tables`` all read it; clients
            # can never write it (it lives in ``main`` outside every
            # allowed namespace).  The audit table logs EVERY dml op —
            # including REJECTED ones — exactly like ``ingestion_runs``
            # does for loads.
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS table_registry (
                    namespace VARCHAR NOT NULL,
                    table_name VARCHAR NOT NULL,
                    full_name VARCHAR NOT NULL,
                    owner VARCHAR,
                    has_pk BOOLEAN NOT NULL DEFAULT FALSE,
                    pinned BOOLEAN NOT NULL DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_write TIMESTAMP,
                    rows_est BIGINT,
                    PRIMARY KEY (namespace, table_name)
                );
            """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS client_dml_audit (
                    audit_id BIGINT PRIMARY KEY,
                    client_tag VARCHAR,
                    op VARCHAR NOT NULL,
                    target VARCHAR NOT NULL,
                    source_sql_sha256 VARCHAR,
                    bytes_in BIGINT,
                    rows_written BIGINT,
                    status VARCHAR NOT NULL,
                    error VARCHAR,
                    duration_ms INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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

            # --- v3 -> v4: company_metadata table (already created above) ---
            if current_version < 4:
                self.conn.execute(
                    "INSERT INTO schema_version (version) VALUES (?)", [4]
                )

            # --- v4 -> v5: weekly_close + weekly_indicators (created above) ---
            if current_version < 5:
                self.conn.execute(
                    "INSERT INTO schema_version (version) VALUES (?)", [5]
                )

            # --- v5 -> v6: fear_greed_history (created above) ---
            if current_version < 6:
                self.conn.execute(
                    "INSERT INTO schema_version (version) VALUES (?)", [6]
                )

            # --- v6 -> v7: signal_records + signal_performance +
            # cluster_metadata (created above; F24-T1) ---
            if current_version < 7:
                self.conn.execute(
                    "INSERT INTO schema_version (version) VALUES (?)", [7]
                )

            # --- v7 -> v8: table_registry + client_dml_audit
            # (created above; F26-T1) ---
            if current_version < 8:
                self.conn.execute(
                    "INSERT INTO schema_version (version) VALUES (?)", [8]
                )

            # --- v8 -> v9: RawClose column on daily_close (R1A-T1) ---
            # Fresh databases already carry RawClose from the CREATE
            # TABLE above; the ADD COLUMN is guarded so migrating an
            # existing DB is row-preserving and re-running is a no-op.
            if current_version < 9:
                cols = {
                    r[0]
                    for r in self.conn.execute(
                        "SELECT column_name FROM information_schema.columns "
                        "WHERE table_name = 'daily_close'"
                    ).fetchall()
                }
                if "RawClose" not in cols:
                    self.conn.execute(
                        "ALTER TABLE daily_close ADD COLUMN RawClose DOUBLE"
                    )
                self.conn.execute(
                    "INSERT INTO schema_version (version) VALUES (?)", [9]
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
        # R1A-T1: RawClose is part of the daily_close contract since
        # schema v9.  Legacy callers that omit the column on a matched
        # update must not erase an existing populated RawClose; only a
        # finite, non-NULL source RawClose can replace a stored value.
        # Zero (^IRX) and negative (WTI Apr-2020) are valid market data.
        if "RawClose" not in work.columns:
            work["RawClose"] = None
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
                    Volume = source.Volume,
                    RawClose = CASE
                        WHEN source.RawClose IS NOT NULL
                             AND isfinite(source.RawClose)
                        THEN source.RawClose
                        ELSE target.RawClose
                    END
                WHEN NOT MATCHED THEN INSERT
                    (Date, Ticker, Open, High, Low, Close, Volume, RawClose)
                VALUES
                    (source.Date, source.Ticker, source.Open, source.High,
                     source.Low, source.Close, source.Volume, source.RawClose);
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

    def delete_daily_rows(self, pairs: List[Tuple[str, str]]) -> int:
        """Delete specific ``(Ticker, Date)`` rows from ``daily_close``.

        INC-T6: targeted eviction of vendor artifact bars (e.g.
        delisting-terminal placeholder rows) — finer-grained than
        ``purge_ticker``/``purge_tickers``.  Write-locked, parameterized
        row-value ``IN`` delete; returns the number of rows deleted.
        An empty ``pairs`` list is a no-op returning 0.
        """
        if not pairs:
            return 0
        placeholders = ", ".join("(?, CAST(? AS DATE))" for _ in pairs)
        params: List[str] = []
        # justified: bound-parameter flattening must interleave ticker,
        # date in the exact order of the SQL placeholders — no
        # SQL/vectorized equivalent for building a param list, and the
        # list is tiny (an ops tool deleting a handful of rows).
        for ticker, row_date in pairs:
            params.extend([str(ticker), str(row_date)])
        with self._write_lock:
            result = self.conn.execute(
                "DELETE FROM daily_close "
                f"WHERE (Ticker, Date) IN ({placeholders})",
                params,
            )
            count = result.fetchone()[0] if result else 0
        logger.warning(
            "RUNTIME EVENT: Deleted %s daily_close row(s) for "
            "(ticker, date) pairs %s.",
            count,
            list(pairs),
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

    def get_ticker_earliest_date(self, ticker: str) -> Optional[str]:
        """Return the earliest Date for ``ticker`` as ISO string, or None if
        the ticker has no rows in ``daily_close``."""
        conn = self._read_only_conn()
        try:
            result = conn.execute(
                "SELECT MIN(Date) FROM daily_close WHERE Ticker = ?", [ticker]
            ).fetchone()
            if result and result[0]:
                return str(result[0])
            return None
        finally:
            conn.close()

    def detect_new_tickers(self, configured_tickers: List[str]) -> List[str]:
        existing = self.get_existing_tickers()
        return [t for t in configured_tickers if t not in existing]

    # ------------------------------------------------------------------
    # Company metadata (F10-T1)
    # ------------------------------------------------------------------

    def get_missing_metadata_tickers(self, tickers: List[str]) -> List[str]:
        """Return the entries of ``tickers`` absent from ``company_metadata``.

        SQL anti-join of the given tickers against ``company_metadata``
        (NOT ``daily_close`` — a ticker can have price history but no
        metadata row).  The result preserves the input order so callers
        fetch in configured order.
        """
        if not tickers:
            return []
        conn = self._read_only_conn()
        try:
            rows = conn.execute(
                """
                SELECT c.ticker
                FROM (SELECT unnest(?) AS ticker) AS c
                WHERE c.ticker NOT IN (SELECT ticker FROM company_metadata)
                """,
                [list(tickers)],
            ).fetchall()
        finally:
            conn.close()
        missing = {r[0] for r in rows}
        return [t for t in tickers if t in missing]

    def upsert_company_metadata(self, rows: List[Dict[str, Any]]) -> int:
        """Upsert ``company_metadata`` rows; return the count staged.

        ``rows`` is a list of dicts with keys ``ticker``,
        ``company_name``, ``sector``, ``industry``, ``quote_type``.
        ``ON CONFLICT (ticker) DO UPDATE`` refreshes the four fields —
        a successful-but-empty fetch writes NULL fields (a placeholder
        row so the ticker stops counting as missing), while a later
        successful fetch overwrites them.
        """
        if not rows:
            return 0
        work = pd.DataFrame(
            rows,
            columns=[
                "ticker",
                "company_name",
                "sector",
                "industry",
                "quote_type",
            ],
        )
        # pandas 3.x 'str' dtype cannot be scanned by duckdb's register()
        # path — normalize to object dtype exactly like upsert_dataframe.
        str_cols = [
            c for c in work.columns if str(work[c].dtype) in ("str", "string")
        ]
        if str_cols:
            work[str_cols] = work[str_cols].astype(object)
        with self._write_lock:
            self.conn.register("staging_metadata", work)
            try:
                self.conn.execute("""
                    INSERT INTO company_metadata AS target
                        (ticker, company_name, sector, industry, quote_type)
                    SELECT ticker, company_name, sector, industry, quote_type
                    FROM staging_metadata
                    ON CONFLICT (ticker) DO UPDATE SET
                        company_name = EXCLUDED.company_name,
                        sector = EXCLUDED.sector,
                        industry = EXCLUDED.industry,
                        quote_type = EXCLUDED.quote_type;
                """)
            finally:
                self.conn.unregister("staging_metadata")
        return len(rows)

    def upsert_fear_greed(self, df: pd.DataFrame) -> int:
        """Upsert a wide Fear & Greed frame into ``fear_greed_history``.

        ``df`` must carry exactly the ``FEAR_GREED_UPSERT_COLUMNS`` —
        ``date`` plus ``fg_score``/``fg_rating`` and each sub-indicator's
        ``<key>``/``<key>_rating`` pair (the engine materializes all of
        them, absent values as NULL).  Uses the project's
        ``INSERT ... ON CONFLICT (date) DO UPDATE`` idiom (same as
        ``upsert_company_metadata``), so a re-run updates changed values
        in place instead of duplicating rows; ``ingested_at`` is stamped
        on every write.  Returns the number of rows staged.
        """
        if df is None or df.empty:
            return 0
        work = df.copy()
        # pandas 3.x 'str' dtype cannot be scanned by duckdb's register()
        # path — normalize to object dtype exactly like upsert_dataframe.
        str_cols = [
            c for c in work.columns if str(work[c].dtype) in ("str", "string")
        ]
        if str_cols:
            work[str_cols] = work[str_cols].astype(object)
        # Coerce the PK to datetime64 so DuckDB compares DATE to DATE.
        work["date"] = pd.to_datetime(work["date"])
        cols = list(FEAR_GREED_UPSERT_COLUMNS)
        set_clause = ", ".join(
            f"{c} = EXCLUDED.{c}" for c in cols if c != "date"
        )
        with self._write_lock:
            self.conn.register("staging_fear_greed", work[cols])
            try:
                self.conn.execute(f"""
                    INSERT INTO fear_greed_history AS target
                        ({", ".join(cols)})
                    SELECT {", ".join(cols)}
                    FROM staging_fear_greed
                    ON CONFLICT (date) DO UPDATE SET
                        {set_clause},
                        ingested_at = now();
                """)
            finally:
                self.conn.unregister("staging_fear_greed")
        return len(work)

    # ------------------------------------------------------------------
    # Weekly layer (F12-T1) — verbatim ports of
    # libs/vw_pipe_data_duckdb.py::aggregate_to_weekly (:753-800) and
    # ::update_weekly_indicators_01 (:317-397).
    # ------------------------------------------------------------------

    def rebuild_weekly_close(self) -> int:
        """Rebuild ``weekly_close`` from ``daily_close``; returns rows written.

        Verbatim port of the legacy ``aggregate_to_weekly`` (live
        Friday-dated variant): the week key is
        ``(date_trunc('week', Date)::DATE + 4)`` — the *nominal Friday*
        of each ISO week, even when the week's sessions end earlier
        (holidays) or the current week is still partial.  OHLC uses
        ``arg_min(Open, Date)`` / ``arg_max(Close, Date)`` /
        ``max(High)`` / ``min(Low)``; ``Volume`` is summed.
        ``INSERT ... ON CONFLICT (Date, Ticker) DO UPDATE`` makes a
        repeat run idempotent.

        INC-T7: after the upsert, an anti-join DELETE removes orphan
        weekly rows whose ``(Date, Ticker)`` is no longer produced by
        the daily aggregation — previously a deleted daily row left its
        weekly rollup behind (e.g. the QRVO 2026-10-09 phantom bar that
        survived ``delete_daily_rows`` + ``weekly_refresh``).
        """
        with self._write_lock:
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS weekly_close (
                    Date DATE,
                    Ticker VARCHAR,
                    Open DOUBLE,
                    High DOUBLE,
                    Low DOUBLE,
                    Close DOUBLE,
                    Volume DOUBLE,
                    PRIMARY KEY (Date, Ticker)
                )
            """)
            self.conn.execute("""
                INSERT INTO weekly_close
                SELECT
                    (date_trunc('week', Date)::DATE + 4) AS Date,
                    Ticker,
                    arg_min(Open, Date) AS Open,
                    max(High) AS High,
                    min(Low) AS Low,
                    arg_max(Close, Date) AS Close,
                    sum(Volume) AS Volume
                FROM daily_close
                GROUP BY 1, 2
                ON CONFLICT (Date, Ticker) DO UPDATE SET
                    Open = excluded.Open,
                    High = excluded.High,
                    Low = excluded.Low,
                    Close = excluded.Close,
                    Volume = excluded.Volume
            """)
            # INC-T7: anti-join cleanup — remove weekly_close rows whose
            # (Date, Ticker) week key is no longer produced by the
            # daily aggregation (their daily source rows were deleted).
            self.conn.execute("""
                DELETE FROM weekly_close
                WHERE (Date, Ticker) NOT IN (
                    SELECT (date_trunc('week', Date)::DATE + 4) AS WeekDate,
                           Ticker
                    FROM daily_close
                )
            """)
            count = self.conn.execute(
                "SELECT COUNT(*) FROM weekly_close"
            ).fetchone()[0]
        logger.info(
            "Weekly aggregation complete. Total weekly records: %d", count
        )
        return int(count)

    def rebuild_weekly_indicators(self) -> int:
        """Rebuild ``weekly_indicators`` from ``weekly_close``; returns
        rows written.

        Verbatim port of the legacy ``update_weekly_indicators_01``
        (the _01 column set under the base ``weekly_indicators`` name):
        ``INSERT OR REPLACE`` over the raw_data -> stoch_steps ->
        stoch_slow_k -> moving_averages -> indicator_base ->
        signal_line_base -> final_calculations CTE chain, including
        ``stoch_80_20_x`` -> ``Slow_D_80_20_Crossover``.

        INC-T7: after the upsert, an anti-join DELETE removes orphan
        indicator rows whose ``(Date, Ticker)`` is absent from
        ``weekly_close`` (e.g. the week orphaned by a
        ``delete_daily_rows`` eviction upstream).
        """
        with self._write_lock:
            self.conn.execute("""
            CREATE TABLE IF NOT EXISTS weekly_indicators (
                Date DATE, Ticker VARCHAR, Slow_K DOUBLE, Slow_D DOUBLE,
                Stoch_Crossover INTEGER, Slow_D_80_20_Crossover INTEGER,
                BB_Upper DOUBLE, BB_Central DOUBLE, BB_Lower DOUBLE,
                BB_Reversal_Signal INTEGER, PPO DOUBLE, PPO_Signal DOUBLE,
                PPO_Hist_Crossover INTEGER, PPO_Zero_Crossover INTEGER,
                Force_Index DOUBLE, Force_Zero_Crossover INTEGER,
                Log_Close_EMA18 DOUBLE, Log_Close_EMA40 DOUBLE,
                PRIMARY KEY (Date, Ticker)
            );
            """)
            sql_query = """
            INSERT OR REPLACE INTO weekly_indicators
            WITH raw_data AS (
                SELECT *, LAG(Close) OVER (PARTITION BY Ticker ORDER BY Date) as prev_close FROM weekly_close
            ),
            stoch_steps AS (
                SELECT *,
                       ((Close - MIN(Low) OVER (PARTITION BY Ticker ORDER BY Date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW)) /
                       NULLIF(MAX(High) OVER (PARTITION BY Ticker ORDER BY Date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) -
                              MIN(Low) OVER (PARTITION BY Ticker ORDER BY Date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW), 0)) * 100 as fast_k
                FROM raw_data
            ),
            stoch_slow_k AS (
                SELECT *, AVG(fast_k) OVER (PARTITION BY Ticker ORDER BY Date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as slow_k FROM stoch_steps
            ),
            moving_averages AS (
                SELECT *,
                       AVG(slow_k) OVER (PARTITION BY Ticker ORDER BY Date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as slow_d,
                       AVG(Close) OVER (PARTITION BY Ticker ORDER BY Date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as bb_mid,
                       STDDEV(Close) OVER (PARTITION BY Ticker ORDER BY Date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as bb_std,
                       AVG(Close) OVER (PARTITION BY Ticker ORDER BY Date ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) as ema12,
                       AVG(Close) OVER (PARTITION BY Ticker ORDER BY Date ROWS BETWEEN 25 PRECEDING AND CURRENT ROW) as ema26,
                       AVG(Close) OVER (PARTITION BY Ticker ORDER BY Date ROWS BETWEEN 17 PRECEDING AND CURRENT ROW) as ema18,
                       AVG(Close) OVER (PARTITION BY Ticker ORDER BY Date ROWS BETWEEN 39 PRECEDING AND CURRENT ROW) as ema40
                FROM stoch_slow_k
            ),
            indicator_base AS (
                SELECT *, ((ema12 - ema26) / NULLIF(ema26, 0)) * 100 as ppo, (Close - prev_close) * Volume as force_idx FROM moving_averages
            ),
            signal_line_base AS (
                SELECT *, AVG(ppo) OVER (PARTITION BY Ticker ORDER BY Date ROWS BETWEEN 8 PRECEDING AND CURRENT ROW) as ppo_signal,
                       LAG(ppo) OVER (PARTITION BY Ticker ORDER BY Date) as prev_ppo,
                       LAG(force_idx) OVER (PARTITION BY Ticker ORDER BY Date) as prev_force,
                       LAG(slow_k) OVER (PARTITION BY Ticker ORDER BY Date) as prev_slow_k,
                       LAG(slow_d) OVER (PARTITION BY Ticker ORDER BY Date) as prev_slow_d
                FROM indicator_base
            ),
            final_calculations AS (
                SELECT Date, Ticker, slow_k, slow_d,
                    CASE WHEN slow_k > slow_d AND prev_slow_k <= prev_slow_d THEN 1
                         WHEN slow_k < slow_d AND prev_slow_k >= prev_slow_d THEN -1 ELSE 0 END as stoch_x,
                    CASE WHEN (slow_k < 20 AND prev_slow_k >= 20) OR (slow_k > 80 AND prev_slow_k <= 80) THEN
                         CASE WHEN slow_k > slow_d AND prev_slow_k <= prev_slow_d THEN 1
                              WHEN slow_k < slow_d AND prev_slow_k >= prev_slow_d THEN -1 ELSE 0 END
                         ELSE 0 END as stoch_80_20_x,
                    bb_mid + (2 * bb_std) as bb_up, bb_mid as bb_central, bb_mid - (2 * bb_std) as bb_low,
                    ppo, ppo_signal, LAG(ppo_signal) OVER (PARTITION BY Ticker ORDER BY Date) as prev_ppo_signal,
                    force_idx, prev_force, prev_ppo, Close, prev_close,
                    -- Guard the RATIO, not just Close: ema18/ema40 are SMA
                    -- windows that can themselves go <= 0 around
                    -- non-positive prints (CL=F Apr-2020), so
                    -- Close/ema <= 0 even when Close > 0.  NULL is the
                    -- honest value for a non-positive log-ratio.
                    CASE WHEN Close / NULLIF(ema18, 0) > 0
                         THEN LN(Close / NULLIF(ema18, 0)) END as log_ema18,
                    CASE WHEN Close / NULLIF(ema40, 0) > 0
                         THEN LN(Close / NULLIF(ema40, 0)) END as log_ema40
                FROM signal_line_base
            )
            SELECT Date, Ticker, slow_k, slow_d, stoch_x, stoch_80_20_x,
                bb_up, bb_central, bb_low,
                CASE WHEN prev_close < bb_low AND Close > bb_low THEN 1
                     WHEN prev_close > bb_up AND Close < bb_up THEN -1 ELSE 0 END as bb_rev,
                ppo, ppo_signal,
                CASE WHEN ppo > ppo_signal AND prev_ppo <= prev_ppo_signal THEN 1
                     WHEN ppo < ppo_signal AND prev_ppo >= prev_ppo_signal THEN -1 ELSE 0 END as ppo_x,
                CASE WHEN ppo > 0 AND prev_ppo <= 0 THEN 1 WHEN ppo < 0 AND prev_ppo >= 0 THEN -1 ELSE 0 END as ppo_zero_x,
                force_idx,
                CASE WHEN force_idx > 0 AND prev_force <= 0 THEN 1 WHEN force_idx < 0 AND prev_force >= 0 THEN -1 ELSE 0 END as force_zero_x,
                log_ema18, log_ema40
            FROM final_calculations;
            """
            self.conn.execute(sql_query)
            # INC-T7: anti-join cleanup — remove weekly_indicators rows
            # whose (Date, Ticker) no longer exists in weekly_close.
            self.conn.execute("""
                DELETE FROM weekly_indicators
                WHERE (Date, Ticker) NOT IN (
                    SELECT Date, Ticker FROM weekly_close
                )
            """)
            count = self.conn.execute(
                "SELECT COUNT(*) FROM weekly_indicators"
            ).fetchone()[0]
        logger.info(
            "Weekly indicators updated. Total indicator records: %d", count
        )
        return int(count)

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

    # ------------------------------------------------------------------
    # Client-DML registry + audit (F26-T1)
    #
    # ``table_registry`` is the source of truth for which client-created
    # objects exist in allowlisted namespaces (``pivot``); drop/pin/TTL
    # decisions consult it, never the catalog alone.
    # ``client_dml_audit`` logs EVERY op — SUCCESS, FAILED and REJECTED —
    # under ``_write_lock`` with the same ``MAX+1`` id idiom as
    # ``ingestion_runs``.
    #
    # The mutating helpers accept an optional ``con``: when the caller
    # already holds ``_write_lock`` inside a multi-statement unit it
    # passes its own cursor so the registry write joins the same
    # transaction; with ``con=None`` the helper acquires the lock on
    # ``self.conn`` itself (never call the locking form while holding
    # the lock — ``threading.Lock`` is not re-entrant).
    # ------------------------------------------------------------------

    _REGISTRY_COLUMNS = (
        "namespace", "table_name", "full_name", "owner", "has_pk",
        "pinned", "created_at", "last_write", "rows_est",
    )

    def record_dml_audit(
        self,
        *,
        client_tag: Optional[str] = None,
        op: str,
        target: str,
        source_sql_sha256: Optional[str] = None,
        bytes_in: Optional[int] = None,
        rows_written: Optional[int] = None,
        status: str,
        error: Optional[str] = None,
        duration_ms: Optional[int] = None,
    ) -> int:
        """Persist one ``client_dml_audit`` row; returns ``audit_id``.

        Mirrors :meth:`record_ingestion_run`: a single parameterized
        INSERT under ``_write_lock`` with the ``MAX+1`` id idiom.
        ``status`` is ``SUCCESS`` | ``FAILED`` | ``REJECTED``.
        """
        with self._write_lock:
            max_id_row = self.conn.execute(
                "SELECT COALESCE(MAX(audit_id), 0) FROM client_dml_audit"
            ).fetchone()
            audit_id = int(max_id_row[0]) + 1
            self.conn.execute(
                """
                INSERT INTO client_dml_audit
                (audit_id, client_tag, op, target, source_sql_sha256,
                 bytes_in, rows_written, status, error, duration_ms)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    audit_id,
                    client_tag,
                    op,
                    target,
                    source_sql_sha256,
                    bytes_in,
                    rows_written,
                    status,
                    error,
                    duration_ms,
                ],
            )
        return audit_id

    def register_table(
        self,
        namespace: str,
        table_name: str,
        owner: Optional[str] = None,
        has_pk: bool = False,
        pinned: bool = True,
        con=None,
    ) -> None:
        """Insert-or-refresh a ``table_registry`` row.

        On conflict the row keeps its original ``created_at`` and its
        ``pinned`` flag (a re-created object must not silently regain a
        pin the owner removed); ``has_pk``/``owner``/``last_write``
        refresh from the new registration.
        """
        params = [
            namespace,
            table_name,
            f"{namespace}.{table_name}",
            owner,
            bool(has_pk),
            bool(pinned),
        ]
        sql = """
            INSERT INTO table_registry
            (namespace, table_name, full_name, owner, has_pk, pinned,
             last_write)
            VALUES (?, ?, ?, ?, ?, ?, now())
            ON CONFLICT (namespace, table_name) DO UPDATE SET
                full_name = excluded.full_name,
                owner = excluded.owner,
                has_pk = excluded.has_pk,
                last_write = now()
        """
        if con is not None:
            con.execute(sql, params)
        else:
            with self._write_lock:
                self.conn.execute(sql, params)

    def unregister_table(self, namespace: str, table_name: str, con=None) -> None:
        """Remove a ``table_registry`` row (no-op when absent)."""
        sql = (
            "DELETE FROM table_registry "
            "WHERE namespace = ? AND table_name = ?"
        )
        if con is not None:
            con.execute(sql, [namespace, table_name])
        else:
            with self._write_lock:
                self.conn.execute(sql, [namespace, table_name])

    def touch_table(
        self,
        namespace: str,
        table_name: str,
        rows_est: Optional[int] = None,
        con=None,
    ) -> None:
        """Stamp ``last_write`` (and optionally ``rows_est``) on a row."""
        sql = """
            UPDATE table_registry
            SET last_write = now(),
                rows_est = COALESCE(?, rows_est)
            WHERE namespace = ? AND table_name = ?
        """
        if con is not None:
            con.execute(sql, [rows_est, namespace, table_name])
        else:
            with self._write_lock:
                self.conn.execute(sql, [rows_est, namespace, table_name])

    def set_table_pinned(
        self, namespace: str, table_name: str, pinned: bool, con=None
    ) -> bool:
        """Flip the registry pin; returns True when a row was updated."""
        sql = (
            "UPDATE table_registry SET pinned = ? "
            "WHERE namespace = ? AND table_name = ?"
        )
        params = [bool(pinned), namespace, table_name]
        if con is not None:
            result = con.execute(sql, params)
        else:
            with self._write_lock:
                result = self.conn.execute(sql, params)
        row = result.fetchone()
        return bool(row and row[0])

    @staticmethod
    def _registry_rows_to_dicts(rows) -> List[Dict[str, Any]]:
        cols = DatabaseManager._REGISTRY_COLUMNS
        return [
            {
                **dict(zip(cols, row)),
                "created_at": str(row[6]) if row[6] is not None else None,
                "last_write": str(row[7]) if row[7] is not None else None,
            }
            for row in rows
        ]

    def get_registered_table(
        self, namespace: str, table_name: str
    ) -> Optional[Dict[str, Any]]:
        """Return the registry row for one object, or ``None``."""
        conn = self._read_only_conn()
        try:
            row = conn.execute(
                """
                SELECT namespace, table_name, full_name, owner, has_pk,
                       pinned, created_at, last_write, rows_est
                FROM table_registry
                WHERE namespace = ? AND table_name = ?
                """,
                [namespace, table_name],
            ).fetchone()
        finally:
            conn.close()
        if row is None:
            return None
        return self._registry_rows_to_dicts([row])[0]

    def list_registered_tables(
        self, namespace: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Registry dump — every registered object, or one namespace."""
        conn = self._read_only_conn()
        try:
            if namespace is None:
                rows = conn.execute(
                    """
                    SELECT namespace, table_name, full_name, owner,
                           has_pk, pinned, created_at, last_write,
                           rows_est
                    FROM table_registry
                    ORDER BY namespace, table_name
                    """
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT namespace, table_name, full_name, owner,
                           has_pk, pinned, created_at, last_write,
                           rows_est
                    FROM table_registry
                    WHERE namespace = ?
                    ORDER BY table_name
                    """,
                    [namespace],
                ).fetchall()
        finally:
            conn.close()
        return self._registry_rows_to_dicts(rows)

    def expired_registrations(
        self, ttl_days: int, now: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """Registered objects eligible for TTL sweep.

        Only ``pinned = FALSE`` objects whose last activity
        (``COALESCE(last_write, created_at)``) is older than
        ``ttl_days`` are returned — pinned objects (the ``pivot``
        default) are never swept.
        """
        reference = now if now is not None else datetime.now()
        cutoff = reference - timedelta(days=int(ttl_days))
        conn = self._read_only_conn()
        try:
            rows = conn.execute(
                """
                SELECT namespace, table_name, full_name, owner, has_pk,
                       pinned, created_at, last_write, rows_est
                FROM table_registry
                WHERE pinned = FALSE
                  AND COALESCE(last_write, created_at) < ?
                ORDER BY namespace, table_name
                """,
                [cutoff],
            ).fetchall()
        finally:
            conn.close()
        return self._registry_rows_to_dicts(rows)

    def namespace_size_estimate_mb(self, namespace: str) -> float:
        """Estimated MB footprint of a namespace (``duckdb_tables``).

        Advisory figure for the optional ``client_dml.max_namespace_mb``
        quota — ``estimated_size`` is DuckDB's own per-table estimate,
        not byte-exact.
        """
        conn = self._read_only_conn()
        try:
            row = conn.execute(
                "SELECT COALESCE(SUM(estimated_size), 0) "
                "FROM duckdb_tables() WHERE schema_name = ?",
                [namespace],
            ).fetchone()
        finally:
            conn.close()
        return float(row[0]) / (1024.0 * 1024.0) if row else 0.0

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
            "last_run": str(row[5]) if row[5] is not None else None,
            "schema_version": row[4] if row[4] is not None else SCHEMA_VERSION,
        }

    # ------------------------------------------------------------------
    # Data-health checks (F9-T5) — report-only, never repair
    # ------------------------------------------------------------------

    # Cap on every per-ticker/per-day list embedded in the health report
    # so a badly damaged table cannot produce an unbounded payload.
    _HEALTH_MAX_ITEMS = 50

    @staticmethod
    def _nyse_sessions(
        start_date: str,
        end_date: str,
        now: Optional[pd.Timestamp] = None,
        grace_minutes: float = 120,
    ) -> Tuple[List[str], Optional[str]]:
        """Return NYSE session dates in ``[start_date, end_date]`` (ISO).

        Only sessions that have *ended* count: when ``now`` is given, a
        session is expected once ``market_close + grace_minutes <= now``
        (mcal's ``market_close`` column is tz-aware; a naive ``now`` is
        treated as UTC).  Returns ``(sessions, pending_session)`` where
        ``pending_session`` is the latest session filtered out for not
        yet having ended — ``None`` when nothing was filtered, or when
        ``now`` is not given (no filtering at all).
        """
        nyse = mcal.get_calendar("NYSE")
        sched = nyse.schedule(start_date=start_date, end_date=end_date)
        if sched.empty or now is None:
            return [d.strftime("%Y-%m-%d") for d in sched.index], None
        now_ts = pd.Timestamp(now)
        if now_ts.tzinfo is None:
            now_ts = now_ts.tz_localize("UTC")
        ended = (
            sched["market_close"] + pd.Timedelta(minutes=grace_minutes)
            <= now_ts
        )
        sessions = [
            d.strftime("%Y-%m-%d") for d in sched.index[ended.to_numpy()]
        ]
        pending_idx = sched.index[(~ended).to_numpy()]
        pending_session = (
            pending_idx.max().strftime("%Y-%m-%d")
            if len(pending_idx)
            else None
        )
        return sessions, pending_session

    def check_daily_close_health(
        self,
        tickers: Optional[List[str]] = None,
        include_gaps: bool = False,
        gap_days: int = 30,
        as_of: Optional[str] = None,
        trading_days: Optional[List[str]] = None,
        max_items: int = _HEALTH_MAX_ITEMS,
        grace_minutes: float = 120,
        now: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """Run report-only data-health checks against ``daily_close``.

        All checks are SQL-driven (SQL > vectorized > loop).  The
        returned dict carries ``health_status`` in
        ``{'OK', 'WARN', 'CHECK_FAILED'}`` plus per-check counts and
        bounded per-ticker lists (first ``max_items``, with totals).
        ``pending_session`` names the NYSE session excluded for not yet
        having ended (``None`` when every session counts — e.g. a
        post-close call or an injected ``trading_days``).

        Checks:

        * **staleness** — tickers whose ``MAX(Date)`` is older than the
          latest *ended* NYSE trading day.
        * **coverage** — entries of ``tickers`` absent from
          ``daily_close`` (skipped when ``tickers`` is None).
        * **integrity** — rows with NULL/non-positive OHLC, ``High <
          Low``, or ``Volume < 0``.
        * **duplicates** — ``(Ticker, Date)`` pairs with count > 1
          (defensive; the primary key normally prevents them).
        * **gaps** (full mode only, ``include_gaps=True``) — per-ticker
          missing NYSE sessions inside the trailing ``gap_days``-session
          window, via a SQL anti-join against the generated calendar.
          Covers tickers present in ``daily_close`` (intersected with
          ``tickers`` when provided); wholly-absent tickers are reported
          by the coverage check instead.

        Parameters
        ----------
        as_of:
            Reference date (``YYYY-MM-DD``) used to derive the latest
            NYSE trading day and the gap window; defaults to today (UTC).
        trading_days:
            Injectable list of expected session dates — when given,
            ``pandas_market_calendars`` is not consulted, every listed
            session counts, the latest trading day is
            ``max(trading_days)`` and the gap window is its last
            ``gap_days`` entries.
        gap_days:
            Number of trailing NYSE sessions in the gap window (>= 1).
        max_items:
            Bound applied to every per-ticker list in the report.
        grace_minutes:
            Minutes after a session's NYSE ``market_close`` before its
            data is expected (default 120 — covers the scheduled
            ~16:14 ET load completing).  Sessions whose
            ``market_close + grace_minutes`` is still in the future are
            excluded from ``latest_trading_day`` and the gap window, so
            pre-open/early-session calls stop flagging today as missing.
        now:
            Injectable wall clock (``datetime``) used to decide which
            sessions have ended; naive values are treated as UTC.  When
            omitted: real time when ``as_of`` is also omitted, else
            end-of-``as_of``-day UTC so sessions on ``as_of`` still
            count (preserves the original test semantics).

        This method never raises: any failure (bad calendar, SQL error)
        returns ``health_status == 'CHECK_FAILED'`` with an ``error``
        field — health is report-only and must never break a load.
        """
        mode = "full" if include_gaps else "light"
        try:
            # Resolve the effective "now" used to decide which NYSE
            # sessions have ended.  A naive now is treated as UTC; an
            # explicit as_of without now treats the whole as_of day as
            # elapsed (end-of-day UTC) so sessions on as_of still count
            # — preserving the pre-F9-T7 test semantics.
            if now is None:
                if as_of is None:
                    now_utc = pd.Timestamp.now(tz="UTC")
                else:
                    now_utc = (
                        pd.Timestamp(as_of).tz_localize("UTC")
                        + pd.Timedelta(days=1)
                        - pd.Timedelta(microseconds=1)
                    )
            else:
                now_utc = pd.Timestamp(now)
                now_utc = (
                    now_utc.tz_localize("UTC")
                    if now_utc.tzinfo is None
                    else now_utc.tz_convert("UTC")
                )
            if as_of is None:
                as_of = now_utc.strftime("%Y-%m-%d")
            if (
                not isinstance(gap_days, int)
                or isinstance(gap_days, bool)
                or gap_days < 1
            ):
                raise ValueError(
                    f"gap_days must be an integer >= 1, got {gap_days!r}"
                )
            if trading_days is not None:
                # Injected calendar: every listed session is expected —
                # no ended-session filtering, nothing pending.
                sessions = sorted({str(d)[:10] for d in trading_days})
                pending_session = None
            else:
                # Look back far enough to cover gap_days sessions plus
                # holiday/weekend stretches (sessions are ~5 of 7 days).
                lookback = max(gap_days * 3, 45) if include_gaps else 30
                start = (
                    pd.Timestamp(as_of) - pd.Timedelta(days=lookback)
                ).strftime("%Y-%m-%d")
                sessions, pending_session = self._nyse_sessions(
                    start, as_of, now=now_utc, grace_minutes=grace_minutes
                )
            latest_trading_day = sessions[-1] if sessions else None
            if latest_trading_day is None:
                return {
                    "health_status": "CHECK_FAILED",
                    "mode": mode,
                    "as_of": as_of,
                    "error": (
                        "could not determine the latest NYSE trading day "
                        f"as of {as_of}"
                    ),
                }

            conn = self._read_only_conn()
            try:
                # (a) STALENESS — per-ticker MAX(Date) older than the
                # latest NYSE trading day.
                stale_rows = conn.execute(
                    """
                    SELECT Ticker, CAST(MAX(Date) AS VARCHAR) AS last_date
                    FROM daily_close
                    GROUP BY Ticker
                    HAVING MAX(Date) < CAST(? AS DATE)
                    ORDER BY Ticker
                    """,
                    [latest_trading_day],
                ).fetchall()

                # (b) COVERAGE — configured tickers absent from
                # daily_close (SQL anti-join via unnest).
                if tickers is not None:
                    missing_rows = conn.execute(
                        """
                        SELECT c.ticker
                        FROM (SELECT unnest(?) AS ticker) AS c
                        WHERE c.ticker NOT IN (
                            SELECT Ticker FROM daily_close
                        )
                        ORDER BY c.ticker
                        """,
                        [list(tickers)],
                    ).fetchall()
                else:
                    missing_rows = []

                # (c) INTEGRITY — NULL/non-positive OHLC, High < Low,
                # Volume < 0.  NULLs in High/Low make the comparison NULL
                # (not counted there); they are already flagged by the
                # OHLC term.
                integrity_row = conn.execute(
                    """
                    SELECT
                        COUNT(*) FILTER (
                            Open IS NULL OR Open <= 0 OR
                            High IS NULL OR High <= 0 OR
                            Low IS NULL OR Low <= 0 OR
                            Close IS NULL OR Close <= 0
                        ) AS bad_ohlc,
                        COUNT(*) FILTER (High < Low) AS high_lt_low,
                        COUNT(*) FILTER (Volume < 0) AS negative_volume,
                        COUNT(*) FILTER (
                            Open IS NULL OR Open <= 0 OR
                            High IS NULL OR High <= 0 OR
                            Low IS NULL OR Low <= 0 OR
                            Close IS NULL OR Close <= 0 OR
                            High < Low OR Volume < 0
                        ) AS violation_count
                    FROM daily_close
                    """
                ).fetchone()
                integrity_samples = conn.execute(
                    """
                    SELECT Ticker, CAST(Date AS VARCHAR) AS d, reason
                    FROM (
                        SELECT Ticker, Date,
                               'null_or_nonpositive_ohlc' AS reason
                        FROM daily_close
                        WHERE Open IS NULL OR Open <= 0 OR
                              High IS NULL OR High <= 0 OR
                              Low IS NULL OR Low <= 0 OR
                              Close IS NULL OR Close <= 0
                        UNION ALL
                        SELECT Ticker, Date, 'high_lt_low'
                        FROM daily_close WHERE High < Low
                        UNION ALL
                        SELECT Ticker, Date, 'negative_volume'
                        FROM daily_close WHERE Volume < 0
                    ) v
                    ORDER BY Ticker, d
                    LIMIT ?
                    """,
                    [max_items],
                ).fetchall()

                # (d) RAW_CLOSE — NULL/non-finite RawClose, or nonzero
                # RawClose with a non-finite/non-positive Close/RawClose
                # factor.  Finite zero/negative RawClose values are
                # valid market observations; zero RawClose has an
                # undefined factor but is not itself invalid.
                raw_invalid_condition = """
                    RawClose IS NULL OR isnan(RawClose) OR isinf(RawClose)
                    OR (
                        RawClose <> 0
                        AND (
                            isnan(Close / RawClose)
                            OR isinf(Close / RawClose)
                            OR (Close / RawClose) <= 0
                        )
                    )
                """
                # Priority-ordered reason filters so each invalid row is
                # counted exactly once (count-faithful reason_counts).
                raw_close_row = conn.execute(
                    f"""
                    SELECT
                        COUNT(*) AS total_rows,
                        COUNT(*) FILTER (WHERE ({raw_invalid_condition}))
                            AS invalid_count,
                        COUNT(DISTINCT CASE WHEN ({raw_invalid_condition})
                                       THEN Ticker END)
                            AS affected_ticker_count,
                        COUNT(*) FILTER (
                            WHERE ({raw_invalid_condition})
                              AND RawClose IS NULL
                        ) AS null_raw,
                        COUNT(*) FILTER (
                            WHERE ({raw_invalid_condition})
                              AND RawClose IS NOT NULL
                              AND (isnan(RawClose) OR isinf(RawClose))
                        ) AS nonfinite_raw,
                        COUNT(*) FILTER (
                            WHERE ({raw_invalid_condition})
                              AND RawClose IS NOT NULL
                              AND NOT (isnan(RawClose) OR isinf(RawClose))
                              AND RawClose <> 0
                              AND (
                                  isnan(Close / RawClose)
                                  OR isinf(Close / RawClose)
                              )
                        ) AS nonfinite_factor,
                        COUNT(*) FILTER (
                            WHERE ({raw_invalid_condition})
                              AND RawClose IS NOT NULL
                              AND NOT (isnan(RawClose) OR isinf(RawClose))
                              AND RawClose <> 0
                              AND NOT (
                                  isnan(Close / RawClose)
                                  OR isinf(Close / RawClose)
                              )
                              AND (Close / RawClose) <= 0
                        ) AS nonpositive_factor,
                        CAST(MAX(Date) FILTER (
                            WHERE ({raw_invalid_condition})
                        ) AS VARCHAR) AS latest_invalid_date
                    FROM daily_close
                    """
                ).fetchone()
                raw_close_samples = conn.execute(
                    f"""
                    SELECT Ticker,
                           COUNT(*) AS invalid_count,
                           CAST(MIN(Date) AS VARCHAR) AS first_date,
                           CAST(MAX(Date) AS VARCHAR) AS last_date
                    FROM daily_close
                    WHERE {raw_invalid_condition}
                    GROUP BY Ticker
                    ORDER BY Ticker
                    LIMIT ?
                    """,
                    [max_items],
                ).fetchall()

                # (e) DUPLICATE (Ticker, Date) keys — defensive; the
                # PRIMARY KEY (Date, Ticker) normally prevents these.
                dup_rows = conn.execute(
                    """
                    SELECT Ticker, CAST(Date AS VARCHAR) AS d,
                           COUNT(*) AS n
                    FROM daily_close
                    GROUP BY Ticker, Date
                    HAVING COUNT(*) > 1
                    ORDER BY Ticker, d
                    """
                ).fetchall()

                # (e) GAPS — full mode only: per-ticker missing NYSE
                # sessions inside the trailing gap_days-session window
                # (SQL anti-join vs the generated calendar).
                gap_rows: List[Tuple] = []
                window: List[str] = []
                if include_gaps:
                    window = sessions[-gap_days:]
                    if tickers is not None:
                        universe_sql = (
                            "SELECT DISTINCT Ticker FROM daily_close "
                            "WHERE Ticker IN (SELECT unnest(?) AS u)"
                        )
                        # Positional params appear in SQL-text order:
                        # cal's unnest first, then universe's unnest.
                        gap_params: List[Any] = [window, list(tickers)]
                    else:
                        universe_sql = (
                            "SELECT DISTINCT Ticker FROM daily_close"
                        )
                        gap_params = [window]
                    gap_rows = conn.execute(
                        f"""
                        WITH cal AS (
                            SELECT CAST(u AS DATE) AS day
                            FROM (SELECT unnest(?) AS u) s
                        ),
                        win AS (
                            SELECT Ticker, Date FROM daily_close
                            WHERE Date BETWEEN (SELECT MIN(day) FROM cal)
                                           AND (SELECT MAX(day) FROM cal)
                        ),
                        universe AS ({universe_sql})
                        SELECT t.Ticker,
                               COUNT(*) AS missing_count,
                               LIST(CAST(c.day AS VARCHAR) ORDER BY c.day)
                                   AS missing_days
                        FROM universe t
                        CROSS JOIN cal c
                        LEFT JOIN win w
                               ON w.Ticker = t.Ticker AND w.Date = c.day
                        WHERE w.Date IS NULL
                        GROUP BY t.Ticker
                        ORDER BY t.Ticker
                        """,
                        gap_params,
                    ).fetchall()
            finally:
                conn.close()
        except Exception as exc:
            logger.error("daily_close health check failed: %s", exc)
            return {
                "health_status": "CHECK_FAILED",
                "mode": mode,
                "as_of": as_of,
                "error": str(exc),
            }

        violation_count = int(integrity_row[3]) if integrity_row else 0
        total_missing_days = sum(int(r[1]) for r in gap_rows)
        raw_close_invalid = int(raw_close_row[1]) if raw_close_row else 0
        findings = (
            len(stale_rows)
            + len(missing_rows)
            + violation_count
            + len(dup_rows)
            + total_missing_days
            + raw_close_invalid
        )
        report: Dict[str, Any] = {
            "health_status": "WARN" if findings else "OK",
            "mode": mode,
            "as_of": as_of,
            "latest_trading_day": latest_trading_day,
            "pending_session": pending_session,
            "checks": {
                "staleness": {
                    "stale_count": len(stale_rows),
                    "stale_tickers": [
                        {"ticker": r[0], "last_date": r[1]}
                        for r in stale_rows[:max_items]
                    ],
                },
                "coverage": {
                    "configured_count": (
                        len(tickers) if tickers is not None else 0
                    ),
                    "missing_count": len(missing_rows),
                    "missing_tickers": [
                        r[0] for r in missing_rows[:max_items]
                    ],
                },
                "integrity": {
                    "violation_count": violation_count,
                    "reason_counts": {
                        "null_or_nonpositive_ohlc": (
                            int(integrity_row[0]) if integrity_row else 0
                        ),
                        "high_lt_low": (
                            int(integrity_row[1]) if integrity_row else 0
                        ),
                        "negative_volume": (
                            int(integrity_row[2]) if integrity_row else 0
                        ),
                    },
                    "violations": [
                        {"ticker": r[0], "date": r[1], "reason": r[2]}
                        for r in integrity_samples
                    ],
                },
                "raw_close": {
                    "total_rows": (
                        int(raw_close_row[0]) if raw_close_row else 0
                    ),
                    "invalid_count": raw_close_invalid,
                    "affected_ticker_count": (
                        int(raw_close_row[2]) if raw_close_row else 0
                    ),
                    "reason_counts": {
                        "null_raw": (
                            int(raw_close_row[3]) if raw_close_row else 0
                        ),
                        "nonfinite_raw": (
                            int(raw_close_row[4]) if raw_close_row else 0
                        ),
                        "nonfinite_factor": (
                            int(raw_close_row[5]) if raw_close_row else 0
                        ),
                        "nonpositive_factor": (
                            int(raw_close_row[6]) if raw_close_row else 0
                        ),
                    },
                    # HRAW-T2 — MAX(Date) over invalid rows only, so the
                    # watcher can judge recency for tickers omitted by
                    # per_ticker truncation; NULL when invalid_count=0.
                    "latest_invalid_date": (
                        raw_close_row[7] if raw_close_row else None
                    ),
                    "per_ticker": [
                        {
                            "ticker": r[0],
                            "invalid_count": int(r[1]),
                            "first_date": r[2],
                            "last_date": r[3],
                        }
                        for r in raw_close_samples
                    ],
                },
                "duplicates": {
                    "duplicate_count": len(dup_rows),
                    "duplicates": [
                        {"ticker": r[0], "date": r[1], "count": int(r[2])}
                        for r in dup_rows[:max_items]
                    ],
                },
            },
        }
        if include_gaps:
            report["checks"]["gaps"] = {
                "gap_days": gap_days,
                "window_start": window[0] if window else None,
                "window_end": window[-1] if window else None,
                "expected_sessions": len(window),
                "tickers_with_gaps": len(gap_rows),
                "total_missing_days": total_missing_days,
                "per_ticker": [
                    {
                        "ticker": r[0],
                        "missing_count": int(r[1]),
                        "missing_days": [
                            str(d) for d in r[2][:max_items]
                        ],
                    }
                    for r in gap_rows[:max_items]
                ],
            }
        return report

    # ------------------------------------------------------------------
    # New-table health checks (F17-T1) — report-only, never repair.
    # Staleness reuses the same "latest ended NYSE session" machinery as
    # check_daily_close_health (F9-T5/F9-T7).
    # ------------------------------------------------------------------

    def _ended_sessions(
        self,
        as_of: Optional[str],
        trading_days: Optional[List[str]],
        now: Optional[datetime],
        grace_minutes: float,
    ) -> Tuple[List[str], Optional[str], str]:
        """Resolve ``(sessions, pending_session, as_of)`` for a check.

        Identical resolution rules to ``check_daily_close_health``: an
        injected ``trading_days`` list bypasses the calendar (nothing
        pending); otherwise the bundled NYSE calendar over the trailing
        30 days is filtered to sessions whose ``market_close +
        grace_minutes <= now``.  A naive ``now`` is treated as UTC; an
        explicit ``as_of`` without ``now`` counts the whole as_of day as
        elapsed.
        """
        if now is None:
            if as_of is None:
                now_utc = pd.Timestamp.now(tz="UTC")
            else:
                now_utc = (
                    pd.Timestamp(as_of).tz_localize("UTC")
                    + pd.Timedelta(days=1)
                    - pd.Timedelta(microseconds=1)
                )
        else:
            now_utc = pd.Timestamp(now)
            now_utc = (
                now_utc.tz_localize("UTC")
                if now_utc.tzinfo is None
                else now_utc.tz_convert("UTC")
            )
        if as_of is None:
            as_of = now_utc.strftime("%Y-%m-%d")
        if trading_days is not None:
            # Injected calendar: every listed session is expected — no
            # ended-session filtering, nothing pending.
            sessions = sorted({str(d)[:10] for d in trading_days})
            pending_session = None
        else:
            start = (
                pd.Timestamp(as_of) - pd.Timedelta(days=30)
            ).strftime("%Y-%m-%d")
            sessions, pending_session = self._nyse_sessions(
                start, as_of, now=now_utc, grace_minutes=grace_minutes
            )
        return sessions, pending_session, as_of

    def check_fear_greed_health(
        self,
        enabled: bool = True,
        as_of: Optional[str] = None,
        trading_days: Optional[List[str]] = None,
        max_items: int = _HEALTH_MAX_ITEMS,
        grace_minutes: float = 120,
        now: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """Run report-only health checks against ``fear_greed_history``.

        SQL-driven like ``check_daily_close_health``; never raises —
        any failure returns ``health_status == 'CHECK_FAILED'`` with an
        ``error`` field.  ``enabled=False`` (``cfg.fear_greed.enabled``)
        returns ``health_status == 'DISABLED'`` immediately, which the
        composite rollup excludes.

        Checks:

        * **presence** — an empty table -> WARN.
        * **staleness** — ``MAX(date)`` vs the latest *ended* NYSE
          session (``stale`` flag + ``latest_date``/``expected_date``/
          ``days_behind`` + ``pending_session``).
        * **integrity** — ``fg_score`` outside [0, 100], ``fg_rating``
          outside the valid vocabulary (NULL allowed), and NULL
          ``fg_score`` counts, each with bounded samples.
        * **series** — sub-indicator value columns that are fully NULL
          -> ``missing_series``.
        """
        try:
            if not enabled:
                return {
                    "health_status": "DISABLED",
                    "as_of": as_of,
                    "reason": "fear_greed.enabled is false",
                }
            sessions, pending_session, as_of = self._ended_sessions(
                as_of, trading_days, now, grace_minutes
            )
            latest_trading_day = sessions[-1] if sessions else None
            if latest_trading_day is None:
                return {
                    "health_status": "CHECK_FAILED",
                    "as_of": as_of,
                    "error": (
                        "could not determine the latest NYSE trading day "
                        f"as of {as_of}"
                    ),
                }

            ratings = ", ".join(f"'{r}'" for r in FEAR_GREED_VALID_RATINGS)
            conn = self._read_only_conn()
            try:
                summary = conn.execute(
                    """
                    SELECT COUNT(*) AS n,
                           CAST(MAX(date) AS VARCHAR) AS latest_date
                    FROM fear_greed_history
                    """
                ).fetchone()
                integrity = conn.execute(
                    f"""
                    SELECT
                        COUNT(*) FILTER (fg_score IS NULL) AS null_score,
                        COUNT(*) FILTER (
                            fg_score < 0 OR fg_score > 100
                        ) AS out_of_range,
                        COUNT(*) FILTER (
                            fg_rating IS NOT NULL
                            AND fg_rating NOT IN ({ratings})
                        ) AS bad_rating
                    FROM fear_greed_history
                    """
                ).fetchone()
                oor_samples = conn.execute(
                    """
                    SELECT CAST(date AS VARCHAR) AS d, fg_score
                    FROM fear_greed_history
                    WHERE fg_score < 0 OR fg_score > 100
                    ORDER BY d
                    LIMIT ?
                    """,
                    [max_items],
                ).fetchall()
                rating_samples = conn.execute(
                    f"""
                    SELECT CAST(date AS VARCHAR) AS d, fg_rating
                    FROM fear_greed_history
                    WHERE fg_rating IS NOT NULL
                      AND fg_rating NOT IN ({ratings})
                    ORDER BY d
                    LIMIT ?
                    """,
                    [max_items],
                ).fetchall()
                # Fully-NULL series: COUNT(<key>) counts non-NULL cells;
                # 0 means the column never carried a value.  The column
                # list comes from the FEAR_GREED_SUB_INDICATORS constant,
                # never caller input.
                series_counts = conn.execute(
                    "SELECT "
                    + ", ".join(
                        f"COUNT({k})" for k in FEAR_GREED_SUB_INDICATORS
                    )
                    + " FROM fear_greed_history"
                ).fetchone()
            finally:
                conn.close()
        except Exception as exc:
            logger.error("fear_greed health check failed: %s", exc)
            return {
                "health_status": "CHECK_FAILED",
                "as_of": as_of,
                "error": str(exc),
            }

        row_count = int(summary[0]) if summary else 0
        latest_date = summary[1] if summary else None
        stale = latest_date is None or latest_date < latest_trading_day
        days_behind = (
            (pd.Timestamp(latest_trading_day) - pd.Timestamp(latest_date)).days
            if latest_date is not None
            else None
        )
        null_score = int(integrity[0]) if integrity else 0
        out_of_range = int(integrity[1]) if integrity else 0
        bad_rating = int(integrity[2]) if integrity else 0
        missing_series = [
            key
            for key, cnt in zip(FEAR_GREED_SUB_INDICATORS, series_counts)
            if int(cnt) == 0
        ]
        findings = (
            (1 if row_count == 0 else 0)
            + (1 if stale else 0)
            + null_score
            + out_of_range
            + bad_rating
            + len(missing_series)
        )
        return {
            "health_status": "WARN" if findings else "OK",
            "as_of": as_of,
            "latest_trading_day": latest_trading_day,
            "pending_session": pending_session,
            "row_count": row_count,
            "checks": {
                "presence": {
                    "row_count": row_count,
                    "empty": row_count == 0,
                },
                "staleness": {
                    "stale": stale,
                    "latest_date": latest_date,
                    "expected_date": latest_trading_day,
                    "days_behind": days_behind,
                },
                "integrity": {
                    "null_fg_score_count": null_score,
                    "out_of_range_count": out_of_range,
                    "out_of_range_samples": [
                        {"date": r[0], "fg_score": r[1]}
                        for r in oor_samples
                    ],
                    "invalid_rating_count": bad_rating,
                    "invalid_rating_samples": [
                        {"date": r[0], "fg_rating": r[1]}
                        for r in rating_samples
                    ],
                },
                "series": {
                    "tracked": len(FEAR_GREED_SUB_INDICATORS),
                    "missing_series": missing_series,
                },
            },
        }

    def check_weekly_health(
        self,
        as_of: Optional[str] = None,
        trading_days: Optional[List[str]] = None,
        max_items: int = _HEALTH_MAX_ITEMS,
        grace_minutes: float = 120,
        now: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """Run report-only health checks against the weekly layer.

        SQL-driven like ``check_daily_close_health``; never raises.
        Checks:

        * **staleness** — ``MAX(weekly_close.Date)`` vs the expected
          nominal-Friday key
          ``date_trunc('week', latest_ended_session)::DATE + 4`` (F12
          week keying); ``latest_week``/``expected_week``/
          ``pending_session`` evidence fields.
        * **parity** — ``weekly_close`` vs ``weekly_indicators`` row
          counts plus the count (and bounded sample) of
          ``(Date, Ticker)`` keys in ``weekly_close`` missing from
          ``weekly_indicators``.
        * **stale tickers** — tickers present in ``weekly_close`` but
          absent from ``daily_close`` (bounded list).
        * **integrity** — NULL OHLC / ``Volume < 0`` counts on
          ``weekly_close``.
        """
        try:
            sessions, pending_session, as_of = self._ended_sessions(
                as_of, trading_days, now, grace_minutes
            )
            latest_trading_day = sessions[-1] if sessions else None
            if latest_trading_day is None:
                return {
                    "health_status": "CHECK_FAILED",
                    "as_of": as_of,
                    "error": (
                        "could not determine the latest NYSE trading day "
                        f"as of {as_of}"
                    ),
                }

            conn = self._read_only_conn()
            try:
                summary = conn.execute(
                    """
                    SELECT
                        CAST((SELECT MAX(Date) FROM weekly_close)
                             AS VARCHAR) AS latest_week,
                        CAST(date_trunc('week', CAST(? AS DATE))::DATE + 4
                             AS VARCHAR) AS expected_week,
                        (SELECT COUNT(*) FROM weekly_close) AS close_rows,
                        (SELECT COUNT(*) FROM weekly_indicators)
                            AS indicator_rows
                    """,
                    [latest_trading_day],
                ).fetchone()
                missing_key_count = conn.execute(
                    """
                    SELECT COUNT(*) FROM weekly_close w
                    WHERE NOT EXISTS (
                        SELECT 1 FROM weekly_indicators i
                        WHERE i.Date = w.Date AND i.Ticker = w.Ticker
                    )
                    """
                ).fetchone()
                missing_key_samples = conn.execute(
                    """
                    SELECT CAST(w.Date AS VARCHAR) AS d, w.Ticker
                    FROM weekly_close w
                    WHERE NOT EXISTS (
                        SELECT 1 FROM weekly_indicators i
                        WHERE i.Date = w.Date AND i.Ticker = w.Ticker
                    )
                    ORDER BY d, w.Ticker
                    LIMIT ?
                    """,
                    [max_items],
                ).fetchall()
                stale_ticker_rows = conn.execute(
                    """
                    SELECT DISTINCT Ticker FROM weekly_close
                    WHERE Ticker NOT IN (SELECT Ticker FROM daily_close)
                    ORDER BY Ticker
                    """
                ).fetchall()
                integrity = conn.execute(
                    """
                    SELECT
                        COUNT(*) FILTER (
                            Open IS NULL OR High IS NULL OR
                            Low IS NULL OR Close IS NULL
                        ) AS null_ohlc,
                        COUNT(*) FILTER (Volume < 0) AS negative_volume
                    FROM weekly_close
                    """
                ).fetchone()
            finally:
                conn.close()
        except Exception as exc:
            logger.error("weekly health check failed: %s", exc)
            return {
                "health_status": "CHECK_FAILED",
                "as_of": as_of,
                "error": str(exc),
            }

        latest_week = summary[0] if summary else None
        expected_week = summary[1] if summary else None
        close_rows = int(summary[2]) if summary else 0
        indicator_rows = int(summary[3]) if summary else 0
        stale = latest_week is None or latest_week < expected_week
        missing_keys = int(missing_key_count[0]) if missing_key_count else 0
        null_ohlc = int(integrity[0]) if integrity else 0
        negative_volume = int(integrity[1]) if integrity else 0
        findings = (
            (1 if stale else 0)
            + (1 if close_rows != indicator_rows else 0)
            + missing_keys
            + len(stale_ticker_rows)
            + null_ohlc
            + negative_volume
        )
        return {
            "health_status": "WARN" if findings else "OK",
            "as_of": as_of,
            "latest_trading_day": latest_trading_day,
            "pending_session": pending_session,
            "checks": {
                "staleness": {
                    "stale": stale,
                    "latest_week": latest_week,
                    "expected_week": expected_week,
                },
                "parity": {
                    "weekly_close_rows": close_rows,
                    "weekly_indicators_rows": indicator_rows,
                    "row_count_match": close_rows == indicator_rows,
                    "missing_indicator_keys": missing_keys,
                    "missing_keys": [
                        {"date": r[0], "ticker": r[1]}
                        for r in missing_key_samples
                    ],
                },
                "stale_tickers": {
                    "count": len(stale_ticker_rows),
                    "tickers": [
                        r[0] for r in stale_ticker_rows[:max_items]
                    ],
                },
                "integrity": {
                    "null_ohlc_count": null_ohlc,
                    "negative_volume_count": negative_volume,
                },
            },
        }

    def check_metadata_health(
        self,
        tickers: Optional[List[str]] = None,
        max_items: int = _HEALTH_MAX_ITEMS,
    ) -> Dict[str, Any]:
        """Run report-only health checks against ``company_metadata``.

        SQL-driven; never raises.  Checks:

        * **coverage** — configured-minus-excluded ``tickers`` absent
          from ``company_metadata`` -> WARN (bounded list).  Skipped
          when ``tickers`` is None.
        * **null fields** — rows where all four value columns
          (``company_name``/``sector``/``industry``/``quote_type``) are
          NULL.  These are legitimate placeholders for unknown symbols
          (F10-T1) — informational count only, never a finding.
        """
        try:
            conn = self._read_only_conn()
            try:
                summary = conn.execute(
                    """
                    SELECT
                        COUNT(*) AS n,
                        COUNT(*) FILTER (
                            company_name IS NULL AND sector IS NULL AND
                            industry IS NULL AND quote_type IS NULL
                        ) AS null_field_rows
                    FROM company_metadata
                    """
                ).fetchone()
                if tickers is not None:
                    missing_rows = conn.execute(
                        """
                        SELECT c.ticker
                        FROM (SELECT unnest(?) AS ticker) AS c
                        WHERE c.ticker NOT IN (
                            SELECT ticker FROM company_metadata
                        )
                        ORDER BY c.ticker
                        """,
                        [list(tickers)],
                    ).fetchall()
                else:
                    missing_rows = []
            finally:
                conn.close()
        except Exception as exc:
            logger.error("company_metadata health check failed: %s", exc)
            return {"health_status": "CHECK_FAILED", "error": str(exc)}

        return {
            "health_status": "WARN" if missing_rows else "OK",
            "checks": {
                "coverage": {
                    "configured_count": (
                        len(tickers) if tickers is not None else 0
                    ),
                    "missing_count": len(missing_rows),
                    "missing_tickers": [
                        r[0] for r in missing_rows[:max_items]
                    ],
                },
                "null_fields": {
                    "row_count": int(summary[0]) if summary else 0,
                    "null_field_rows": int(summary[1]) if summary else 0,
                },
            },
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
