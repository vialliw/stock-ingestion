"""Challenger variant of the DuckDB ``DatabaseManager``.

This implementation deliberately differs structurally from the reference
implementation in spec §10 (which ``src/db.py`` is expected to follow
closely).  The public API is identical so callers can substitute one for
the other; only the internals diverge.

Structural differences from the reference (executor) implementation:

1. **Composition over a monolithic class.** ``DatabaseManager`` delegates
   to two collaborators: a :class:`_WriteGate` that owns the single
   read-write connection and a :class:`_ReadPool` that maintains a bounded
   pool of read cursors.  The reference uses one class with a
   ``_read_only_conn()`` helper that opens and closes a fresh connection
   per query.

2. **``threading.RLock`` (reentrant) instead of ``threading.Lock``.**  This
   lets a public write method call another write method without
   re-acquiring the lock and deadlocking.

3. **A cursor pool instead of per-query connections.**  Reads borrow a
   cursor from a bounded :class:`_ReadPool` (cursors are derived from the
   single read-write instance via ``cursor()``) and return it when done,
   avoiding the open/close cost of the reference's ``_read_only_conn()``
   helper.  Pooling cursors is also required on DuckDB 1.4+, which rejects
   opening a ``read_only=True`` connection to a file that already has an
   open read-write connection.

4. **Read-only enforcement at the SQL layer, not the connection layer.**
   Because pooled cursors share the read-write instance, ``execute_query``
   rejects any statement that does not begin with ``SELECT`` or ``WITH``
   (case-insensitive) before it reaches DuckDB.  This is defense-in-depth
   -- the same principle the Flight server applies to ``do_get`` -- and is
   structurally different from the reference, which relies on DuckDB's
   ``read_only=True`` connection mode.

5. **``INSERT ... ON CONFLICT (Date, Ticker) DO UPDATE`` instead of
   ``MERGE INTO ... WHEN MATCHED / WHEN NOT MATCHED``** for the upsert
   path.  Both produce the same upsert semantics against the
   ``PRIMARY KEY (Date, Ticker)`` constraint.

6. **Named SQL parameters** (``$ticker``, ``$split_date`` ...) instead of
   positional ``?`` placeholders, so parameter order cannot be confused.

7. **Context-manager protocol** (``__enter__`` / ``__exit__``) plus a
   ``__del__`` safety net, so the manager can be used as ``with
   DatabaseManager(path) as db: ...`` and is cleaned up even if
   ``close()`` is forgotten.
"""

from __future__ import annotations

import logging
import queue
import threading
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import duckdb
import pandas as pd
import pandas_market_calendars as mcal
import pyarrow as pa

logger = logging.getLogger("MarketDataPipeline")

SCHEMA_VERSION = 5

# Maximum number of read cursors the pool will keep alive at once.
# Additional concurrent readers block until a cursor is returned.
_READ_POOL_CAP = 8


# ---------------------------------------------------------------------------
# Internal collaborators
# ---------------------------------------------------------------------------


class _WriteGate:
    """Owns the single read-write DuckDB connection, guarded by an RLock.

    All write operations (DDL, upsert, delete, split recording, CHECKPOINT)
    go through :meth:`connection` so they are serialized with respect to
    each other.  Read operations borrow a cursor via :meth:`new_cursor`
    and run concurrently without acquiring this lock.
    """

    def __init__(self, db_path: str) -> None:
        self._lock = threading.RLock()
        self._conn = duckdb.connect(db_path, read_only=False)

    @property
    def lock(self) -> threading.RLock:
        """The reentrant write lock (exposed for close ordering)."""
        return self._lock

    @contextmanager
    def connection(self):
        """Acquire the write lock and yield the read-write connection."""
        with self._lock:
            yield self._conn

    def new_cursor(self) -> duckdb.DuckDBPyConnection:
        """Return a new cursor sharing the read-write instance (for reads)."""
        return self._conn.cursor()

    def close(self) -> None:
        with self._lock:
            try:
                self._conn.execute("CHECKPOINT;")
                logger.info("DuckDB storage gracefully detached.")
            except Exception as exc:  # noqa: BLE001 - best-effort on shutdown
                logger.error(f"Error executing CHECKPOINT on close: {exc}")
            finally:
                self._conn.close()


class _ReadPool:
    """A bounded pool of read cursors, reused across queries.

    Cursors are created lazily from a caller-supplied factory (a cursor on
    the shared read-write instance) up to ``cap``; once the cap is reached,
    callers block on :meth:`read` until a cursor is returned.  This avoids
    the per-query connection open/close cost of the reference
    implementation while still allowing many concurrent readers.
    """

    def __init__(self, cursor_factory: Callable[[], duckdb.DuckDBPyConnection],
                 cap: int = _READ_POOL_CAP) -> None:
        self._factory = cursor_factory
        self._cap = cap
        self._idle: "queue.Queue[duckdb.DuckDBPyConnection]" = queue.Queue()
        self._created = 0
        self._guard = threading.Lock()

    def _acquire(self) -> duckdb.DuckDBPyConnection:
        # Fast path: an idle cursor is immediately available.
        try:
            return self._idle.get_nowait()
        except queue.Empty:
            pass
        # Slow path: maybe create a new one, otherwise block for one.
        with self._guard:
            if self._created < self._cap:
                self._created += 1
                return self._factory()
        # At cap: block until another reader returns a cursor.
        return self._idle.get()

    def _release(self, conn: duckdb.DuckDBPyConnection) -> None:
        self._idle.put(conn)

    @contextmanager
    def read(self):
        """Borrow a read cursor for the duration of the block."""
        conn = self._acquire()
        try:
            yield conn
        finally:
            self._release(conn)

    def close(self) -> None:
        with self._guard:
            while True:
                try:
                    conn = self._idle.get_nowait()
                except queue.Empty:
                    break
                conn.close()
                self._created -= 1


# ---------------------------------------------------------------------------
# Schema DDL (kept as module-level constants so the critic can introspect)
# ---------------------------------------------------------------------------

_DAILY_CLOSE_DDL = """
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
"""

_SPLITS_PROCESSED_DDL = """
CREATE TABLE IF NOT EXISTS splits_processed (
    Ticker VARCHAR NOT NULL,
    SplitDate DATE NOT NULL,
    SplitFactor DOUBLE NOT NULL,
    ProcessedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (Ticker, SplitDate)
);
"""

_SCHEMA_VERSION_DDL = """
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER NOT NULL,
    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

_COMPANY_METADATA_DDL = """
CREATE TABLE IF NOT EXISTS company_metadata (
    ticker VARCHAR PRIMARY KEY,
    company_name VARCHAR,
    sector VARCHAR,
    industry VARCHAR,
    quote_type VARCHAR
);
"""

# Arrow staging schema for upsert_company_metadata — explicit so all-NULL
# columns still land as VARCHAR (from_pylist would infer null type).
_COMPANY_METADATA_ARROW_SCHEMA = pa.schema(
    [
        pa.field("ticker", pa.string()),
        pa.field("company_name", pa.string()),
        pa.field("sector", pa.string()),
        pa.field("industry", pa.string()),
        pa.field("quote_type", pa.string()),
    ]
)

# ON CONFLICT refresh — structurally different from MERGE INTO (diff #5).
_COMPANY_METADATA_UPSERT_SQL = """
INSERT INTO company_metadata
    (ticker, company_name, sector, industry, quote_type)
SELECT ticker, company_name, sector, industry, quote_type
FROM staging_metadata
ON CONFLICT (ticker) DO UPDATE SET
    company_name = excluded.company_name,
    sector       = excluded.sector,
    industry     = excluded.industry,
    quote_type   = excluded.quote_type;
"""

# Upsert via INSERT ... ON CONFLICT (structurally different from MERGE INTO).
_UPSERT_SQL = """
INSERT INTO daily_close (Date, Ticker, Open, High, Low, Close, Volume)
SELECT Date, Ticker, Open, High, Low, Close, Volume FROM staging_df
ON CONFLICT (Date, Ticker) DO UPDATE SET
    Open   = excluded.Open,
    High   = excluded.High,
    Low    = excluded.Low,
    Close  = excluded.Close,
    Volume = excluded.Volume;
"""

# ---------------------------------------------------------------------------
# Fear & Greed history (F16-T1) — CNN fearandgreed/graphdata payload.
# The 9 sub-indicator payload keys below are the LIVE API names (verified
# 2026-09-20); the reference doc docs/enhancements/2260920.md lists stale
# names (stock_price_momentum / market_volatility) that do not exist.
# ---------------------------------------------------------------------------

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
# except the ``ingested_at`` DEFAULT.
FEAR_GREED_UPSERT_COLUMNS: Tuple[str, ...] = (
    ("date", "fg_score", "fg_rating")
    + tuple(
        col for key in FEAR_GREED_SUB_INDICATORS for col in (key, f"{key}_rating")
    )
)

_FEAR_GREED_HISTORY_DDL = """
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
"""

# Arrow staging schema for upsert_fear_greed — explicit so all-NULL
# rating columns still land as VARCHAR (from_pylist would infer null
# type), mirroring _COMPANY_METADATA_ARROW_SCHEMA.
_FEAR_GREED_ARROW_SCHEMA = pa.schema(
    [pa.field("date", pa.date32()), pa.field("fg_score", pa.float64()),
     pa.field("fg_rating", pa.string())]
    + [
        pa.field(col, pa.float64() if col == key else pa.string())
        for key in FEAR_GREED_SUB_INDICATORS
        for col in (key, f"{key}_rating")
    ]
)

# ON CONFLICT refresh on the date PK — same idiom as
# _COMPANY_METADATA_UPSERT_SQL; ingested_at is stamped on every write.
_FEAR_GREED_UPSERT_SQL = (
    "INSERT INTO fear_greed_history ("
    + ", ".join(FEAR_GREED_UPSERT_COLUMNS)
    + ")\nSELECT "
    + ", ".join(FEAR_GREED_UPSERT_COLUMNS)
    + " FROM staging_fear_greed\nON CONFLICT (date) DO UPDATE SET\n    "
    + ",\n    ".join(
        f"{c} = excluded.{c}" for c in FEAR_GREED_UPSERT_COLUMNS if c != "date"
    )
    + ",\n    ingested_at = now();"
)

# ---------------------------------------------------------------------------
# Weekly layer (F12-T1) — verbatim ports of
# libs/vw_pipe_data_duckdb.py::aggregate_to_weekly (:753-800, the live
# Friday-dated variant) and ::update_weekly_indicators_01 (:317-397).
# Volume is DOUBLE to match our daily_close (legacy used BIGINT; same values).
# ---------------------------------------------------------------------------

_WEEKLY_CLOSE_DDL = """
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
"""

_WEEKLY_INDICATORS_DDL = """
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
"""

# Nominal-Friday week key: date_trunc('week', Date) gives the ISO Monday;
# +4 days gives the Friday.  arg_min/arg_max carry first-Open/last-Close.
_WEEKLY_CLOSE_SQL = """
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
    Volume = excluded.Volume;
"""

# update_weekly_indicators_01 column set under the base weekly_indicators
# name — INSERT OR REPLACE over the 7-stage CTE chain.
_WEEKLY_INDICATORS_SQL = """
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
        LN(Close / NULLIF(ema18, 0)) as log_ema18, LN(Close / NULLIF(ema40, 0)) as log_ema40
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


# ---------------------------------------------------------------------------
# Data-health SQL (F9-T5) — module-level constants, mirroring the DDL
# convention.  Named parameters ($name) per this variant's style.
# ---------------------------------------------------------------------------

_STALENESS_SQL = """
SELECT Ticker, CAST(MAX(Date) AS VARCHAR) AS last_date
FROM daily_close
GROUP BY Ticker
HAVING MAX(Date) < CAST($latest AS DATE)
ORDER BY Ticker;
"""

_COVERAGE_SQL = """
SELECT c.ticker
FROM (SELECT unnest($tickers) AS ticker) AS c
WHERE c.ticker NOT IN (SELECT Ticker FROM daily_close)
ORDER BY c.ticker;
"""

_INTEGRITY_COUNTS_SQL = """
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
FROM daily_close;
"""

_INTEGRITY_SAMPLES_SQL = """
SELECT Ticker, CAST(Date AS VARCHAR) AS d, reason
FROM (
    SELECT Ticker, Date, 'null_or_nonpositive_ohlc' AS reason
    FROM daily_close
    WHERE Open IS NULL OR Open <= 0 OR High IS NULL OR High <= 0 OR
          Low IS NULL OR Low <= 0 OR Close IS NULL OR Close <= 0
    UNION ALL
    SELECT Ticker, Date, 'high_lt_low' FROM daily_close WHERE High < Low
    UNION ALL
    SELECT Ticker, Date, 'negative_volume' FROM daily_close WHERE Volume < 0
) v
ORDER BY Ticker, d
LIMIT $max_items;
"""

_DUPLICATES_SQL = """
SELECT Ticker, CAST(Date AS VARCHAR) AS d, COUNT(*) AS n
FROM daily_close
GROUP BY Ticker, Date
HAVING COUNT(*) > 1
ORDER BY Ticker, d;
"""

# {universe} is filled with one of the two constants below — the table
# expression itself is static SQL, never caller input.
_GAPS_SQL_TEMPLATE = """
WITH cal AS (
    SELECT CAST(u AS DATE) AS day
    FROM (SELECT unnest($cal) AS u) s
),
win AS (
    SELECT Ticker, Date FROM daily_close
    WHERE Date BETWEEN (SELECT MIN(day) FROM cal) AND (SELECT MAX(day) FROM cal)
),
universe AS ({universe})
SELECT t.Ticker,
       COUNT(*) AS missing_count,
       LIST(CAST(c.day AS VARCHAR) ORDER BY c.day) AS missing_days
FROM universe t
CROSS JOIN cal c
LEFT JOIN win w ON w.Ticker = t.Ticker AND w.Date = c.day
WHERE w.Date IS NULL
GROUP BY t.Ticker
ORDER BY t.Ticker;
"""

_GAP_UNIVERSE_ALL = "SELECT DISTINCT Ticker FROM daily_close"
_GAP_UNIVERSE_CONFIGURED = (
    "SELECT DISTINCT Ticker FROM daily_close "
    "WHERE Ticker IN (SELECT unnest($tickers) AS u)"
)

# ---------------------------------------------------------------------------
# F17-T1 data-health SQL — the new-table checks (fear_greed_history,
# weekly_close/weekly_indicators, company_metadata).  Named parameters
# ($name) per this variant's style.
# ---------------------------------------------------------------------------

# Valid fg_rating vocabulary (CNN lowercase labels).  NULL is allowed —
# the list only flags non-NULL outliers.  Kept as a Python tuple so the
# two queries below share one definition.
FEAR_GREED_VALID_RATINGS: Tuple[str, ...] = (
    "extreme fear",
    "fear",
    "neutral",
    "greed",
    "extreme greed",
)

_FEAR_GREED_RATINGS_SQL = ", ".join(
    f"'{r}'" for r in FEAR_GREED_VALID_RATINGS
)

_FG_SUMMARY_SQL = """
SELECT COUNT(*) AS n,
       CAST(MAX(date) AS VARCHAR) AS latest_date
FROM fear_greed_history;
"""

_FG_INTEGRITY_SQL = f"""
SELECT
    COUNT(*) FILTER (fg_score IS NULL) AS null_score,
    COUNT(*) FILTER (fg_score < 0 OR fg_score > 100) AS out_of_range,
    COUNT(*) FILTER (
        fg_rating IS NOT NULL
        AND fg_rating NOT IN ({_FEAR_GREED_RATINGS_SQL})
    ) AS bad_rating
FROM fear_greed_history;
"""

_FG_OUT_OF_RANGE_SAMPLES_SQL = """
SELECT CAST(date AS VARCHAR) AS d, fg_score
FROM fear_greed_history
WHERE fg_score < 0 OR fg_score > 100
ORDER BY d
LIMIT $max_items;
"""

_FG_BAD_RATING_SAMPLES_SQL = f"""
SELECT CAST(date AS VARCHAR) AS d, fg_rating
FROM fear_greed_history
WHERE fg_rating IS NOT NULL
  AND fg_rating NOT IN ({_FEAR_GREED_RATINGS_SQL})
ORDER BY d
LIMIT $max_items;
"""

# COUNT(<key>) counts non-NULL cells; 0 = a fully-NULL series.  The
# column list is built from the FEAR_GREED_SUB_INDICATORS constant —
# never caller input.
_FG_SERIES_COUNTS_SQL = (
    "SELECT "
    + ", ".join(f"COUNT({k})" for k in FEAR_GREED_SUB_INDICATORS)
    + " FROM fear_greed_history;"
)

# Nominal-Friday week key (F12): date_trunc('week', d)::DATE + 4 of the
# week containing the latest ended NYSE session.
_WEEKLY_SUMMARY_SQL = """
SELECT
    CAST((SELECT MAX(Date) FROM weekly_close) AS VARCHAR) AS latest_week,
    CAST(date_trunc('week', CAST($latest AS DATE))::DATE + 4
         AS VARCHAR) AS expected_week,
    (SELECT COUNT(*) FROM weekly_close) AS close_rows,
    (SELECT COUNT(*) FROM weekly_indicators) AS indicator_rows;
"""

_WEEKLY_MISSING_KEY_COUNT_SQL = """
SELECT COUNT(*) FROM weekly_close w
WHERE NOT EXISTS (
    SELECT 1 FROM weekly_indicators i
    WHERE i.Date = w.Date AND i.Ticker = w.Ticker
);
"""

_WEEKLY_MISSING_KEY_SAMPLES_SQL = """
SELECT CAST(w.Date AS VARCHAR) AS d, w.Ticker
FROM weekly_close w
WHERE NOT EXISTS (
    SELECT 1 FROM weekly_indicators i
    WHERE i.Date = w.Date AND i.Ticker = w.Ticker
)
ORDER BY d, w.Ticker
LIMIT $max_items;
"""

_WEEKLY_STALE_TICKERS_SQL = """
SELECT DISTINCT Ticker FROM weekly_close
WHERE Ticker NOT IN (SELECT Ticker FROM daily_close)
ORDER BY Ticker;
"""

_WEEKLY_INTEGRITY_SQL = """
SELECT
    COUNT(*) FILTER (
        Open IS NULL OR High IS NULL OR
        Low IS NULL OR Close IS NULL
    ) AS null_ohlc,
    COUNT(*) FILTER (Volume < 0) AS negative_volume
FROM weekly_close;
"""

_METADATA_SUMMARY_SQL = """
SELECT
    COUNT(*) AS n,
    COUNT(*) FILTER (
        company_name IS NULL AND sector IS NULL AND
        industry IS NULL AND quote_type IS NULL
    ) AS null_field_rows
FROM company_metadata;
"""

_METADATA_COVERAGE_SQL = """
SELECT c.ticker
FROM (SELECT unnest($tickers) AS ticker) AS c
WHERE c.ticker NOT IN (SELECT ticker FROM company_metadata)
ORDER BY c.ticker;
"""

# Cap on every per-ticker/per-day list embedded in the health report so a
# badly damaged table cannot produce an unbounded payload.
_HEALTH_MAX_ITEMS = 50


def _coerce_fg_cell(value: Any) -> Any:
    """Normalize one ``fear_greed_history`` cell for Arrow staging.

    NaN/NaT become ``None`` (SQL NULL) and ``pd.Timestamp``/``datetime``
    become ``datetime.date`` so ``pa.Table.from_pylist`` binds the
    explicit ``_FEAR_GREED_ARROW_SCHEMA`` types.
    """
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.date()
    return value


def _validate_select_only(sql: str) -> None:
    """Reject statements that are not SELECT or WITH (CTE) queries.

    Pooled read cursors share the read-write instance, so read-only
    enforcement is applied at the SQL layer as defense-in-depth (the same
    principle the Flight server uses for ``do_get``).
    """
    stripped = sql.strip().upper()
    if not stripped.startswith("SELECT") and not stripped.startswith("WITH"):
        raise ValueError(
            "execute_query only accepts SELECT and WITH (CTE) statements"
        )


# ---------------------------------------------------------------------------
# Public DatabaseManager
# ---------------------------------------------------------------------------


class DatabaseManager:
    """Thread-safe DuckDB manager with concurrent read access.

    Writes are serialized through an internal :class:`_WriteGate`
    (reentrant lock); reads run concurrently through an internal
    :class:`_ReadPool` of cursors.  Read-only access is enforced by
    validating that ``execute_query`` statements begin with SELECT or WITH.
    """

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._write = _WriteGate(db_path)
        self._reads = _ReadPool(self._write.new_cursor)
        self._closed = False
        self._init_schema()

    # -- schema -------------------------------------------------------------

    def _init_schema(self) -> None:
        with self._write.connection() as conn:
            # --- Schema migration: v1 -> v2 (rename daily_quotes -> daily_close) ---
            try:
                row = conn.execute(
                    "SELECT MAX(version) FROM schema_version"
                ).fetchone()
                current_version = row[0] if row and row[0] is not None else 0
            except duckdb.Error:
                current_version = 0
            if current_version == 1:
                # Check if the old daily_quotes table still exists
                tables = {
                    r[0]
                    for r in conn.execute(
                        "SELECT table_name FROM information_schema.tables"
                    ).fetchall()
                }
                if "daily_quotes" in tables:
                    conn.execute(
                        "ALTER TABLE daily_quotes RENAME TO daily_close"
                    )
                conn.execute(
                    "INSERT INTO schema_version (version) VALUES ($version)",
                    {"version": SCHEMA_VERSION},
                )
            conn.execute(_DAILY_CLOSE_DDL)
            conn.execute(_SPLITS_PROCESSED_DDL)
            conn.execute(_SCHEMA_VERSION_DDL)
            conn.execute(_COMPANY_METADATA_DDL)
            # F12-T1: weekly layer (Friday-dated weekly_close + the
            # weekly_indicators _01 column set).
            conn.execute(_WEEKLY_CLOSE_DDL)
            conn.execute(_WEEKLY_INDICATORS_DDL)
            # F16-T1: CNN Fear & Greed history table.
            conn.execute(_FEAR_GREED_HISTORY_DDL)
            conn.execute(
                "INSERT INTO schema_version (version) "
                "SELECT $version WHERE NOT EXISTS ("
                "  SELECT 1 FROM schema_version WHERE version = $version"
                ");",
                {"version": SCHEMA_VERSION},
            )

    # -- reads --------------------------------------------------------------

    def execute_query(self, query: str) -> pa.Table:
        """Execute a read-only SQL query and return an Arrow table.

        Only statements beginning with ``SELECT`` or ``WITH`` are
        accepted; anything else raises ``ValueError`` before reaching
        DuckDB.
        """
        _validate_select_only(query)
        with self._reads.read() as conn:
            result = conn.execute(query).arrow()
        # DuckDB 1.4+ returns a RecordBatchReader from .arrow(); older
        # versions return a pa.Table directly.  Normalize to pa.Table.
        if isinstance(result, pa.RecordBatchReader):
            result = result.read_all()
        return result

    def get_existing_tickers(self) -> Set[str]:
        with self._reads.read() as conn:
            rows = conn.execute("SELECT DISTINCT Ticker FROM daily_close").fetchall()
        return {row[0] for row in rows}

    def get_aligned_start_date(self) -> Optional[str]:
        """Earliest ``Date`` across all tickers as an ISO string, or ``None``."""
        with self._reads.read() as conn:
            result = conn.execute("SELECT MIN(Date) FROM daily_close").fetchone()
        if result and result[0]:
            return str(result[0])
        return None

    def is_split_processed(self, ticker: str, split_date: str) -> bool:
        with self._reads.read() as conn:
            row = conn.execute(
                "SELECT 1 FROM splits_processed "
                "WHERE Ticker = $ticker AND SplitDate = $split_date",
                {"ticker": ticker, "split_date": split_date},
            ).fetchone()
        return row is not None

    def generate_top_20_dollar_volume(
        self, target_date: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        with self._reads.read() as conn:
            if target_date:
                rel = conn.execute(
                    "SELECT Ticker, CAST(Date AS VARCHAR) AS Date, "
                    "       Close, Volume, "
                    "       (Close * Volume) AS dollar_volume "
                    "FROM daily_close "
                    "WHERE Date = $target_date "
                    "ORDER BY dollar_volume DESC "
                    "LIMIT 20;",
                    {"target_date": target_date},
                )
            else:
                rel = conn.execute(
                    "SELECT Ticker, CAST(Date AS VARCHAR) AS Date, "
                    "       Close, Volume, "
                    "       (Close * Volume) AS dollar_volume "
                    "FROM daily_close "
                    "WHERE Date = (SELECT MAX(Date) FROM daily_close) "
                    "ORDER BY dollar_volume DESC "
                    "LIMIT 20;"
                )
            cols = [desc[0] for desc in rel.description]
            rows = rel.fetchall()
        # Date already comes back as a string via SQL CAST — no
        # Python stringify loop needed (SQL > loop hierarchy).
        result: List[Dict[str, Any]] = [dict(zip(cols, row)) for row in rows]
        return result

    def detect_new_tickers(self, configured_tickers: List[str]) -> List[str]:
        existing = self.get_existing_tickers()
        return [t for t in configured_tickers if t not in existing]

    # -- company metadata (F10-T1) -------------------------------------

    def get_missing_metadata_tickers(self, tickers: List[str]) -> List[str]:
        """Entries of ``tickers`` absent from ``company_metadata``.

        SQL anti-join against ``company_metadata`` (NOT ``daily_close``);
        the result preserves the input order so callers fetch in
        configured order.
        """
        if not tickers:
            return []
        with self._reads.read() as conn:
            rows = conn.execute(
                "SELECT c.ticker "
                "FROM (SELECT unnest($tickers) AS ticker) AS c "
                "WHERE c.ticker NOT IN "
                "(SELECT ticker FROM company_metadata)",
                {"tickers": list(tickers)},
            ).fetchall()
        missing = {row[0] for row in rows}
        return [t for t in tickers if t in missing]

    def upsert_company_metadata(self, rows: List[Dict[str, Any]]) -> int:
        """Upsert ``company_metadata`` rows; return the count staged.

        ``rows`` is a list of dicts keyed ``ticker`` / ``company_name`` /
        ``sector`` / ``industry`` / ``quote_type``.  Staged through an
        Arrow table (sidesteps the pandas ``str``-dtype limitation) and
        upserted via ``ON CONFLICT (ticker) DO UPDATE``.
        """
        if not rows:
            return 0
        arrow_table = pa.Table.from_pylist(
            rows, schema=_COMPANY_METADATA_ARROW_SCHEMA
        )
        with self._write.connection() as conn:
            conn.register("staging_metadata", arrow_table)
            try:
                conn.execute(_COMPANY_METADATA_UPSERT_SQL)
            finally:
                conn.unregister("staging_metadata")
        return len(rows)

    # -- fear & greed history (F16-T1) ---------------------------------

    def upsert_fear_greed(self, df: pd.DataFrame) -> int:
        """Upsert a wide Fear & Greed frame into ``fear_greed_history``.

        ``df`` carries exactly ``FEAR_GREED_UPSERT_COLUMNS`` (``date``
        plus ``fg_score``/``fg_rating`` and each sub-indicator's
        ``<key>``/``<key>_rating`` pair; absent values are NULL).
        Staged through an Arrow table with an explicit schema — like
        ``upsert_company_metadata`` this sidesteps the pandas ``str``
        dtype limitation and guarantees all-NULL rating columns still
        bind as VARCHAR.  ``INSERT ... ON CONFLICT (date) DO UPDATE``
        updates changed values in place (no duplicates on re-run) and
        re-stamps ``ingested_at``.  Returns the row count staged.
        """
        if df is None or df.empty:
            return 0
        records = df.loc[:, list(FEAR_GREED_UPSERT_COLUMNS)].to_dict(
            "records"
        )
        # Normalize each record: NaN/NaT -> None, Timestamp/datetime ->
        # datetime.date, so from_pylist binds clean DATE/DOUBLE/VARCHAR.
        cleaned = [
            {k: _coerce_fg_cell(v) for k, v in row.items()}
            for row in records
        ]
        arrow_table = pa.Table.from_pylist(
            cleaned, schema=_FEAR_GREED_ARROW_SCHEMA
        )
        with self._write.connection() as conn:
            conn.register("staging_fear_greed", arrow_table)
            try:
                conn.execute(_FEAR_GREED_UPSERT_SQL)
            finally:
                conn.unregister("staging_fear_greed")
        return len(records)

    # -- weekly layer (F12-T1) ------------------------------------------
    # Verbatim SQL ports of libs/vw_pipe_data_duckdb.py
    # aggregate_to_weekly (:753-800) and update_weekly_indicators_01
    # (:317-397); module-level constants per this variant's convention.

    def rebuild_weekly_close(self) -> int:
        """Rebuild ``weekly_close`` from ``daily_close``; return row count.

        Nominal-Friday week key ``(date_trunc('week', Date)::DATE + 4)``;
        ``arg_min(Open, Date)`` / ``arg_max(Close, Date)`` /
        ``max(High)`` / ``min(Low)`` / ``sum(Volume)``; idempotent via
        ``ON CONFLICT (Date, Ticker) DO UPDATE``.
        """
        with self._write.connection() as conn:
            conn.execute(_WEEKLY_CLOSE_DDL)
            conn.execute(_WEEKLY_CLOSE_SQL)
            count = conn.execute(
                "SELECT COUNT(*) FROM weekly_close"
            ).fetchone()[0]
        logger.info(
            f"Weekly aggregation complete. Total weekly records: {count}"
        )
        return int(count)

    def rebuild_weekly_indicators(self) -> int:
        """Rebuild ``weekly_indicators`` from ``weekly_close``; return row
        count.  ``INSERT OR REPLACE`` over the _01 CTE chain (stochastic
        incl. ``Slow_D_80_20_Crossover``, Bollinger, PPO, Force Index,
        log-EMAs)."""
        with self._write.connection() as conn:
            conn.execute(_WEEKLY_INDICATORS_DDL)
            conn.execute(_WEEKLY_INDICATORS_SQL)
            count = conn.execute(
                "SELECT COUNT(*) FROM weekly_indicators"
            ).fetchone()[0]
        logger.info(
            f"Weekly indicators updated. Total indicator records: {count}"
        )
        return int(count)

    # -- data-health checks (F9-T5; report-only — never repairs) --------

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

        All checks are SQL-driven (SQL > vectorized > loop); the queries
        run on a pooled read cursor and never mutate state.  The returned
        dict carries ``health_status`` in ``{'OK', 'WARN',
        'CHECK_FAILED'}`` plus per-check counts and bounded per-ticker
        lists (first ``max_items``, with totals).
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

            with self._reads.read() as conn:
                # (a) STALENESS — per-ticker MAX(Date) older than the
                # latest NYSE trading day.
                stale_rows = conn.execute(
                    _STALENESS_SQL, {"latest": latest_trading_day}
                ).fetchall()

                # (b) COVERAGE — configured tickers absent from
                # daily_close (SQL anti-join via unnest).
                if tickers is not None:
                    missing_rows = conn.execute(
                        _COVERAGE_SQL, {"tickers": list(tickers)}
                    ).fetchall()
                else:
                    missing_rows = []

                # (c) INTEGRITY — NULL/non-positive OHLC, High < Low,
                # Volume < 0.  NULLs in High/Low make the comparison NULL
                # (not counted there); they are already flagged by the
                # OHLC term.
                integrity_row = conn.execute(_INTEGRITY_COUNTS_SQL).fetchone()
                integrity_samples = conn.execute(
                    _INTEGRITY_SAMPLES_SQL, {"max_items": max_items}
                ).fetchall()

                # (d) DUPLICATE (Ticker, Date) keys — defensive; the
                # PRIMARY KEY (Date, Ticker) normally prevents these.
                dup_rows = conn.execute(_DUPLICATES_SQL).fetchall()

                # (e) GAPS — full mode only: per-ticker missing NYSE
                # sessions inside the trailing gap_days-session window
                # (SQL anti-join vs the generated calendar).
                gap_rows: List[Tuple] = []
                window: List[str] = []
                if include_gaps:
                    window = sessions[-gap_days:]
                    if tickers is not None:
                        gap_sql = _GAPS_SQL_TEMPLATE.format(
                            universe=_GAP_UNIVERSE_CONFIGURED
                        )
                        gap_params = {
                            "cal": window,
                            "tickers": list(tickers),
                        }
                    else:
                        gap_sql = _GAPS_SQL_TEMPLATE.format(
                            universe=_GAP_UNIVERSE_ALL
                        )
                        gap_params = {"cal": window}
                    gap_rows = conn.execute(gap_sql, gap_params).fetchall()
        except Exception as exc:
            logger.error(f"daily_close health check failed: {exc}")
            return {
                "health_status": "CHECK_FAILED",
                "mode": mode,
                "as_of": as_of,
                "error": str(exc),
            }

        violation_count = int(integrity_row[3]) if integrity_row else 0
        total_missing_days = sum(int(r[1]) for r in gap_rows)
        findings = (
            len(stale_rows)
            + len(missing_rows)
            + violation_count
            + len(dup_rows)
            + total_missing_days
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

    # -- new-table health checks (F17-T1; report-only — never repairs) --
    # Staleness reuses the same "latest ended NYSE session" machinery as
    # check_daily_close_health (F9-T5/F9-T7).

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

        SQL-driven like ``check_daily_close_health`` — the queries run
        on a pooled read cursor and never mutate state; the method never
        raises — any failure returns ``health_status == 'CHECK_FAILED'``
        with an ``error`` field.  ``enabled=False``
        (``cfg.fear_greed.enabled``) returns ``health_status ==
        'DISABLED'`` immediately, which the composite rollup excludes.

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

            with self._reads.read() as conn:
                summary = conn.execute(_FG_SUMMARY_SQL).fetchone()
                integrity = conn.execute(_FG_INTEGRITY_SQL).fetchone()
                oor_samples = conn.execute(
                    _FG_OUT_OF_RANGE_SAMPLES_SQL, {"max_items": max_items}
                ).fetchall()
                rating_samples = conn.execute(
                    _FG_BAD_RATING_SAMPLES_SQL, {"max_items": max_items}
                ).fetchall()
                series_counts = conn.execute(_FG_SERIES_COUNTS_SQL).fetchone()
        except Exception as exc:
            logger.error(f"fear_greed health check failed: {exc}")
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

        SQL-driven on pooled read cursors; never raises.  Checks:

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

            with self._reads.read() as conn:
                summary = conn.execute(
                    _WEEKLY_SUMMARY_SQL, {"latest": latest_trading_day}
                ).fetchone()
                missing_key_count = conn.execute(
                    _WEEKLY_MISSING_KEY_COUNT_SQL
                ).fetchone()
                missing_key_samples = conn.execute(
                    _WEEKLY_MISSING_KEY_SAMPLES_SQL,
                    {"max_items": max_items},
                ).fetchall()
                stale_ticker_rows = conn.execute(
                    _WEEKLY_STALE_TICKERS_SQL
                ).fetchall()
                integrity = conn.execute(_WEEKLY_INTEGRITY_SQL).fetchone()
        except Exception as exc:
            logger.error(f"weekly health check failed: {exc}")
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

        SQL-driven on a pooled read cursor; never raises.  Checks:

        * **coverage** — configured-minus-excluded ``tickers`` absent
          from ``company_metadata`` -> WARN (bounded list).  Skipped
          when ``tickers`` is None.
        * **null fields** — rows where all four value columns
          (``company_name``/``sector``/``industry``/``quote_type``) are
          NULL.  These are legitimate placeholders for unknown symbols
          (F10-T1) — informational count only, never a finding.
        """
        try:
            with self._reads.read() as conn:
                summary = conn.execute(_METADATA_SUMMARY_SQL).fetchone()
                if tickers is not None:
                    missing_rows = conn.execute(
                        _METADATA_COVERAGE_SQL,
                        {"tickers": list(tickers)},
                    ).fetchall()
                else:
                    missing_rows = []
        except Exception as exc:
            logger.error(f"company_metadata health check failed: {exc}")
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

    # -- writes -------------------------------------------------------------

    def upsert_dataframe(self, df: pd.DataFrame) -> int:
        """Upsert a dataframe into ``daily_close``; return rows staged.

        The dataframe is converted to a pyarrow Table before being
        registered as the ``staging_df`` view.  Going through Arrow (rather
        than registering the pandas DataFrame directly) sidesteps pandas
        dtype incompatibilities -- e.g. pandas 3.0's ``str`` dtype is not
        recognized by DuckDB's pandas registration path -- and lets DuckDB
        infer column types from the explicit Arrow schema.
        """
        if df.empty:
            return 0
        arrow_table = pa.Table.from_pandas(df, preserve_index=False)
        with self._write.connection() as conn:
            conn.register("staging_df", arrow_table)
            try:
                conn.execute(_UPSERT_SQL)
            finally:
                conn.unregister("staging_df")
        return len(df)

    def purge_ticker(self, ticker: str) -> None:
        with self._write.connection() as conn:
            conn.execute(
                "DELETE FROM daily_close WHERE Ticker = $ticker",
                {"ticker": ticker},
            )
        logger.critical(
            f"RUNTIME EVENT: Delisted ticker data completely purged for '{ticker}'."
        )

    def purge_tickers(self, tickers: List[str]) -> int:
        """Bulk-purge multiple tickers in a single SQL DELETE.

        Returns the number of rows deleted.
        """
        if not tickers:
            return 0
        placeholders = ", ".join(f"$t{i}" for i in range(len(tickers)))
        params = {f"t{i}": t for i, t in enumerate(tickers)}
        with self._write.connection() as conn:
            result = conn.execute(
                f"DELETE FROM daily_close WHERE Ticker IN ({placeholders})",
                params,
            )
            count = result.fetchone()[0] if result else 0
        logger.critical(
            f"RUNTIME EVENT: Bulk-purged {count} row(s) for tickers {tickers}."
        )
        return count

    def record_split(self, ticker: str, split_date: str, split_factor: float) -> None:
        """Record a split in ``splits_processed`` (idempotent)."""
        with self._write.connection() as conn:
            conn.execute(
                "INSERT INTO splits_processed (Ticker, SplitDate, SplitFactor) "
                "VALUES ($ticker, $split_date, $factor) "
                "ON CONFLICT (Ticker, SplitDate) DO NOTHING;",
                {"ticker": ticker, "split_date": split_date, "factor": split_factor},
            )

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
        # Build named-parameter placeholders: ($t0_ticker, $t0_date, $t0_factor), ...
        placeholders = []
        params: Dict[str, Any] = {}
        for i, (ticker, split_date, split_factor) in enumerate(splits):
            placeholders.append(f"($t{i}_ticker, $t{i}_date, $t{i}_factor)")
            params[f"t{i}_ticker"] = ticker
            params[f"t{i}_date"] = split_date
            params[f"t{i}_factor"] = split_factor
        values_clause = ", ".join(placeholders)
        with self._write.connection() as conn:
            conn.execute(
                f"INSERT INTO splits_processed "
                f"(Ticker, SplitDate, SplitFactor) "
                f"VALUES {values_clause} "
                f"ON CONFLICT (Ticker, SplitDate) DO NOTHING;",
                params,
            )
        return len(splits)

    def delete_ticker_data(self, ticker: str) -> None:
        """Delete all rows for a ticker (used before re-download after a split)."""
        with self._write.connection() as conn:
            conn.execute(
                "DELETE FROM daily_close WHERE Ticker = $ticker",
                {"ticker": ticker},
            )

    # -- lifecycle ----------------------------------------------------------

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._reads.close()
        self._write.close()

    def __enter__(self) -> "DatabaseManager":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass


__all__ = ["DatabaseManager", "SCHEMA_VERSION"]
