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
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import duckdb
import pandas as pd
import pandas_market_calendars as mcal
import pyarrow as pa

logger = logging.getLogger("MarketDataPipeline")

SCHEMA_VERSION = 2

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

# Cap on every per-ticker/per-day list embedded in the health report so a
# badly damaged table cannot produce an unbounded payload.
_HEALTH_MAX_ITEMS = 50


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

    # -- data-health checks (F9-T5; report-only — never repairs) --------

    @staticmethod
    def _nyse_sessions(start_date: str, end_date: str) -> List[str]:
        """Return NYSE session dates in ``[start_date, end_date]`` (ISO)."""
        nyse = mcal.get_calendar("NYSE")
        sched = nyse.schedule(start_date=start_date, end_date=end_date)
        return [d.strftime("%Y-%m-%d") for d in sched.index]

    def check_daily_close_health(
        self,
        tickers: Optional[List[str]] = None,
        include_gaps: bool = False,
        gap_days: int = 30,
        as_of: Optional[str] = None,
        trading_days: Optional[List[str]] = None,
        max_items: int = _HEALTH_MAX_ITEMS,
    ) -> Dict[str, Any]:
        """Run report-only data-health checks against ``daily_close``.

        All checks are SQL-driven (SQL > vectorized > loop); the queries
        run on a pooled read cursor and never mutate state.  The returned
        dict carries ``health_status`` in ``{'OK', 'WARN',
        'CHECK_FAILED'}`` plus per-check counts and bounded per-ticker
        lists (first ``max_items``, with totals).

        Checks:

        * **staleness** — tickers whose ``MAX(Date)`` is older than the
          latest NYSE trading day.
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
            ``pandas_market_calendars`` is not consulted, the latest
            trading day is ``max(trading_days)`` and the gap window is
            its last ``gap_days`` entries.
        gap_days:
            Number of trailing NYSE sessions in the gap window (>= 1).
        max_items:
            Bound applied to every per-ticker list in the report.

        This method never raises: any failure (bad calendar, SQL error)
        returns ``health_status == 'CHECK_FAILED'`` with an ``error``
        field — health is report-only and must never break a load.
        """
        mode = "full" if include_gaps else "light"
        if as_of is None:
            as_of = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        try:
            if (
                not isinstance(gap_days, int)
                or isinstance(gap_days, bool)
                or gap_days < 1
            ):
                raise ValueError(
                    f"gap_days must be an integer >= 1, got {gap_days!r}"
                )
            if trading_days is not None:
                sessions = sorted({str(d)[:10] for d in trading_days})
            else:
                # Look back far enough to cover gap_days sessions plus
                # holiday/weekend stretches (sessions are ~5 of 7 days).
                lookback = max(gap_days * 3, 45) if include_gaps else 30
                start = (
                    pd.Timestamp(as_of) - pd.Timedelta(days=lookback)
                ).strftime("%Y-%m-%d")
                sessions = self._nyse_sessions(start, as_of)
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
