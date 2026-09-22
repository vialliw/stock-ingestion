"""Signal-input pipeline — server-side port (F24-T1).

Verbatim port of the six-step pipeline from
``quant_strategy_building/signal_input/`` (``orchestrator.py``,
``process_signal_folder_to_duckdb.py``, ``delete_older_signal_runs.py``,
``calculate_and_save_performance_03.py``, ``setup_cluster_metadata.py``)
onto a single DuckDB connection managed by the Flight service.

Architecture changes (all required by the single-writer model):

* The pipeline runs **inside** the service — ``IngestionEngine``
  calls these functions with ``self.db.conn`` under
  ``self.db._write_lock``; the standalone ``signals.duckdb`` file is
  gone and the three tables live in ``stock_ingestion.duckdb``
  (schema v7).
* ``HIST_PRICE_DB_PATH`` is replaced by in-database joins to the same
  database's ``daily_close`` table — no ``ATTACH``.
* ``LOG_FILE_PATH`` is obsolete — pruned rows and per-step counts are
  returned as JSON-serialisable dicts instead of written to a log file.
* The legacy ``df.iterrows()`` row-insert loop is vectorized into a
  single ``INSERT ... BY NAME SELECT * FROM stage ON CONFLICT DO
  NOTHING`` per file (identical dedup semantics; satisfies the
  SQL > vectorized > loop rule).

Each function returns a JSON-serialisable summary dict.
"""

from __future__ import annotations

import datetime
import logging
import os
from decimal import Decimal
from typing import Any, Dict, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)

# Exact required column set for ``*_active_signal_combined.xlsx``
# signal files (legacy ``required_headers`` — set-compared, so order in
# the file does not matter).
REQUIRED_SIGNAL_HEADERS: List[str] = [
    "Model",
    "Trading_Date",
    "Ticker",
    "Open",
    "High",
    "Low",
    "Close",
    "Cluster",
    "Hist_WinRate",
    "ATR",
    "best_k",
    "Target",
    "Current_Price",
    "Stop_Price",
    "Target_Percent",
    "Stop_Percent",
    "Reward_Risk_Ratio",
    "Shares_to_buy",
    "Max_Drawdown",
    "Centroid_Dist",
    "Drift_Warning",
]

_SIGNAL_FILE_SUFFIX = "_active_signal_combined.xlsx"

# Legacy column buckets (process_signal_folder_to_duckdb.py:131-156):
# int-coerced, str-coerced, bool-coerced; everything else float.
_INT_COLUMNS = ("Cluster", "Shares_to_buy", "Max_Drawdown")
_STR_COLUMNS = ("Model", "Ticker")
_TRUE_STRINGS = ("true", "1", "t", "y", "yes")


def _jsonable(value: Any) -> Any:
    """Convert DuckDB/Python scalar to a JSON-serialisable value.

    ``NUMERIC`` columns come back as ``Decimal`` and ``DATE`` as
    ``datetime.date`` — neither is JSON-serialisable by ``json.dumps``.
    """
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.isoformat()
    return value


def _normalize_signal_frame(df: pd.DataFrame, file_name: str) -> pd.DataFrame:
    """Vectorized equivalent of the legacy per-row value coercion.

    Reproduces the per-column conversions of the legacy ``iterrows``
    loop (``pd.isna -> None``; ``Trading_Date`` -> date;
    ``Drift_Warning`` -> bool incl. truthy strings; Cluster /
    Shares_to_buy / Max_Drawdown -> int; Model / Ticker -> str;
    everything else -> float) using pandas nullable dtypes so NaN/NA
    lands as SQL NULL in the staged insert.
    """
    stage = pd.DataFrame(index=df.index)
    # pandas 3.x 'str' dtype cannot be scanned by duckdb's register()
    # path — use object dtype (same normalization as db.upsert_dataframe).
    stage["Signal_File"] = pd.Series(
        [file_name] * len(df), index=df.index, dtype=object
    )

    # justified: per-column dtype normalization — each of the 21 signal
    # columns needs a DIFFERENT coercion (date/bool/int/str/float), so
    # the mapping is inherently column-wise; every conversion inside is
    # a vectorized pandas op over the whole column (no row iteration).
    for col in REQUIRED_SIGNAL_HEADERS:
        src = df[col]
        if col == "Trading_Date":
            dates = pd.to_datetime(src, errors="coerce").dt.date
            # NaT -> NaN inside the object column; normalize to None so
            # DuckDB scans NULL (legacy inserted None for NaN).
            stage[col] = dates.where(dates.notna(), None)
        elif col == "Drift_Warning":
            stage[col] = src.map(
                lambda v: (
                    None
                    if pd.isna(v)
                    else (
                        str(v).strip().lower() in _TRUE_STRINGS
                        if isinstance(v, str)
                        else bool(v)
                    )
                )
            ).astype("boolean")
        elif col in _INT_COLUMNS:
            stage[col] = pd.to_numeric(src, errors="coerce").astype("Int64")
        elif col in _STR_COLUMNS:
            stage[col] = src.map(
                lambda v: None if pd.isna(v) else str(v)
            ).astype(object)
        else:
            stage[col] = pd.to_numeric(src, errors="coerce").astype("Float64")
    return stage


def ingest_signal_folder(
    con, signal_folder_path: str, backdate_days: int
) -> Dict[str, Any]:
    """Ingest ``*_active_signal_combined.xlsx`` files into
    ``signal_records``.

    Port of ``process_signal_folder_to_duckdb``: scans
    ``signal_folder_path`` for files modified within ``backdate_days``
    days, validates the 21-column header set, and inserts each file's
    rows in ONE vectorized statement (``INSERT ... BY NAME SELECT *
    FROM stage ON CONFLICT DO NOTHING``) — same PK-dedup semantics as
    the legacy per-row ``RETURNING 1`` loop.  Ends with
    CHECKPOINT + VACUUM like the legacy job.

    Returns ``{status, folder_path, files_seen, files_processed,
    files_skipped, inserted, skipped_duplicates, files: [...]}``.
    """
    if not os.path.isdir(signal_folder_path):
        logger.error(
            "Signal folder path not found: %s", signal_folder_path
        )
        return {
            "status": "SKIPPED",
            "reason": f"folder not found: {signal_folder_path}",
            "folder_path": signal_folder_path,
            "files_seen": 0,
            "files_processed": 0,
            "files_skipped": 0,
            "inserted": 0,
            "skipped_duplicates": 0,
            "files": [],
        }

    # Calculate the time threshold (backdate_days days ago from now)
    n_days_ago = datetime.datetime.now() - datetime.timedelta(
        days=backdate_days
    )

    target_files = [
        (os.path.join(signal_folder_path, name), name)
        for name in os.listdir(signal_folder_path)
        if name.endswith(_SIGNAL_FILE_SUFFIX)
        and datetime.datetime.fromtimestamp(
            os.path.getmtime(os.path.join(signal_folder_path, name))
        )
        >= n_days_ago
    ]

    result: Dict[str, Any] = {
        "status": "SUCCESS",
        "folder_path": signal_folder_path,
        "files_seen": len(target_files),
        "files_processed": 0,
        "files_skipped": 0,
        "inserted": 0,
        "skipped_duplicates": 0,
        "files": [],
    }

    if not target_files:
        logger.info(
            "No signal files found matching criteria (suffix & <= %d "
            "days old) in %s.",
            backdate_days,
            signal_folder_path,
        )
        result["status"] = "NO_FILES"
        return result

    logger.info("Found %d signal file(s) to process.", len(target_files))

    insert_sql = """
        INSERT INTO signal_records BY NAME
        SELECT * FROM signal_stage
        ON CONFLICT DO NOTHING
        RETURNING 1;
    """

    # justified: file-level iteration — each xlsx is a separate
    # pd.read_excel I/O call; the ROWS inside each file are inserted
    # by a single vectorized INSERT, not a Python loop.
    for full_path, file_name in target_files:
        logger.info("Processing signal file: %s", file_name)
        file_entry: Dict[str, Any] = {"file": file_name}
        try:
            df = pd.read_excel(full_path)
            file_headers = [str(col).strip() for col in df.columns]
        except Exception as exc:
            logger.warning(
                "Error reading %s: %s. Skipping file.", file_name, exc
            )
            file_entry.update(
                {"skipped": True, "reason": f"read error: {exc}"}
            )
            result["files_skipped"] += 1
            result["files"].append(file_entry)
            continue

        if set(file_headers) != set(REQUIRED_SIGNAL_HEADERS):
            logger.warning(
                "Columns in %s do not match the signal_records "
                "structure. Skipping file.",
                file_name,
            )
            file_entry.update(
                {"skipped": True, "reason": "header mismatch"}
            )
            result["files_skipped"] += 1
            result["files"].append(file_entry)
            continue

        stage = _normalize_signal_frame(df, file_name)
        con.register("signal_stage", stage)
        try:
            inserted_rows = con.execute(insert_sql).fetchall()
        finally:
            con.unregister("signal_stage")

        file_inserted = len(inserted_rows)
        file_skipped = len(stage) - file_inserted
        file_entry.update(
            {
                "rows": len(stage),
                "inserted": file_inserted,
                "skipped_duplicates": file_skipped,
            }
        )
        result["files"].append(file_entry)
        result["files_processed"] += 1
        result["inserted"] += file_inserted
        result["skipped_duplicates"] += file_skipped
        logger.info(
            "   %s — inserted: %d, skipped (duplicates): %d",
            file_name,
            file_inserted,
            file_skipped,
        )

    # Automated database maintenance (runs after all inputs, as legacy)
    logger.info("Signal ingest maintenance: CHECKPOINT + VACUUM")
    con.execute("CHECKPOINT;")
    con.execute("VACUUM;")
    return result


def delete_older_signal_runs(con) -> Dict[str, Any]:
    """Deduplicate ``signal_records`` on (Trading_Date, Model, Ticker).

    Verbatim port of ``delete_older_signal_runs``: keeps only the row
    with the latest ``Signal_File`` (string/timestamp sort DESC) per
    partition and deletes ``rn > 1`` rows via rowid.

    Returns ``{deleted}``.
    """
    con.execute("""
        CREATE OR REPLACE TEMP TABLE duplicates_to_remove AS
        WITH ranked_signals AS (
            SELECT
                rowid AS duckdb_rowid,
                Trading_Date,
                Model,
                Ticker,
                Signal_File,
                ROW_NUMBER() OVER (
                    PARTITION BY Trading_Date, Model, Ticker
                    ORDER BY Signal_File DESC  -- Ensures 22:12 > 20:44 > 14:53
                ) AS rn
            FROM main.signal_records
        )
        SELECT duckdb_rowid FROM ranked_signals WHERE rn > 1;
    """)

    records_to_delete = con.execute(
        "SELECT COUNT(*) FROM duplicates_to_remove"
    ).fetchone()[0]

    try:
        if records_to_delete == 0:
            logger.info(
                "No duplicate signal runs found; signal_records is clean."
            )
            return {"deleted": 0}

        logger.info(
            "Removing %d older duplicate signal record(s)...",
            records_to_delete,
        )
        con.execute("""
            DELETE FROM main.signal_records
            WHERE rowid IN (SELECT duckdb_rowid FROM duplicates_to_remove);
        """)
        con.execute("CHECKPOINT;")
        con.execute("VACUUM;")
        logger.info(
            "Deleted %d duplicate signal record(s).", records_to_delete
        )
        return {"deleted": int(records_to_delete)}
    finally:
        con.execute("DROP TABLE IF EXISTS duplicates_to_remove;")


def repair_missing_close_prices(con, silent: bool = False) -> Dict[str, Any]:
    """Fill NULL ``signal_records.Close`` from ``daily_close``.

    Port of the orchestrator's ``repair_missing_close_prices``: the
    legacy version ``ATTACH``ed a separate price database read-only;
    here ``daily_close`` lives in the same database, so the UPDATE is a
    plain in-DB join (TRIM on Ticker, CAST on dates — verbatim).

    ``silent`` suppresses the per-ticker warning logs (used by the
    post-performance cleanup sweep); the unmatched list is always
    returned in the result.  Returns ``{null_before, repaired,
    still_null, unmatched: [{Ticker, Trading_Date}]}``.
    """
    if not silent:
        logger.info("Inspecting signals for missing close prices...")

    null_count = con.execute(
        "SELECT COUNT(1) FROM signal_records WHERE Close IS NULL"
    ).fetchone()[0]

    if null_count == 0:
        if not silent:
            logger.info(
                "Integrity check passed: 0 records with missing Close."
            )
        return {
            "null_before": 0,
            "repaired": 0,
            "still_null": 0,
            "unmatched": [],
        }

    if not silent:
        logger.info(
            "Found %d records with missing Close prices; repairing "
            "from daily_close.",
            null_count,
        )

    con.execute("""
        UPDATE signal_records
        SET Close = daily_close.Close
        FROM daily_close
        WHERE signal_records.Close IS NULL
          AND TRIM(signal_records.Ticker) = TRIM(daily_close.Ticker)
          AND CAST(signal_records.Trading_Date AS DATE)
              = CAST(daily_close.Date AS DATE)
    """)

    still_null = con.execute(
        "SELECT COUNT(1) FROM signal_records WHERE Close IS NULL"
    ).fetchone()[0]

    unmatched: List[Dict[str, Any]] = []
    if still_null > 0:
        unmatched_records = con.execute(
            "SELECT Ticker, Trading_Date FROM signal_records "
            "WHERE Close IS NULL"
        ).fetchall()
        unmatched = [
            {"Ticker": t, "Trading_Date": _jsonable(d)}
            for t, d in unmatched_records
        ]
        if not silent:
            logger.warning(
                "%d records could not be matched to daily_close.",
                still_null,
            )
            # justified: per-record warning emission is logging output,
            # not data transformation — cannot be vectorized.
            for ticker, dt in unmatched_records:
                logger.warning(
                    "   -> Unmatched Dropout: Ticker='%s' | "
                    "Trading_Date=%s",
                    ticker,
                    dt,
                )

    fixed_count = null_count - still_null
    if not silent:
        logger.info(
            "Data repair complete. Updated %d record(s). "
            "(Still missing: %d)",
            fixed_count,
            still_null,
        )
    return {
        "null_before": int(null_count),
        "repaired": int(fixed_count),
        "still_null": int(still_null),
        "unmatched": unmatched,
    }


def prune_unwanted_signals(
    con,
    min_reward_risk: float,
    min_target_pct: float,
    exclude_clusters: Optional[List[int]] = None,
    exclude_models: Optional[List[str]] = None,
    filter_below_open: bool = False,
) -> Dict[str, Any]:
    """Prune low-quality signals out of ``signal_records``.

    Port of the orchestrator's ``prune_unwanted_signals``: identical
    WHERE-OR logic (``Reward_Risk_Ratio < ? OR Target_Percent < ? OR
    Cluster = ANY(?) OR Model = ANY(?)`` plus optional
    ``Current_Price < Open``).  Unlike the legacy ``target_clusters or
    [0, 4]`` fallback, empty/None lists exclude NOTHING — ``ANY([])``
    is false for every row — matching the configured default of "no
    cluster/model exclusions" (only the reward-risk / target-percent
    thresholds stay active).

    The legacy run wrote a ``pruned_signals_*.log`` file
    (``LOG_FILE_PATH``); that is obsolete — the pruned records are
    returned in this dict.  Returns ``{pruned_count, remaining,
    pruned_records: [...]}``.
    """
    logger.info("Pruning unwanted signals from signal_records...")

    exclude_clusters = exclude_clusters or []
    exclude_models = exclude_models or []

    # Dynamically build WHERE conditions (verbatim OR-logic)
    where_conditions = [
        "Reward_Risk_Ratio < ?",
        "Target_Percent < ?",
        "Cluster = ANY(?)",
        "Model = ANY(?)",
    ]
    params: List[Any] = [
        min_reward_risk,
        min_target_pct,
        exclude_clusters,
        exclude_models,
    ]

    if filter_below_open:
        where_conditions.append("Current_Price < Open")

    where_clause = " OR ".join(where_conditions)

    # 1. Identify records before deleting (was: pruned log file)
    select_query = f"""
        SELECT Ticker, Signal_File, Current_Price, Open,
               Reward_Risk_Ratio, Target_Percent, Cluster, Model
        FROM signal_records
        WHERE {where_clause}
    """
    rel = con.execute(select_query, params)
    cols = [desc[0] for desc in rel.description]
    rows = rel.fetchall()
    pruned_records = [
        {k: _jsonable(v) for k, v in zip(cols, row)} for row in rows
    ]

    # 2. Perform deletion
    before_count = con.execute(
        "SELECT COUNT(1) FROM signal_records"
    ).fetchone()[0]
    con.execute(
        f"DELETE FROM signal_records WHERE {where_clause}", params
    )
    after_count = con.execute(
        "SELECT COUNT(1) FROM signal_records"
    ).fetchone()[0]

    logger.info(
        "Prune complete. Dropped %d record(s). Remaining: %d",
        before_count - after_count,
        after_count,
    )
    return {
        "pruned_count": int(before_count - after_count),
        "remaining": int(after_count),
        "pruned_records": pruned_records,
    }


def calculate_and_save_performance(
    con,
    max_holding_days: int = 8,
    overwrite_existing: bool = False,
) -> Dict[str, Any]:
    """Compute forward-return outcomes into ``signal_performance``.

    Verbatim port of ``calculate_and_save_performance_03``: the
    ``price_db.daily_close`` ATTACH becomes an in-DB join to
    ``daily_close``; the CTE chain is unchanged — ``holding_t`` is a
    row_number over forward prices (``p.Date > Trading_Date``), the
    tracked window is ``holding_t <= max_holding_days``, first-hit
    target/stop dates + T are taken via MIN, ``LEAST`` resolves a day
    where both hit, ``Time_Exit_Close`` is the window-end close when
    neither hits AND ``total_forward_days >= max_holding_days``
    (insufficient-forward-day signals are skipped), and
    ``overwrite_existing`` switches between ``INSERT OR REPLACE`` and
    ``INSERT ... ON CONFLICT DO NOTHING``.

    Returns ``{mode, max_holding_days, rows_processed,
    missing_tickers}``.
    """
    # --- Check for Missing Tickers (verbatim, minus ATTACH) ---
    missing_tickers_query = """
        SELECT DISTINCT s.Ticker
        FROM main.signal_records s
        LEFT JOIN daily_close p ON s.Ticker = p.Ticker
        WHERE p.Ticker IS NULL;
    """
    missing_tickers = [
        row[0] for row in con.execute(missing_tickers_query).fetchall()
    ]

    if missing_tickers:
        logger.warning(
            "Tickers present in signals but missing from daily_close "
            "(cannot be evaluated, skipped): %s",
            ", ".join(missing_tickers),
        )

    # Comprehensive processing query using advanced CTE boundaries
    # (verbatim — only the price_db. catalog prefix is dropped).
    processing_query = f"""
        WITH RankedPrices AS (
            SELECT
                s.Ticker,
                s.Trading_Date AS Signal_Date,
                s.Cluster,
                s.Model,
                s.Signal_File,
                s.Target,
                s.Stop_Price,
                p.Date AS Price_Date,
                p.Close AS Day_Close,
                ROW_NUMBER() OVER(
                    PARTITION BY s.Ticker, s.Trading_Date, s.Cluster, s.Model, s.Signal_File
                    ORDER BY p.Date ASC
                ) AS holding_t
            FROM main.signal_records s
            JOIN daily_close p
              ON s.Ticker = p.Ticker
             AND p.Date > s.Trading_Date
        ),
        MaxAvailableT AS (
            SELECT
                Ticker, Signal_Date, Cluster, Model, Signal_File,
                MAX(holding_t) as max_available_t
            FROM RankedPrices
            GROUP BY Ticker, Signal_Date, Cluster, Model, Signal_File
        ),
        TrackedWindows AS (
            SELECT * FROM RankedPrices WHERE holding_t <= {max_holding_days}
        ),
        AggregatedHits AS (
            SELECT
                w.Ticker, w.Signal_Date, w.Cluster, w.Model, w.Signal_File,

                MIN(CASE WHEN w.Day_Close >= w.Target THEN w.Price_Date END) AS Hit_Target_Date,
                MIN(CASE WHEN w.Day_Close >= w.Target THEN w.holding_t END) AS Hit_Target_T,

                MIN(CASE WHEN w.Day_Close <= w.Stop_Price THEN w.Price_Date END) AS Hit_Stop_Date,
                MIN(CASE WHEN w.Day_Close <= w.Stop_Price THEN w.holding_t END) AS Hit_Stop_T,

                MAX(CASE WHEN w.holding_t = {max_holding_days} THEN w.Day_Close END) AS Window_End_Close,

                ANY_VALUE(m.max_available_t) as total_forward_days
            FROM TrackedWindows w
            JOIN MaxAvailableT m
              ON w.Ticker = m.Ticker
             AND w.Signal_Date = m.Signal_Date
             AND w.Cluster = m.Cluster
             AND w.Model = m.Model
             AND w.Signal_File = m.Signal_File
            GROUP BY w.Ticker, w.Signal_Date, w.Cluster, w.Model, w.Signal_File
        ),
        FilteredResults AS (
            SELECT
                Ticker, Signal_Date, Cluster, Model, Signal_File,
                Hit_Target_Date, Hit_Target_T, Hit_Stop_Date, Hit_Stop_T,

                CASE
                    WHEN Hit_Target_T IS NULL AND Hit_Stop_T IS NULL THEN Window_End_Close
                    ELSE NULL
                END AS Time_Exit_Close,

                CASE
                    WHEN Hit_Target_T IS NOT NULL AND Hit_Stop_T IS NOT NULL THEN LEAST(Hit_Target_T, Hit_Stop_T)
                    WHEN Hit_Target_T IS NOT NULL THEN Hit_Target_T
                    WHEN Hit_Stop_T IS NOT NULL THEN Hit_Stop_T
                    ELSE {max_holding_days}
                END AS T
            FROM AggregatedHits
            WHERE
               Hit_Target_T IS NOT NULL
               OR Hit_Stop_T IS NOT NULL
               OR total_forward_days >= {max_holding_days}
        )
    """

    column_selection = """
        Ticker, Signal_Date, Cluster, Model, Signal_File,
        Hit_Target_Date, Hit_Target_T, Hit_Stop_Date, Hit_Stop_T,
        Time_Exit_Close, T
    """

    # ``RETURNING 1`` replaces the legacy ``conn.rowcount`` read (which
    # is always -1 under duckdb >= 1.x, so the legacy log always showed
    # 0) — the mutation count is accurate in the returned dict.
    if overwrite_existing:
        mode = "overwrite"
        upsert_query = f"""
            {processing_query}
            INSERT OR REPLACE INTO main.signal_performance (
                Ticker, Trading_Date, Cluster, Model, Signal_File,
                Hit_Target_Date, Hit_Target_T, Hit_Stop_Date, Hit_Stop_T, Time_Exit_Close, T
            )
            SELECT {column_selection} FROM FilteredResults
            RETURNING 1;
        """
    else:
        mode = "append"
        upsert_query = f"""
            {processing_query}
            INSERT INTO main.signal_performance (
                Ticker, Trading_Date, Cluster, Model, Signal_File,
                Hit_Target_Date, Hit_Target_T, Hit_Stop_Date, Hit_Stop_T, Time_Exit_Close, T
            )
            SELECT {column_selection} FROM FilteredResults
            ON CONFLICT DO NOTHING
            RETURNING 1;
        """

    logger.info(
        "Calculating path windows (T <= %d, mode=%s)...",
        max_holding_days,
        mode,
    )
    rows_processed = len(con.execute(upsert_query).fetchall())
    logger.info(
        "Performance calculations complete. Mutated %d rows.",
        rows_processed,
    )
    return {
        "mode": mode,
        "max_holding_days": int(max_holding_days),
        "rows_processed": rows_processed,
        "missing_tickers": missing_tickers,
    }


def setup_cluster_metadata(con) -> Dict[str, Any]:
    """Populate ``cluster_metadata`` (6 rows, verbatim INSERT OR REPLACE)."""
    con.execute("""
        INSERT OR REPLACE INTO cluster_metadata (Cluster, Description) VALUES
        (0, '0: Bull Market Drift'),
        (1, '1: High-Confidence V-Recovery'),
        (2, '2: Volatile Mean Reversion'),
        (3, '3: Momentum Dip'),
        (4, '4: Panic / Deep Stress'),
        (5, '5: Others');
    """)
    logger.info("Cluster metadata table populated (6 rows).")
    return {"rows": 6}


__all__ = [
    "REQUIRED_SIGNAL_HEADERS",
    "ingest_signal_folder",
    "delete_older_signal_runs",
    "repair_missing_close_prices",
    "prune_unwanted_signals",
    "calculate_and_save_performance",
    "setup_cluster_metadata",
]
