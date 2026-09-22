"""Signal-analysis pipeline — server-side port (F25-T1).

Verbatim port of the five-phase job from
``quant_strategy_building/signal_input2/`` (``orchestrator_generic.py``,
``phase1_ingestion.py``, ``phase2_dedup_and_repair.py``,
``phase3_query_engine.py``, ``phase4_backtest_runner.py``,
``phase5_analytics_matrix.py``) plus the multi-set loop of
``process_mutli_sets_signals.py`` onto a single DuckDB connection
managed by the Flight service.

Architecture changes (all required by the single-writer model):

* The pipeline runs **inside** the service — ``IngestionEngine``
  calls these functions with ``self.db.conn`` under
  ``self.db._write_lock``; the per-set ``signals.duckdb`` files are
  gone and every object lives inside the service database under a
  per-set schema ``signals_<name>`` (``CREATE SCHEMA IF NOT EXISTS``
  at runtime — SCHEMA_VERSION stays 7, no migration).
* Every created table AND view carries the ``aly_`` prefix (this job's
  ``aly_signal_performance`` is a DIFFERENT 15-column table than F24's
  ``main.signal_performance`` and its both-hit tie-break differs —
  target only wins when ``Raw_Target_T < Raw_Stop_T``; a same-day hit
  goes to the stop).
* ``HIST_PRICE_DB_PATH`` / ``price_db.daily_close`` ATTACH joins are
  replaced by in-database joins to ``main.daily_close`` — no ATTACH.
* Phase-4 print outputs (the per-view scorecard matrix and the edge
  validator metrics) are returned as JSON-serialisable dicts in the
  step results; the log lines are kept.
* Reports land under ``reports.report_dir/signal_analysis/<name>/``
  instead of next to a per-set DuckDB file.

Each function returns a JSON-serialisable summary dict.
"""

from __future__ import annotations

import glob
import logging
import os
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
import pandas_market_calendars as mcal
import pytz

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------
# Constants (verbatim legacy)
# ---------------------------------------------------------------------

# Initialize NYSE calendar and Timezones (phase1_ingestion.py)
_nyse = mcal.get_calendar("NYSE")
_london_tz = pytz.timezone("Europe/London")
_ny_tz = pytz.timezone("America/New_York")

_SIGNAL_FILE_SUFFIX = "_active_signal_combined.xlsx"

RAW_TABLE = "aly_signal_records_raw"
PERF_TABLE = "aly_signal_performance"

# The four fixed views (orchestrator_generic.force_compile_phase3_views,
# aly_-prefixed per the F25 naming convention).
VIEW_BASELINE = "aly_v_signals_baseline_all_signals"
VIEW_STRICT_EOD = "aly_v_signals_strict_eod_price_above_open"
VIEW_INTRADAY = "aly_v_signals_intraday_high_rr"
VIEW_ATR = "aly_v_signals_atr_target_multiple_filter"
ALL_VIEWS = (VIEW_BASELINE, VIEW_STRICT_EOD, VIEW_INTRADAY, VIEW_ATR)

# Safety net: set names are slug-validated in config ([a-z0-9_]+) so the
# derived schema name is always a safe SQL identifier; this guards any
# caller that bypasses config validation.
_SCHEMA_NAME_RE = re.compile(r"^[a-z0-9_]+$")


def schema_for_set(set_name: str) -> str:
    """Return the per-set schema name ``signals_<name>``."""
    return f"signals_{set_name}"


def _check_schema(schema: str) -> None:
    if not _SCHEMA_NAME_RE.fullmatch(schema):
        raise ValueError(f"unsafe schema name {schema!r}")


def _table_exists(con, schema: str, table_name: str) -> bool:
    """information_schema existence check (SHOW TABLES only sees the
    current schema)."""
    return bool(
        con.execute(
            "SELECT COUNT(1) FROM information_schema.tables "
            "WHERE table_schema = ? AND table_name = ?",
            [schema, table_name],
        ).fetchone()[0]
    )


# =====================================================================
# Phase 1 — ingestion (phase1_ingestion.py)
# =====================================================================


def is_intraday_signal(filename: str) -> bool:
    """
    Extracts YYYYMMDD_HHMM from filename assuming London local time (GMT/BST).
    Converts timestamp to US Eastern Time (EST/EDT) catering for winter/summer DST.
    Returns True if converted time falls within NYSE RTH (09:30 - 16:00 EST).
    """
    basename = os.path.basename(filename)
    match = re.search(r"(\d{8})_(\d{4})", basename)
    if not match:
        return False

    date_str, time_str = match.groups()
    try:
        # Parse naive datetime
        naive_dt = datetime.strptime(f"{date_str} {time_str}", "%Y%m%d %H%M")
    except ValueError:
        return False

    # 1. Localize to London time (automatically accounts for GMT vs BST)
    london_dt = _london_tz.localize(naive_dt)

    # 2. Convert to New York time (automatically accounts for EST vs EDT)
    ny_dt = london_dt.astimezone(_ny_tz)

    # 3. Check if the NY date is a valid market trading day (handles weekends & US holidays)
    schedule = _nyse.schedule(start_date=ny_dt.date(), end_date=ny_dt.date())
    if schedule.empty:
        return False  # Weekend or Exchange Holiday

    # 4. Check if converted NY time (HHMM) falls between 09:30 and 16:00
    ny_time_int = ny_dt.hour * 100 + ny_dt.minute
    return 930 <= ny_time_int <= 1600


def setup_raw_table_schema(con, schema: str) -> None:
    """
    Creates aly_signal_records_raw with a clean primary key and flexible
    column definitions (verbatim DDL, aly_-prefixed + schema-qualified).
    """
    _check_schema(schema)
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {schema}.{RAW_TABLE} (
            Signal_File VARCHAR NOT NULL,
            Model VARCHAR NOT NULL,
            Trading_Date DATE NOT NULL,
            Ticker VARCHAR NOT NULL,
            Open DOUBLE,
            High DOUBLE,
            Low DOUBLE,
            Close DOUBLE,
            Cluster INTEGER,
            Hist_WinRate DOUBLE,
            ATR DOUBLE,
            best_k DOUBLE,
            Target DOUBLE,
            Current_Price DOUBLE,
            Stop_Price DOUBLE,
            Target_Percent DOUBLE,
            Stop_Percent DOUBLE,
            Reward_Risk_Ratio DOUBLE,
            Shares_to_buy INTEGER,
            Max_Drawdown DOUBLE,
            Centroid_Dist DOUBLE,
            Drift_Warning BOOLEAN,
            Is_Intraday BOOLEAN NOT NULL,
            Ingested_At TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (Trading_Date, Model, Ticker, Signal_File)
        );
    """)


def ingest_signals(
    con,
    schema: str,
    signal_folder_path: str,
    backdate_days: int = 90,
) -> Dict[str, Any]:
    """Ingest ``*_active_signal_combined.xlsx`` files into
    ``{schema}.aly_signal_records_raw``.

    Verbatim port of ``phase1_ingestion.ingest_raw_signals``: globs the
    folder, DROP+recreates the raw table per run, tags each file with
    the London->NYSE ``Is_Intraday`` flag, iterates ALL sheets, and
    ``INSERT OR REPLACE``s rows filtered by ``Trading_Date >=
    today - backdate_days`` (a DATE filter, not file mtime).

    Returns ``{status, folder_path, files_seen, files_processed,
    sheets_processed, inserted, files: [...]}`` — ``status`` is
    ``NO_FILES`` when the glob is empty (folder missing or no matching
    files; the table is left untouched, as legacy).
    """
    _check_schema(schema)
    logger.info(
        "Scanning folder '%s' for active signal Excel files...",
        signal_folder_path,
    )
    search_pattern = os.path.join(
        signal_folder_path, f"*{_SIGNAL_FILE_SUFFIX}"
    )
    excel_files = glob.glob(search_pattern)

    result: Dict[str, Any] = {
        "status": "SUCCESS",
        "folder_path": signal_folder_path,
        "files_seen": len(excel_files),
        "files_processed": 0,
        "sheets_processed": 0,
        "inserted": 0,
        "files": [],
    }

    if not excel_files:
        logger.warning("No active signal Excel files found.")
        result["status"] = "NO_FILES"
        return result

    # Calculate cutoff date based on backdate_days
    cutoff_date = (datetime.now() - timedelta(days=backdate_days)).date()
    logger.info(
        "Found %d file(s). Ingesting records on/after %s "
        "(%d days lookback)...",
        len(excel_files),
        cutoff_date,
        backdate_days,
    )

    con.execute(f"DROP TABLE IF EXISTS {schema}.{RAW_TABLE}")
    setup_raw_table_schema(con, schema)

    insert_sql = f"""
        INSERT OR REPLACE INTO {schema}.{RAW_TABLE} (
            Signal_File, Model, Trading_Date, Ticker, Open, High, Low, Close,
            Cluster, Hist_WinRate, ATR, best_k, Target, Current_Price,
            Stop_Price, Target_Percent, Stop_Percent, Reward_Risk_Ratio,
            Shares_to_buy, Max_Drawdown, Centroid_Dist, Drift_Warning, Is_Intraday
        )
        SELECT
            Signal_File, Model, Trading_Date, Ticker,
            CAST(Open AS DOUBLE), CAST(High AS DOUBLE), CAST(Low AS DOUBLE), CAST(Close AS DOUBLE),
            CAST(Cluster AS INTEGER), CAST(Hist_WinRate AS DOUBLE), CAST(ATR AS DOUBLE), CAST(best_k AS DOUBLE),
            CAST(Target AS DOUBLE), CAST(Current_Price AS DOUBLE), CAST(Stop_Price AS DOUBLE),
            CAST(Target_Percent AS DOUBLE), CAST(Stop_Percent AS DOUBLE), CAST(Reward_Risk_Ratio AS DOUBLE),
            CAST(Shares_to_buy AS INTEGER), CAST(Max_Drawdown AS DOUBLE), CAST(Centroid_Dist AS DOUBLE),
            CAST(Drift_Warning AS BOOLEAN), Is_Intraday
        FROM df_temp
        WHERE Trading_Date >= ?
        RETURNING 1;
    """

    # justified: file-level iteration — each xlsx is a separate
    # pd.read_excel I/O call; the ROWS inside each sheet are inserted by
    # a single vectorized INSERT OR REPLACE, not a Python loop.
    for file_path in sorted(excel_files):
        file_name = os.path.basename(file_path)
        intraday_flag = is_intraday_signal(file_name)
        file_entry: Dict[str, Any] = {
            "file": file_name,
            "is_intraday": bool(intraday_flag),
        }

        try:
            # pd.ExcelFile holds the file handle open (Windows locks it
            # until close/GC) — bound it to a context manager.
            with pd.ExcelFile(file_path) as xls:
                sheet_names = list(xls.sheet_names)
            # justified: sheet-level iteration — each sheet is a separate
            # pd.read_excel I/O call inside one workbook.
            for sheet in sheet_names:
                df = pd.read_excel(file_path, sheet_name=sheet)
                if df.empty or "Ticker" not in df.columns:
                    continue

                df["Signal_File"] = file_name
                df["Is_Intraday"] = intraday_flag

                if "Cluster" in df.columns:
                    df["Cluster"] = df["Cluster"].fillna(-1).astype(int)
                else:
                    df["Cluster"] = -1

                df["Trading_Date"] = pd.to_datetime(
                    df["Trading_Date"]
                ).dt.date

                # pandas 3.x 'str' dtype cannot be scanned by duckdb's
                # register() path — normalize to object dtype (same fix
                # as db.upsert_dataframe / signal_pipeline).
                str_cols = [
                    c
                    for c in df.columns
                    if str(df[c].dtype) in ("str", "string")
                ]
                if str_cols:
                    df[str_cols] = df[str_cols].astype(object)

                con.register("df_temp", df)
                try:
                    inserted_rows = len(
                        con.execute(insert_sql, [cutoff_date]).fetchall()
                    )
                finally:
                    con.unregister("df_temp")

                result["sheets_processed"] += 1
                result["inserted"] += inserted_rows
                file_entry.setdefault("sheets", 0)
                file_entry["sheets"] += 1
                file_entry["inserted"] = (
                    file_entry.get("inserted", 0) + inserted_rows
                )

            result["files_processed"] += 1
            result["files"].append(file_entry)

        except Exception as e:
            logger.error("Error processing file '%s': %s", file_name, e)
            file_entry["error"] = str(e)
            result["files"].append(file_entry)

    logger.info("Phase 1 Ingestion Complete.")
    return result


# =====================================================================
# Phase 2 — deduplication & price repair (phase2_dedup_and_repair.py)
# =====================================================================


def deduplicate_signals(con, schema: str) -> Dict[str, Any]:
    """Deduplicate ``{schema}.aly_signal_records_raw``.

    Verbatim port of ``deduplicate_raw_signals``: keeps only the LATEST
    ``Signal_File`` for each ``(Trading_Date, Model, Ticker)`` via a
    ranked TEMP table + ``DELETE ... USING``.

    Returns ``{deleted, remaining}``.
    """
    _check_schema(schema)
    logger.info("Starting signal deduplication process...")

    raw = f"{schema}.{RAW_TABLE}"
    before_count = con.execute(
        f"SELECT COUNT(1) FROM {raw}"
    ).fetchone()[0]

    # Identify older runs per (Trading_Date, Model, Ticker). The helper
    # temp table carries the aly_ prefix like every object this
    # enhancement creates, and is dropped in the finally block below so
    # nothing leaks onto the long-lived service connection.
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE aly_temp_dedup_keys AS
        WITH ranked_signals AS (
            SELECT
                Trading_Date,
                Model,
                Ticker,
                Signal_File,
                ROW_NUMBER() OVER (
                    PARTITION BY Trading_Date, Model, Ticker
                    ORDER BY Signal_File DESC
                ) AS rn
            FROM {raw}
        )
        SELECT Trading_Date, Model, Ticker, Signal_File
        FROM ranked_signals
        WHERE rn > 1;
    """)

    # Delete older runs from aly_signal_records_raw
    try:
        con.execute(f"""
            DELETE FROM {raw}
            USING aly_temp_dedup_keys
            WHERE {raw}.Trading_Date = aly_temp_dedup_keys.Trading_Date
              AND {raw}.Model = aly_temp_dedup_keys.Model
              AND {raw}.Ticker = aly_temp_dedup_keys.Ticker
              AND {raw}.Signal_File = aly_temp_dedup_keys.Signal_File;
        """)
    finally:
        con.execute("DROP TABLE IF EXISTS aly_temp_dedup_keys")

    after_count = con.execute(f"SELECT COUNT(1) FROM {raw}").fetchone()[0]
    logger.info(
        "Deduplication complete. Dropped %d duplicate run records. "
        "Remaining: %d",
        before_count - after_count,
        after_count,
    )
    return {
        "deleted": int(before_count - after_count),
        "remaining": int(after_count),
    }


def repair_missing_close_prices(con, schema: str) -> Dict[str, Any]:
    """Fill NULL/NaN ``Close`` in ``{schema}.aly_signal_records_raw``
    from ``main.daily_close``.

    Port of ``repair_missing_close_prices``: the legacy ``ATTACH``ed a
    separate price DB (``hist_db.DAILY_CLOSE``); here ``daily_close``
    lives in the same database, so the UPDATE is a plain in-DB join
    (TRIM on Ticker, CAST on dates, ``isnan`` handling — verbatim).

    Returns ``{null_before, repaired, still_null, unmatched: [...]}``.
    """
    _check_schema(schema)
    logger.info(
        "Inspecting %s.%s for missing Close prices...", schema, RAW_TABLE
    )

    raw = f"{schema}.{RAW_TABLE}"
    null_count = con.execute(
        f"SELECT COUNT(1) FROM {raw} WHERE Close IS NULL OR isnan(Close)"
    ).fetchone()[0]

    if null_count == 0:
        logger.info(
            "Integrity check passed: 0 records with missing Close prices."
        )
        return {
            "null_before": 0,
            "repaired": 0,
            "still_null": 0,
            "unmatched": [],
        }

    logger.info(
        "Found %d records with missing Close prices. Repairing from "
        "daily_close...",
        null_count,
    )

    # Vectorized update targeting aly_signal_records_raw and daily_close
    # (verbatim; hist_db.DAILY_CLOSE -> main.daily_close).
    con.execute(f"""
        UPDATE {raw}
        SET Close = main.daily_close.Close
        FROM main.daily_close
        WHERE ({raw}.Close IS NULL OR isnan({raw}.Close))
          AND TRIM({raw}.Ticker) = TRIM(main.daily_close.Ticker)
          AND CAST({raw}.Trading_Date AS DATE)
              = CAST(main.daily_close.Date AS DATE);
    """)

    still_null = con.execute(
        f"SELECT COUNT(1) FROM {raw} WHERE Close IS NULL OR isnan(Close)"
    ).fetchone()[0]

    unmatched: List[Dict[str, Any]] = []
    if still_null > 0:
        unmatched_rows = con.execute(
            f"SELECT Ticker, Trading_Date FROM {raw} "
            f"WHERE Close IS NULL OR isnan(Close)"
        ).fetchall()
        unmatched = [
            {
                "Ticker": t,
                "Trading_Date": d.isoformat() if d is not None else None,
            }
            for t, d in unmatched_rows
        ]
        logger.warning(
            "%d records could not be matched to daily_close.", still_null
        )

    fixed_count = null_count - still_null
    logger.info(
        "Data repair complete. Updated %d records. "
        "(Remaining missing: %d)",
        fixed_count,
        still_null,
    )
    return {
        "null_before": int(null_count),
        "repaired": int(fixed_count),
        "still_null": int(still_null),
        "unmatched": unmatched,
    }


# =====================================================================
# Phase 3 — dynamic query engine (phase3_query_engine.py +
#           orchestrator_generic.force_compile_phase3_views)
# =====================================================================


class SignalQueryEngine:
    """
    Builds dynamic SQL queries and DuckDB Views based on declarative
    parameter configurations (verbatim port; operates on
    ``{schema}.aly_signal_records_raw`` and creates schema-qualified
    views).
    """

    def __init__(self, con, schema: str):
        _check_schema(schema)
        self.con = con
        self.schema = schema

    def build_where_clause(self, params: Dict[str, Any]) -> str:
        """
        Translates a dictionary of parameter filters into a dynamic SQL
        WHERE clause (verbatim).
        """
        conditions = ["1=1"]  # Base condition

        # 1. Intraday / EOD Signal Filtering
        if params.get("is_intraday") is not None:
            conditions.append(f"Is_Intraday = {params['is_intraday']}")

        # 2. Filter out signals generated below Open (Current_Price < Open)
        if params.get("filter_below_open", False):
            conditions.append("Current_Price >= Open")

        # 3. Minimum Reward-to-Risk Ratio
        if params.get("min_rr") is not None:
            conditions.append(f"Reward_Risk_Ratio >= {params['min_rr']}")

        # 4. Filter by specific Models (e.g., ['SCIENTIFIC_SET_1', 'SCIENTIFIC_SET_3'])
        if params.get("models"):
            model_list = ", ".join([f"'{m}'" for m in params["models"]])
            conditions.append(f"Model IN ({model_list})")

        # 5. Target Percent Filter (Absolute Range)
        if params.get("min_target_pct") is not None:
            conditions.append(f"Target_Percent >= {params['min_target_pct']}")
        if params.get("max_target_pct") is not None:
            conditions.append(f"Target_Percent <= {params['max_target_pct']}")

        # 6. Target normalized by ATR Volatility Multiple
        # Target Distance = (Target - Current_Price) / ATR
        if params.get("min_target_atr_multiple") is not None:
            conditions.append("(Target - Current_Price) / NULLIF(ATR, 0) >= " + str(params['min_target_atr_multiple']))

        # 7. Cluster Filtering
        if params.get("clusters") is not None:
            cluster_list = ", ".join([str(c) for c in params["clusters"]])
            conditions.append(f"Cluster IN ({cluster_list})")

        return " AND ".join(conditions)

    def create_filtered_view(self, view_name: str, params: Dict[str, Any]) -> int:
        """
        Creates a DuckDB SQL VIEW containing only the signals matching
        the parameter set.  Returns the number of matching records.
        """
        _check_schema(self.schema)
        if not _SCHEMA_NAME_RE.fullmatch(view_name):
            raise ValueError(f"unsafe view name {view_name!r}")
        where_clause = self.build_where_clause(params)
        sql = f"""
            CREATE OR REPLACE VIEW {self.schema}.{view_name} AS
            SELECT *
            FROM {self.schema}.{RAW_TABLE}
            WHERE {where_clause};
        """

        self.con.execute(sql)
        count = self.con.execute(
            f"SELECT COUNT(1) FROM {self.schema}.{view_name}"
        ).fetchone()[0]

        logger.info(
            "Created View '%s.%s' | Matching Signals: %d",
            self.schema,
            view_name,
            count,
        )
        return count


# Pre-defined Dynamic Parameter Sets for Testing (verbatim)
DEFAULT_PARAMETER_SETS = {
    "Baseline_All_Signals": {
        "is_intraday": None,           # All signals (Intraday + EOD)
        "filter_below_open": False,    # Include Current_Price < Open
        "min_rr": 0.0,
    },
    "Strict_EOD_Price_Above_Open": {
        "is_intraday": False,          # Only EOD Signals
        "filter_below_open": True,     # Exclude signals where Current_Price < Open
        "min_rr": 1.2,
    },
    "Intraday_High_RR": {
        "is_intraday": True,           # Only Intraday Signals
        "filter_below_open": False,
        "min_rr": 1.5,
    },
    "ATR_Target_Multiple_Filter": {
        "is_intraday": None,
        "filter_below_open": True,
        "min_rr": 1.0,
        "min_target_atr_multiple": 1.5  # Target must be at least 1.5x ATR away
    }
}


def build_views(con, schema: str) -> Dict[str, Any]:
    """Compile the four fixed Phase-3 views inside ``schema``.

    Port of ``orchestrator_generic.force_compile_phase3_views`` — the
    four ``CREATE OR REPLACE VIEW`` statements verbatim, re-targeted at
    ``{schema}.aly_v_signals_*`` over ``{schema}.aly_signal_records_raw``.

    Returns ``{status, views: {name: row_count}}`` — ``NO_TABLE`` when
    the raw table is absent (legacy warned and returned).
    """
    _check_schema(schema)
    logger.info("Verifying Phase 3 view compilation inside %s", schema)

    views_sql = {
        VIEW_BASELINE:
            f"CREATE OR REPLACE VIEW {schema}.{VIEW_BASELINE} AS "
            f"SELECT * FROM {schema}.{RAW_TABLE};",

        VIEW_STRICT_EOD:
            f"CREATE OR REPLACE VIEW {schema}.{VIEW_STRICT_EOD} AS "
            f"SELECT * FROM {schema}.{RAW_TABLE} "
            f"WHERE Is_Intraday = FALSE AND Current_Price >= Open "
            f"AND Reward_Risk_Ratio >= 1.2;",

        VIEW_INTRADAY:
            f"CREATE OR REPLACE VIEW {schema}.{VIEW_INTRADAY} AS "
            f"SELECT * FROM {schema}.{RAW_TABLE} "
            f"WHERE Is_Intraday = TRUE AND Reward_Risk_Ratio >= 1.5;",

        VIEW_ATR:
            f"CREATE OR REPLACE VIEW {schema}.{VIEW_ATR} AS "
            f"SELECT * FROM {schema}.{RAW_TABLE} "
            f"WHERE Current_Price >= Open AND Reward_Risk_Ratio >= 1.0 "
            f"AND (Target - Current_Price) / NULLIF(ATR, 0) >= 1.5;",
    }

    if not _table_exists(con, schema, RAW_TABLE):
        logger.warning("Table '%s.%s' not found yet.", schema, RAW_TABLE)
        return {"status": "NO_TABLE", "views": {}}

    counts: Dict[str, int] = {}
    for view_name, sql in views_sql.items():
        try:
            con.execute(sql)
            cnt = con.execute(
                f"SELECT COUNT(1) FROM {schema}.{view_name}"
            ).fetchone()[0]
            counts[view_name] = int(cnt)
            logger.info(
                "View Verified/Created: %s.%s (%d rows)",
                schema,
                view_name,
                cnt,
            )
        except Exception as e:
            logger.warning(
                "Could not build fallback view %s: %s", view_name, e
            )
    return {"status": "SUCCESS", "views": counts}


# =====================================================================
# Phase 4 — performance builder, backtest engine & statistical
#           validation suite (phase4_backtest_runner.py)
# =====================================================================


def ensure_schema_up_to_date(con, schema: str) -> None:
    """
    Ensures ``{schema}.aly_signal_performance`` has all required columns
    (e.g., Time_Exit_Date) even if the table pre-existed under an older
    schema (verbatim port; ``SHOW TABLES`` swapped for
    information_schema because SHOW TABLES only sees the current schema).
    """
    _check_schema(schema)
    try:
        if _table_exists(con, schema, PERF_TABLE):
            cols = [
                c[1]
                for c in con.execute(
                    f"PRAGMA table_info('{schema}.{PERF_TABLE}')"
                ).fetchall()
            ]
            if "Time_Exit_Date" not in cols:
                logger.info(
                    "Upgrading schema: Adding 'Time_Exit_Date' column "
                    "to %s.%s...",
                    schema,
                    PERF_TABLE,
                )
                con.execute(
                    f"ALTER TABLE {schema}.{PERF_TABLE} "
                    f"ADD COLUMN Time_Exit_Date DATE;"
                )
    except Exception as e:
        logger.warning("Schema migration warning: %s", e)


def verify_execution_status_integrity(con, schema: str) -> Dict[str, int]:
    """
    Validates mathematical correctness of Execution_Status codes against
    ``main.daily_close`` (verbatim; ``price_db.daily_close`` -> in-DB
    ``main.daily_close``).  Returns ``{err1, err2, err3}`` so the counts
    also land in the action result JSON.
    """
    _check_schema(schema)
    validation_sql = f"""
        SELECT
            COUNT(CASE WHEN p.Execution_Status = 1 AND price.Low > s.Current_Price THEN 1 END) AS err1,
            COUNT(CASE WHEN p.Execution_Status = 2 AND price.Open < s.Target THEN 1 END) AS err2,
            COUNT(CASE WHEN p.Execution_Status = 3 AND price.Low <= s.Current_Price THEN 1 END) AS err3
        FROM {schema}.{PERF_TABLE} p
        JOIN {schema}.{RAW_TABLE} s
          ON p.Ticker = s.Ticker
         AND p.Trading_Date = s.Trading_Date
         AND p.Cluster = COALESCE(s.Cluster, 0)
         AND p.Model = COALESCE(s.Model, 'UNKNOWN')
         AND p.Signal_File = COALESCE(s.Signal_File, 'UNKNOWN')
        JOIN main.daily_close price
          ON p.Ticker = price.Ticker
         AND price.Date = (
             SELECT MIN(Date)
             FROM main.daily_close
             WHERE Ticker = s.Ticker AND Date > s.Trading_Date
         );
    """
    err1, err2, err3 = con.execute(validation_sql).fetchone()

    if err1 + err2 + err3 == 0:
        logger.info(
            "`Execution_Status` Verification Passed! All status codes "
            "are 100% accurate."
        )
    else:
        logger.error(
            "Execution Verification Failed! Logic Errors: "
            "Code1=%d, Code2=%d, Code3=%d",
            err1,
            err2,
            err3,
        )
    return {"err1": int(err1), "err2": int(err2), "err3": int(err3)}


def generate_performance_excel_report(
    con, schema: str, report_dir: str
) -> Dict[str, Any]:
    """Export all ``{schema}.aly_signal_performance`` records to
    ``RPT_signal_performance.xlsx`` inside ``report_dir`` (verbatim
    query; the output dir is the per-set report subfolder, not the
    legacy signals-DB dirname)."""
    _check_schema(schema)
    os.makedirs(report_dir, exist_ok=True)
    excel_path = os.path.join(report_dir, "RPT_signal_performance.xlsx")

    report_query = f"""
        SELECT
            p.Ticker,
            p.Trading_Date,
            p.Cluster,
            p.Model,
            p.Signal_File,
            s.Current_Price AS Signal_Close,
            s.Target AS Target_Price,
            ROUND((s.Target - s.Current_Price) / s.Current_Price * 100.0, 2) AS Target_Pct,
            s.Stop_Price,
            ROUND((s.Stop_Price - s.Current_Price) / s.Current_Price * 100.0, 2) AS Stop_Pct,

            CASE
                WHEN p.Hit_Target_Date IS NOT NULL THEN 'TARGET_HIT'
                WHEN p.Hit_Stop_Date IS NOT NULL THEN 'STOP_HIT'
                ELSE 'TIME_EXIT'
            END AS Outcome,

            p.Hit_Target_Date,
            p.Hit_Target_T,
            p.Hit_Stop_Date,
            p.Hit_Stop_T,
            p.Time_Exit_Date,
            p.Time_Exit_Close,
            p.T AS Holding_Duration,

            p.Next_Day_Open,
            p.Open_Gap_Pct,
            p.Execution_Status AS Execution_Status_Code,
            CASE p.Execution_Status
                WHEN 1 THEN 'VALID_FILLABLE'
                WHEN 2 THEN 'INVALID_OPEN_ABOVE_TARGET'
                WHEN 3 THEN 'UNFILLED_GAPPED_UP'
                ELSE 'UNKNOWN'
            END AS Execution_Status_Desc
        FROM {schema}.{PERF_TABLE} p
        LEFT JOIN {schema}.{RAW_TABLE} s
          ON p.Ticker = s.Ticker
         AND p.Trading_Date = s.Trading_Date
         AND p.Cluster = COALESCE(s.Cluster, 0)
         AND p.Model = COALESCE(s.Model, 'UNKNOWN')
         AND p.Signal_File = COALESCE(s.Signal_File, 'UNKNOWN')
        ORDER BY p.Trading_Date DESC, p.Ticker;
    """

    df_report = con.execute(report_query).df()
    with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
        df_report.to_excel(writer, sheet_name="Signal Performance", index=False)

    logger.info(
        "Excel report successfully exported (%d rows): %s",
        len(df_report),
        excel_path,
    )
    return {"path": excel_path, "rows": int(len(df_report))}


def calculate_and_save_performance(
    con,
    schema: str,
    report_dir: str,
    max_holding_days: int = 8,
    overwrite_existing: bool = True,
) -> Dict[str, Any]:
    """Compute forward-return outcomes into
    ``{schema}.aly_signal_performance`` (15-column table — DIFFERENT
    from F24's 11-col ``main.signal_performance``).

    Verbatim port of ``phase4_backtest_runner.calculate_and_save_performance``:
    ``holding_t`` is a row_number over forward prices
    (``p.Date > Trading_Date``), the tracked window is
    ``holding_t <= max_holding_days``, first-hit target/stop via MIN,
    **stop wins a same-day tie** (target counts only when
    ``Raw_Target_T < Raw_Stop_T``; stop wins when
    ``Raw_Stop_T <= Raw_Target_T``), ``Time_Exit_*`` when neither hits
    AND ``total_forward_days >= max_holding_days``, Execution_Status
    1/2/3 from ``T1_Open``/``T1_Low`` vs ``Current_Price``/``Target``,
    then the execution-status integrity check and the Excel export.

    ``overwrite_existing`` is kept for signature parity (the legacy
    body always does INSERT OR REPLACE).
    """
    _check_schema(schema)
    raw = f"{schema}.{RAW_TABLE}"
    perf = f"{schema}.{PERF_TABLE}"

    create_perf_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {perf} (
            "Ticker" VARCHAR,
            "Trading_Date" DATE,
            "Cluster" NUMERIC(1,0),
            "Model" VARCHAR,
            "Signal_File" VARCHAR,
            "Hit_Target_Date" DATE,
            "Hit_Target_T" INTEGER,
            "Hit_Stop_Date" DATE,
            "Hit_Stop_T" INTEGER,
            "Time_Exit_Date" DATE,
            "Time_Exit_Close" NUMERIC(12,2),
            "T" INTEGER,
            "Next_Day_Open" NUMERIC(12,2),
            "Open_Gap_Pct" NUMERIC(8,4),
            "Execution_Status" UTINYINT,
            PRIMARY KEY (Ticker, Trading_Date, Cluster, Model, Signal_File)
        );
    """
    con.execute(create_perf_table_sql)
    ensure_schema_up_to_date(con, schema)

    processing_query = f"""
        WITH RankedPrices AS (
            SELECT
                s.Ticker,
                s.Trading_Date AS Signal_Date,
                COALESCE(s.Cluster, 0) AS Cluster,
                COALESCE(s.Model, 'UNKNOWN') AS Model,
                COALESCE(s.Signal_File, 'UNKNOWN') AS Signal_File,
                s.Current_Price,
                s.Target,
                s.Stop_Price,
                p.Date AS Price_Date,
                p.Open AS Day_Open,
                p.High AS Day_High,
                p.Low  AS Day_Low,
                p.Close AS Day_Close,
                ROW_NUMBER() OVER(
                    PARTITION BY s.Ticker, s.Trading_Date, COALESCE(s.Cluster, 0), COALESCE(s.Model, 'UNKNOWN'), COALESCE(s.Signal_File, 'UNKNOWN')
                    ORDER BY p.Date ASC
                ) AS holding_t
            FROM {raw} s
            JOIN main.daily_close p
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
                ANY_VALUE(w.Current_Price) AS Current_Price,
                ANY_VALUE(w.Target) AS Target,
                ANY_VALUE(w.Stop_Price) AS Stop_Price,

                MIN(CASE WHEN w.Day_Close >= w.Target THEN w.Price_Date END) AS Raw_Target_Date,
                MIN(CASE WHEN w.Day_Close >= w.Target THEN w.holding_t END) AS Raw_Target_T,

                MIN(CASE WHEN w.Day_Close <= w.Stop_Price THEN w.Price_Date END) AS Raw_Stop_Date,
                MIN(CASE WHEN w.Day_Close <= w.Stop_Price THEN w.holding_t END) AS Raw_Stop_T,

                MAX(CASE WHEN w.holding_t = {max_holding_days} THEN w.Price_Date END) AS Window_End_Date,
                MAX(CASE WHEN w.holding_t = {max_holding_days} THEN w.Day_Close END) AS Window_End_Close,

                ANY_VALUE(m.max_available_t) as total_forward_days,

                MAX(CASE WHEN w.holding_t = 1 THEN w.Day_Open END) AS T1_Open,
                MAX(CASE WHEN w.holding_t = 1 THEN w.Day_Low END) AS T1_Low
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

                CASE
                    WHEN Raw_Target_T IS NOT NULL AND (Raw_Stop_T IS NULL OR Raw_Target_T < Raw_Stop_T)
                    THEN Raw_Target_Date ELSE NULL
                END AS Hit_Target_Date,

                CASE
                    WHEN Raw_Target_T IS NOT NULL AND (Raw_Stop_T IS NULL OR Raw_Target_T < Raw_Stop_T)
                    THEN Raw_Target_T ELSE NULL
                END AS Hit_Target_T,

                CASE
                    WHEN Raw_Stop_T IS NOT NULL AND (Raw_Target_T IS NULL OR Raw_Stop_T <= Raw_Target_T)
                    THEN Raw_Stop_Date ELSE NULL
                END AS Hit_Stop_Date,

                CASE
                    WHEN Raw_Stop_T IS NOT NULL AND (Raw_Target_T IS NULL OR Raw_Stop_T <= Raw_Target_T)
                    THEN Raw_Stop_T ELSE NULL
                END AS Hit_Stop_T,

                CASE WHEN Raw_Target_T IS NULL AND Raw_Stop_T IS NULL THEN Window_End_Date ELSE NULL END AS Time_Exit_Date,
                CASE WHEN Raw_Target_T IS NULL AND Raw_Stop_T IS NULL THEN Window_End_Close ELSE NULL END AS Time_Exit_Close,

                CASE
                    WHEN Raw_Target_T IS NOT NULL AND Raw_Stop_T IS NOT NULL THEN LEAST(Raw_Target_T, Raw_Stop_T)
                    WHEN Raw_Target_T IS NOT NULL THEN Raw_Target_T
                    WHEN Raw_Stop_T IS NOT NULL THEN Raw_Stop_T
                    ELSE {max_holding_days}
                END AS T,

                T1_Open AS Next_Day_Open,
                ROUND((T1_Open - Current_Price) / Current_Price * 100.0, 4) AS Open_Gap_Pct,
                CASE
                    WHEN T1_Open >= Target THEN 2
                    WHEN T1_Low <= Current_Price THEN 1
                    WHEN T1_Low > Current_Price THEN 3
                    ELSE 0
                END AS Execution_Status
            FROM AggregatedHits
            WHERE Raw_Target_T IS NOT NULL OR Raw_Stop_T IS NOT NULL OR total_forward_days >= {max_holding_days}
        )
    """

    column_selection = """
        Ticker, Signal_Date, Cluster, Model, Signal_File,
        Hit_Target_Date, Hit_Target_T, Hit_Stop_Date, Hit_Stop_T,
        Time_Exit_Date, Time_Exit_Close, T, Next_Day_Open, Open_Gap_Pct, Execution_Status
    """

    upsert_query = f"""
        {processing_query}
        INSERT OR REPLACE INTO {perf} (
            Ticker, Trading_Date, Cluster, Model, Signal_File,
            Hit_Target_Date, Hit_Target_T, Hit_Stop_Date, Hit_Stop_T, Time_Exit_Date, Time_Exit_Close, T,
            Next_Day_Open, Open_Gap_Pct, Execution_Status
        )
        SELECT {column_selection} FROM FilteredResults
        RETURNING 1;
    """
    # ``RETURNING 1`` reports an accurate mutation count (F24 pattern —
    # the legacy job relied on conn.rowcount which duckdb returns -1
    # for, so it never had a real count).
    rows_processed = len(con.execute(upsert_query).fetchall())

    integrity = verify_execution_status_integrity(con, schema)
    report = generate_performance_excel_report(con, schema, report_dir)

    return {
        "rows_processed": int(rows_processed),
        "max_holding_days": int(max_holding_days),
        "execution_status_integrity": integrity,
        "excel_report": report,
    }


def run_backtest_for_view(
    con, schema: str, view_name: str, holding_period_days: int = 8
) -> Dict[str, Any]:
    """Per-view scorecard row (``BacktestRunner.run_backtest_for_view``
    verbatim; ``holding_period_days`` is unused in the legacy query and
    kept for signature parity).  ``view_name`` is the aly_-prefixed
    schema-local view name; the label strips ``aly_v_signals_`` so the
    printed config name matches the legacy output."""
    _check_schema(schema)
    if not _SCHEMA_NAME_RE.fullmatch(view_name):
        raise ValueError(f"unsafe view name {view_name!r}")
    label = view_name.replace("aly_v_signals_", "").upper()
    query = f"""
        SELECT
            '{label}' AS "Config / View",
            COUNT(p.Ticker) AS "Total Trades",
            COUNT(p.Hit_Target_Date) AS "Target Hits",
            COUNT(p.Hit_Stop_Date) AS "Stop Hits",
            COUNT(CASE WHEN p.Hit_Target_Date IS NULL AND p.Hit_Stop_Date IS NULL THEN 1 END) AS "Time Exits",
            COUNT(CASE WHEN p.Execution_Status = 1 AND p.Hit_Target_Date IS NOT NULL THEN 1 END) AS "Realized Target Hits (Filled)",
            COUNT(CASE WHEN p.Execution_Status != 1 AND p.Hit_Target_Date IS NOT NULL THEN 1 END) AS "Unfilled Target Hits"
        FROM {schema}.{view_name} s
        JOIN {schema}.{PERF_TABLE} p
          ON s.Ticker = p.Ticker
         AND s.Trading_Date = p.Trading_Date
         AND COALESCE(s.Cluster, 0) = p.Cluster
         AND COALESCE(s.Model, 'UNKNOWN') = p.Model
         AND COALESCE(s.Signal_File, 'UNKNOWN') = p.Signal_File;
    """
    return con.execute(query).df().to_dict(orient="records")[0]


class SignalEdgeValidator:
    """
    Evaluates signal performance against structural requirements:
    Expectancy, Profit Factor, Fill Rates, Benchmark Relative Alpha
    (SPY), and a Normalized Monte Carlo Random Entry Baseline
    (verbatim port; reads through the shared service connection — the
    legacy ``ATTACH`` becomes in-DB ``main.daily_close`` joins).
    """

    def __init__(self, con, schema: str, benchmark_ticker: str = "SPY"):
        _check_schema(schema)
        self.con = con
        self.schema = schema
        self.benchmark_ticker = benchmark_ticker

    def run_validation(
        self, max_holding_days: int = 8, mc_iterations: int = 50
    ) -> pd.DataFrame:
        conn = self.con
        schema = self.schema

        df_perf = conn.execute(f"""
            SELECT
                p.Ticker, p.Trading_Date, p.Hit_Target_Date, p.Hit_Stop_Date,
                p.Time_Exit_Date, p.Time_Exit_Close, p.T AS Duration, p.Execution_Status,
                s.Current_Price AS Signal_Close, s.Target AS Target_Price, s.Stop_Price
            FROM {schema}.{PERF_TABLE} p
            JOIN {schema}.{RAW_TABLE} s
              ON p.Ticker = s.Ticker AND p.Trading_Date = s.Trading_Date
             AND p.Cluster = COALESCE(s.Cluster, 0)
             AND p.Model = COALESCE(s.Model, 'UNKNOWN')
             AND p.Signal_File = COALESCE(s.Signal_File, 'UNKNOWN')
        """).df()

        if df_perf.empty:
            logger.error("No signals found for validation.")
            return pd.DataFrame()

        # Trade Returns
        df_perf["Trade_Return_Pct"] = np.where(
            df_perf["Hit_Target_Date"].notna(),
            (df_perf["Target_Price"] - df_perf["Signal_Close"]) / df_perf["Signal_Close"] * 100.0,
            np.where(
                df_perf["Hit_Stop_Date"].notna(),
                (df_perf["Stop_Price"] - df_perf["Signal_Close"]) / df_perf["Signal_Close"] * 100.0,
                (df_perf["Time_Exit_Close"] - df_perf["Signal_Close"]) / df_perf["Signal_Close"] * 100.0,
            ),
        )

        wins = df_perf[df_perf["Trade_Return_Pct"] > 0]["Trade_Return_Pct"]
        losses = df_perf[df_perf["Trade_Return_Pct"] < 0]["Trade_Return_Pct"]

        win_rate = len(wins) / len(df_perf) if len(df_perf) > 0 else 0
        avg_win = wins.mean() if len(wins) > 0 else 0.0
        avg_loss = abs(losses.mean()) if len(losses) > 0 else 0.0
        profit_factor = wins.sum() / abs(losses.sum()) if abs(losses.sum()) > 0 else np.nan

        valid_fills = len(df_perf[df_perf["Execution_Status"] == 1])
        fill_rate = (valid_fills / len(df_perf)) * 100.0 if len(df_perf) > 0 else 0

        # SPY Excess Return
        alpha = self._calculate_spy_alpha(conn, df_perf)

        # Normalized Monte Carlo Test
        mc_win_rate = self._run_normalized_monte_carlo(
            conn, df_perf, max_holding_days, iterations=mc_iterations
        )

        metrics = {
            "Total Trades Evaluated": len(df_perf),
            "Win Rate (%)": round(win_rate * 100, 2),
            "Avg Win (%)": round(avg_win, 2),
            "Avg Loss (%)": round(avg_loss, 2),
            "Expectancy (%)": round(
                (win_rate * avg_win) - ((1 - win_rate) * avg_loss), 2
            ),
            "Profit Factor": (
                round(profit_factor, 2)
                if not np.isnan(profit_factor)
                else "N/A"
            ),
            "Valid Fill Rate (%)": round(fill_rate, 2),
            "SPY Excess Alpha (%)": round(alpha, 2),
            "Monte Carlo Random Win Rate (%)": round(mc_win_rate, 2),
            "Timing Edge Over Random (%)": round(
                (win_rate * 100) - mc_win_rate, 2
            ),
        }

        df_summary = pd.DataFrame([metrics])
        self._print_report(df_summary)
        return df_summary

    def _calculate_spy_alpha(self, conn, df_perf: pd.DataFrame) -> float:
        try:
            df_spy = (
                conn.execute(
                    f"SELECT Date, Close FROM main.daily_close "
                    f"WHERE Ticker = '{self.benchmark_ticker}'"
                )
                .df()
                .set_index("Date")
            )
            alpha_list = []
            # justified: per-trade alpha lookup — each row needs two
            # scalar index probes (entry/exit date) into the SPY price
            # series; a SQL aggregate would not reproduce the legacy
            # row-level skip semantics (pd.notna + index membership).
            for _, row in df_perf.iterrows():
                entry_date = row["Trading_Date"]
                exit_date = (
                    row["Hit_Target_Date"]
                    or row["Hit_Stop_Date"]
                    or row["Time_Exit_Date"]
                )
                if (
                    pd.notna(exit_date)
                    and entry_date in df_spy.index
                    and exit_date in df_spy.index
                ):
                    spy_ret = (
                        df_spy.loc[exit_date, "Close"]
                        - df_spy.loc[entry_date, "Close"]
                    ) / df_spy.loc[entry_date, "Close"] * 100.0
                    alpha_list.append(row["Trade_Return_Pct"] - spy_ret)
            return np.mean(alpha_list) if alpha_list else 0.0
        except Exception:
            return 0.0

    def _run_normalized_monte_carlo(
        self,
        conn,
        df_perf: pd.DataFrame,
        max_holding_days: int,
        iterations: int,
    ) -> float:
        """
        Normalized Monte Carlo Engine: Converts target/stop into %
        relative distance, samples random entry dates across price
        history, and evaluates baseline win rate (verbatim — stays
        UNSEEDED like the legacy job; results vary run to run).
        """
        try:
            df_perf["Target_Pct"] = (
                df_perf["Target_Price"] - df_perf["Signal_Close"]
            ) / df_perf["Signal_Close"]
            df_perf["Stop_Pct"] = (
                df_perf["Stop_Price"] - df_perf["Signal_Close"]
            ) / df_perf["Signal_Close"]

            mc_win_rates = []
            tickers = tuple(df_perf["Ticker"].unique())
            ticker_clause = (
                f"= '{tickers[0]}'" if len(tickers) == 1 else f"IN {tickers}"
            )

            # Fetch valid price date map
            dates_df = conn.execute(
                f"SELECT Ticker, Date, Close FROM main.daily_close "
                f"WHERE Ticker {ticker_clause}"
            ).df()
            if dates_df.empty:
                return 0.0

            for _ in range(iterations):
                sample_wins = 0
                sample_count = 0

                # justified: per-sample random-window simulation — each
                # row draws a random entry index and evaluates a price
                # window; the randomness is inherently sequential and
                # cannot be vectorized into SQL (legacy algorithm).
                for _, row in df_perf.sample(
                    n=min(len(df_perf), 150), replace=True
                ).iterrows():
                    ticker_dates = dates_df[dates_df["Ticker"] == row["Ticker"]]
                    if len(ticker_dates) <= max_holding_days:
                        continue

                    # Pick random start index
                    rand_idx = np.random.randint(
                        0, len(ticker_dates) - max_holding_days
                    )
                    entry_row = ticker_dates.iloc[rand_idx]
                    window = ticker_dates.iloc[
                        rand_idx + 1 : rand_idx + 1 + max_holding_days
                    ]

                    sim_entry = entry_row["Close"]
                    sim_target = sim_entry * (1.0 + row["Target_Pct"])
                    sim_stop = sim_entry * (1.0 + row["Stop_Pct"])

                    # Target/Stop Hit Check on Random Window
                    target_hit_t = window[window["Close"] >= sim_target]
                    stop_hit_t = window[window["Close"] <= sim_stop]

                    has_target = not target_hit_t.empty
                    has_stop = not stop_hit_t.empty

                    if has_target and not has_stop:
                        sample_wins += 1
                    elif has_target and has_stop:
                        if target_hit_t.index[0] < stop_hit_t.index[0]:
                            sample_wins += 1
                    sample_count += 1

                if sample_count > 0:
                    mc_win_rates.append((sample_wins / sample_count) * 100.0)

            return float(np.mean(mc_win_rates)) if mc_win_rates else 0.0
        except Exception as e:
            logger.warning("Monte Carlo Engine execution notice: %s", e)
            return 0.0

    def _print_report(self, df_summary: pd.DataFrame):
        logger.info("=" * 80)
        logger.info(
            "                  SIGNAL STATISTICAL EDGE & QUALITY CHECKLIST"
        )
        logger.info("=" * 80)
        # justified: per-column report emission — logging output, not
        # data transformation; each line is one metric row.
        for col in df_summary.columns:
            logger.info("  - %-35s: %s", col, df_summary[col].iloc[0])
        logger.info("=" * 80)

        expectancy = df_summary["Expectancy (%)"].iloc[0]
        pf = df_summary["Profit Factor"].iloc[0]
        edge = df_summary["Timing Edge Over Random (%)"].iloc[0]

        logger.info("VERDICT & DIAGNOSTICS:")
        if expectancy > 0.3 and (pf != "N/A" and pf >= 1.3) and edge > 3.0:
            logger.info(
                "  GENUINE EDGE DETECTED: Signals beat random baseline "
                "and market noise."
            )
        elif expectancy > 0:
            logger.info(
                "  MODERATE PERFORMANCE: Strategy shows modest gains; "
                "sensitive to market drift."
            )
        else:
            logger.info(
                "  NO STATISTICAL EDGE: Performance is market-dependent "
                "or noise-driven."
            )
        logger.info("=" * 80)


def run_backtest(
    con,
    schema: str,
    report_dir: str,
    max_holding_days: int = 8,
    benchmark: str = "SPY",
    mc_iterations: int = 50,
) -> Dict[str, Any]:
    """Phase 4 bundle (orchestrator_generic Phase-4 block):

    1. ``calculate_and_save_performance`` — build/refresh
       ``{schema}.aly_signal_performance`` + integrity check + the
       ``RPT_signal_performance.xlsx`` export.
    2. Per-view scorecard — ``run_backtest_for_view`` over the four
       ``aly_v_signals_*`` views (the legacy printed scorecard matrix
       is logged AND returned as ``scorecard`` rows for the JSON).
    3. ``SignalEdgeValidator.run_validation`` — metrics dict into
       ``edge_validation`` (empty perf -> ``{"status": "NO_DATA"}``).
    """
    perf = calculate_and_save_performance(
        con, schema, report_dir, max_holding_days=max_holding_days
    )

    scorecard_rows: List[Dict[str, Any]] = []
    # justified: one scorecard query per fixed view — four constant
    # view evaluations, not a data loop (legacy iterated the same list).
    for view in ALL_VIEWS:
        try:
            metrics = run_backtest_for_view(
                con, schema, view, holding_period_days=8
            )
            scorecard_rows.append(metrics)
        except Exception as ex:
            logger.warning("Could not execute view '%s': %s", view, ex)

    if scorecard_rows:
        scorecard_df = pd.DataFrame(scorecard_rows)
        logger.info(
            "STRATEGY SCORECARD MATRIX (PHASE 4 COMPLETE)\n%s",
            scorecard_df.to_string(index=False),
        )

    validator = SignalEdgeValidator(
        con, schema, benchmark_ticker=benchmark
    )
    df_summary = validator.run_validation(
        max_holding_days=max_holding_days, mc_iterations=mc_iterations
    )
    if df_summary.empty:
        edge_validation: Dict[str, Any] = {"status": "NO_DATA"}
    else:
        edge_validation = df_summary.to_dict(orient="records")[0]

    return {
        "rows_processed": perf["rows_processed"],
        "max_holding_days": perf["max_holding_days"],
        "execution_status_integrity": perf["execution_status_integrity"],
        "excel_report": perf["excel_report"],
        "scorecard": scorecard_rows,
        "edge_validation": edge_validation,
    }


# =====================================================================
# Phase 5 — multi-dimensional matrix analytics (phase5_analytics_matrix.py)
# =====================================================================


class Phase5MatrixAnalyzer:
    """Verbatim port of ``Phase5MatrixAnalyzer`` onto the shared service
    connection; all references retargeted to
    ``{schema}.aly_signal_performance`` / ``{schema}.aly_signal_records_raw``."""

    def __init__(self, con, schema: str):
        _check_schema(schema)
        self.con = con
        self.schema = schema

    def load_analytics_dataset(self) -> pd.DataFrame:
        schema = self.schema
        query = f"""
            SELECT
                p.Ticker,
                p.Trading_Date,
                p.Model,
                CAST(p.Cluster AS VARCHAR) AS Cluster,
                s.Reward_Risk_Ratio,
                s.Current_Price,
                s.Target,
                s.Stop_Price,
                s.ATR,
                ROUND((s.Target - s.Current_Price) / s.Current_Price * 100.0, 2) AS Target_Pct,
                ROUND((s.Current_Price - s.Stop_Price) / s.Current_Price * 100.0, 2) AS Stop_Pct,
                p.Execution_Status,
                CASE
                    WHEN p.Hit_Target_Date IS NOT NULL THEN (s.Target - s.Current_Price) / s.Current_Price * 100.0
                    WHEN p.Hit_Stop_Date IS NOT NULL THEN (s.Stop_Price - s.Current_Price) / s.Current_Price * 100.0
                    ELSE (p.Time_Exit_Close - s.Current_Price) / s.Current_Price * 100.0
                END AS Trade_Return_Pct
            FROM {schema}.{PERF_TABLE} p
            JOIN {schema}.{RAW_TABLE} s
              ON p.Ticker = s.Ticker
             AND p.Trading_Date = s.Trading_Date
             AND p.Cluster = COALESCE(s.Cluster, 0)
             AND p.Model = COALESCE(s.Model, 'UNKNOWN')
             AND p.Signal_File = COALESCE(s.Signal_File, 'UNKNOWN');
        """
        df = self.con.execute(query).df()

        if df.empty:
            logger.warning("No dataset returned for Phase 5 matrix analysis.")
            return df

        # 1. Fixed Target Percentage Binning
        df["Target_Pct_Bin"] = pd.cut(
            df["Target_Pct"],
            bins=[-np.inf, 2.0, 5.0, 10.0, np.inf],
            labels=["0-2%", "2-5%", "5-10%", "10%+"],
        )

        # 2. ATR-based Target Distance Binning: (Target - Current_Price) / ATR
        # Fallback to Stop distance if ATR column is missing/null
        if "ATR" in df.columns and not df["ATR"].isna().all():
            df["Target_ATR_Multiple"] = (df["Target"] - df["Current_Price"]) / df["ATR"]
        else:
            # Fallback estimation using Stop distance as 1 ATR equivalent if ATR is absent
            df["Target_ATR_Multiple"] = (df["Target"] - df["Current_Price"]) / (df["Current_Price"] - df["Stop_Price"]).replace(0, np.nan)

        df["Target_ATR_Bin"] = pd.cut(
            df["Target_ATR_Multiple"],
            bins=[-np.inf, 1.0, 2.0, 3.0, np.inf],
            labels=["<1.0x ATR", "1.0-2.0x ATR", "2.0-3.0x ATR", "3.0x+ ATR"],
        )

        # Reward/Risk Ratio Binning
        df["RR_Bin"] = pd.cut(
            df["Reward_Risk_Ratio"],
            bins=[-np.inf, 1.0, 1.5, 2.0, np.inf],
            labels=["<1.0", "1.0-1.5", "1.5-2.0", "2.0+"],
        )
        return df

    def calculate_trend_decay(
        self, df: pd.DataFrame, window_size: int = 20, recent_days: int = 15
    ) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame()

        df_work = df.copy()
        df_work["Is_Win"] = (df_work["Trade_Return_Pct"] > 0).astype(int)
        df_work["Trading_Date"] = pd.to_datetime(df_work["Trading_Date"])
        df_work = df_work.sort_values(by=["Model", "Cluster", "Trading_Date"])

        # Compute Rolling Win Rate per group
        df_work["Rolling_Win_Rate"] = df_work.groupby(["Model", "Cluster"])["Is_Win"] \
                                             .transform(lambda x: x.rolling(window=window_size, min_periods=5).mean() * 100.0)

        max_date = df_work["Trading_Date"].max()
        cutoff_recent = max_date - pd.Timedelta(days=recent_days)

        summary_rows = []
        # justified: per-(Model,Cluster) group aggregation — each group
        # produces one summary dict with conditional status logic; the
        # inner ops (rolling, mean) are vectorized, but the per-group
        # branch logic is inherently group-wise (verbatim legacy).
        for (model, cluster), group in df_work.groupby(["Model", "Cluster"]):
            total_trades = len(group)
            hist_winrate = group["Is_Win"].mean() * 100.0

            recent_group = group[group["Trading_Date"] >= cutoff_recent]
            recent_trades = len(recent_group)
            recent_winrate = recent_group["Is_Win"].mean() * 100.0 if recent_trades > 0 else hist_winrate

            delta_winrate = recent_winrate - hist_winrate
            latest_rolling_winrate = group["Rolling_Win_Rate"].iloc[-1] if not group["Rolling_Win_Rate"].isna().all() else hist_winrate

            if recent_trades >= 3 and delta_winrate <= -10.0:
                status = "⚠️ DECAY DETECTED (CAUTION)"
            elif recent_trades >= 3 and delta_winrate >= 5.0:
                status = "🔥 STRONG REGIME"
            else:
                status = "✅ STABLE"

            summary_rows.append({
                "Model": model,
                "Cluster": str(cluster),
                "Total_Trades": total_trades,
                "Recent_Trades_15D": recent_trades,
                "Lifetime_WinRate_%": round(hist_winrate, 2),
                "Recent_15D_WinRate_%": round(recent_winrate, 2),
                "Latest_Rolling_WinRate_%": round(latest_rolling_winrate, 2),
                "WinRate_Delta_%": round(delta_winrate, 2),
                "Status_Alert": status
            })

        df_decay = pd.DataFrame(summary_rows)
        return df_decay.sort_values(by="WinRate_Delta_%", ascending=True)

    def generate_matrix_report(self, output_dir: str) -> Dict[str, Any]:
        """Write the three heatmap PNGs + ``RPT_phase5_analytics_matrix.xlsx``
        (4 sheets) into ``output_dir``.  Returns ``{status, files, rows}``
        (``NO_DATA`` when the dataset is empty — legacy returned early)."""
        df = self.load_analytics_dataset()
        if df.empty:
            return {"status": "NO_DATA", "files": [], "rows": 0}

        os.makedirs(output_dir, exist_ok=True)

        # 1. Base Metrics: Model x Cluster
        pivot_model_cluster = self._calculate_grouped_metrics(df, ["Model", "Cluster"])

        # 2. Base Metrics: Target_Pct_Bin (Fixed %) x RR_Bin
        pivot_target_rr = self._calculate_grouped_metrics(df, ["Target_Pct_Bin", "RR_Bin"])

        # 3. Base Metrics: Target_ATR_Bin (ATR Multiples) x RR_Bin
        pivot_target_atr_rr = self._calculate_grouped_metrics(df, ["Target_ATR_Bin", "RR_Bin"])

        # 4. Performance Trend & Strategy Decay Analysis
        df_trend_decay = self.calculate_trend_decay(df)

        # --- COMBINE 'Model_vs_Cluster' & 'Trend_Decay_Analysis' ---
        # Ensure 'Cluster' column is consistent data type for clean merging
        pivot_model_cluster["Cluster"] = pivot_model_cluster["Cluster"].astype(str)
        df_trend_decay["Cluster"] = df_trend_decay["Cluster"].astype(str)

        # Drop duplicate metrics from df_trend_decay before merging
        decay_metrics = df_trend_decay.drop(columns=["Total_Trades", "Lifetime_WinRate_%"], errors="ignore")

        # Merge statistical performance metrics with trend/decay features
        combined_model_cluster = pd.merge(
            pivot_model_cluster,
            decay_metrics,
            on=["Model", "Cluster"],
            how="left",
        )

        files: List[str] = []

        # --- Render Heatmaps ---
        winrate_matrix = pivot_model_cluster.pivot(index="Model", columns="Cluster", values="Win_Rate_Pct")
        path = os.path.join(output_dir, "heatmap_winrate_model_cluster.png")
        if self._plot_heatmap(
            pivot_data=winrate_matrix,
            title="Win Rate (%) by Model and Cluster",
            save_path=path,
            fmt=".1f",
        ):
            files.append(path)

        expectancy_matrix = pivot_target_rr.pivot(index="Target_Pct_Bin", columns="RR_Bin", values="Expectancy_Pct")
        path = os.path.join(output_dir, "heatmap_expectancy_target_rr.png")
        if self._plot_heatmap(
            pivot_data=expectancy_matrix,
            title="Expectancy (%) by Target Pct and Reward/Risk Ratio",
            save_path=path,
            fmt=".2f",
        ):
            files.append(path)

        expectancy_atr_matrix = pivot_target_atr_rr.pivot(index="Target_ATR_Bin", columns="RR_Bin", values="Expectancy_Pct")
        path = os.path.join(output_dir, "heatmap_expectancy_target_atr_rr.png")
        if self._plot_heatmap(
            pivot_data=expectancy_atr_matrix,
            title="Expectancy (%) by Target ATR Multiple and Reward/Risk Ratio",
            save_path=path,
            fmt=".2f",
        ):
            files.append(path)

        # --- Export Multi-Tab Matrix Excel Report ---
        excel_path = os.path.join(output_dir, "RPT_phase5_analytics_matrix.xlsx")
        with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
            combined_model_cluster.to_excel(writer, sheet_name="Model_vs_Cluster", index=False)
            pivot_target_rr.to_excel(writer, sheet_name="Target_vs_RR", index=False)
            pivot_target_atr_rr.to_excel(writer, sheet_name="Target_ATR_vs_RR", index=False)
            df.to_excel(writer, sheet_name="Raw_Evaluated_Data", index=False)
        files.append(excel_path)

        logger.info(
            "Heatmaps generated and Phase 5 Excel Matrix exported: %s",
            excel_path,
        )
        return {
            "status": "SUCCESS",
            "files": files,
            "rows": int(len(df)),
        }

    def _calculate_grouped_metrics(self, df: pd.DataFrame, group_cols: list) -> pd.DataFrame:
        grouped = df.groupby(group_cols, observed=False).apply(self._metrics_aggregator, include_groups=False).reset_index()
        return grouped

    @staticmethod
    def _metrics_aggregator(group: pd.DataFrame) -> pd.Series:
        total_trades = len(group)
        if total_trades == 0:
            return pd.Series({
                "Total_Trades": 0, "Win_Rate_Pct": 0.0, "Avg_Win_Pct": 0.0, "Avg_Loss_Pct": 0.0, "Expectancy_Pct": 0.0, "Profit_Factor": 0.0
            })

        wins = group[group["Trade_Return_Pct"] > 0]["Trade_Return_Pct"]
        losses = group[group["Trade_Return_Pct"] < 0]["Trade_Return_Pct"]

        win_rate = (len(wins) / total_trades) * 100.0
        avg_win = wins.mean() if len(wins) > 0 else 0.0
        avg_loss = abs(losses.mean()) if len(losses) > 0 else 0.0

        expectancy = ((win_rate / 100.0) * avg_win) - ((1 - (win_rate / 100.0)) * avg_loss)
        profit_factor = wins.sum() / abs(losses.sum()) if abs(losses.sum()) > 0 else np.nan

        return pd.Series({
            "Total_Trades": total_trades,
            "Win_Rate_Pct": round(win_rate, 2),
            "Avg_Win_Pct": round(avg_win, 2),
            "Avg_Loss_Pct": round(avg_loss, 2),
            "Expectancy_Pct": round(expectancy, 2),
            "Profit_Factor": round(profit_factor, 2) if pd.notna(profit_factor) else 0.0
        })

    @staticmethod
    def _plot_heatmap(pivot_data: pd.DataFrame, title: str, save_path: str, fmt: str = ".1f") -> bool:
        """Render one heatmap PNG.  Returns True when a file was written.

        matplotlib/seaborn are lazily imported and the Agg backend is
        forced — the service runs headless under the Windows Task
        Scheduler (same constraint as F8-T2's stdio guard).
        """
        if pivot_data.dropna(how="all").empty:
            logger.warning(
                "Skipping heatmap generation for '%s' (empty data matrix).",
                title,
            )
            return False

        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import seaborn as sns

        plt.figure(figsize=(10, 6))
        sns.heatmap(
            pivot_data,
            annot=True,
            fmt=fmt,
            cmap="RdYlGn",
            center=0 if "Expectancy" in title else None,
            linewidths=0.5,
        )
        plt.title(title, fontsize=12, fontweight="bold")
        plt.tight_layout()
        plt.savefig(save_path, dpi=300)
        plt.close()
        return True


def run_phase5(con, schema: str, report_dir: str) -> Dict[str, Any]:
    """Standard entry point (legacy ``run_phase_5``): run the matrix
    analyzer and write outputs under ``report_dir``."""
    analyzer = Phase5MatrixAnalyzer(con, schema)
    return analyzer.generate_matrix_report(output_dir=report_dir)


__all__ = [
    "ALL_VIEWS",
    "DEFAULT_PARAMETER_SETS",
    "PERF_TABLE",
    "RAW_TABLE",
    "Phase5MatrixAnalyzer",
    "SignalEdgeValidator",
    "SignalQueryEngine",
    "build_views",
    "calculate_and_save_performance",
    "deduplicate_signals",
    "ensure_schema_up_to_date",
    "generate_performance_excel_report",
    "ingest_signals",
    "is_intraday_signal",
    "repair_missing_close_prices",
    "run_backtest",
    "run_backtest_for_view",
    "run_phase5",
    "schema_for_set",
    "setup_raw_table_schema",
    "verify_execution_status_integrity",
]
