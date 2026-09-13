"""Generate ``Report_Top_25_Active.xlsx`` — top-25 tickers by average
dollar-volume over their last 30 trading periods.

The query runs against the ``daily_close`` table.  Preferred path is the
running Arrow Flight service (``do_get`` with a JSON ``{"sql": ...}``
ticket on ``grpc://{server.flight_host}:{server.flight_port}``) so the
report works even while the service holds the DuckDB write lock.  When
the Flight server is unreachable (or the Flight query fails for any
reason) the script falls back to a direct *read-only* DuckDB connection
(``duckdb.connect(path, read_only=True)``), which only succeeds when no
process holds the write lock.  If both paths fail the error is logged
and the script exits ``1``; success exits ``0``.

The workbook is written to ``dirname(config.logging.log_file)`` — with
the shipped ``src/config.yaml`` that is ``C:\\Intel\\parquet`` — via
pandas + openpyxl, and every column width is auto-adjusted to the
maximum content length plus a small padding.

``scripts/daily_load_once.py`` chains :func:`run_report` after every
successful daily load (SUCCESS or PARTIAL via either the Flight action
or the direct fallback path); a report failure is logged there but never
changes the load's exit code.

Can also be run manually:

    python scripts/report_top25_active.py
"""

from __future__ import annotations

import json
import logging
import os
import sys
from typing import Optional

# Ensure src/ is importable regardless of the caller's working directory
# (mirrors scripts/daily_load_once.py).
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _REPO_ROOT)

_CONFIG_PATH = os.path.join(_REPO_ROOT, "src", "config.yaml")
_FALLBACK_LOG = os.path.join(
    _REPO_ROOT, "logs", "report_top25_active_error.log"
)

import duckdb
import pandas as pd
import pyarrow.flight as flight
from pythonjsonlogger import json as jsonlogger

from src.config import PipelineConfig

logger = logging.getLogger(__name__)

REPORT_FILENAME = "Report_Top_25_Active.xlsx"
_REPORT_SHEET = "Top 25 Active"

_TEXT_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
_JSON_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"

# Short probe answering "is the Flight service up?" — kept small so a
# stopped service does not stall the report before the direct read-only
# fallback takes over (mirrors daily_load_once._FLIGHT_CONNECT_TIMEOUT_S).
_FLIGHT_CONNECT_TIMEOUT_S = 5

# Bound on the do_get call itself so a wedged-but-connected server
# cannot hang the report — or a chained daily_load_once — forever.
# 120 s mirrors the server's own query_timeout_seconds default
# (src/config.py ServerConfig): a query legitimately needing longer
# would already be killed server-side at the same horizon, and the
# top-25 query takes well under a second on the live database.
_FLIGHT_GET_TIMEOUT_S = 120

# Padding added to each auto-sized column so content is not flush
# against the cell edge in Excel.
_WIDTH_PADDING = 2

# PM-supplied query, adapted: LIMIT 25 and friendly output headers
# ("Ticker", "Avg Trade Money", "% of Total").
TOP25_SQL = """
WITH trade_money AS (
  SELECT Ticker, Date,
    ((Open + High + Low + "Close") / 4.0) * Volume AS trade_money
  FROM daily_close
),
last_30_periods AS (
  SELECT Ticker, trade_money,
    ROW_NUMBER() OVER (PARTITION BY Ticker ORDER BY Date DESC) AS period_rank
  FROM trade_money
),
avg_trade_money AS (
  SELECT Ticker, AVG(trade_money) AS avg_trade_money
  FROM last_30_periods WHERE period_rank <= 30 GROUP BY Ticker
),
total_trade_money AS (
  SELECT SUM(avg_trade_money) AS total FROM avg_trade_money
)
SELECT a.Ticker AS "Ticker",
  a.avg_trade_money AS "Avg Trade Money",
  ROUND((a.avg_trade_money / t.total) * 100, 2) AS "% of Total"
FROM avg_trade_money a CROSS JOIN total_trade_money t
ORDER BY a.avg_trade_money DESC
LIMIT 25
"""


def _configure_logging(config: PipelineConfig) -> None:
    """Log to ``cfg.logging.log_file`` (Task Scheduler has no console).

    Honors ``cfg.logging.log_format`` the same way
    ``scripts/daily_load_once.py`` does.  When the report is chained
    from ``daily_load_once`` the root logger is already configured, so
    this is only needed for standalone runs — it is a no-op if the root
    logger already has handlers.
    """
    root = logging.getLogger()
    if root.handlers:
        return
    log_file = config.logging.log_file
    log_dir = os.path.dirname(log_file)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)

    if config.logging.log_format == "json":
        formatter: logging.Formatter = jsonlogger.JsonFormatter(_JSON_FORMAT)
    else:
        formatter = logging.Formatter(_TEXT_FORMAT)

    root.setLevel(logging.INFO)
    handler = logging.FileHandler(log_file, encoding="utf-8")
    handler.setFormatter(formatter)
    root.addHandler(handler)


def _configure_fallback_logging() -> None:
    """Last-resort logging when the config (and thus ``log_file``)
    cannot be loaded — writes to a repo-local file plus stderr."""
    os.makedirs(os.path.dirname(_FALLBACK_LOG), exist_ok=True)
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    file_handler = logging.FileHandler(_FALLBACK_LOG, encoding="utf-8")
    file_handler.setFormatter(logging.Formatter(_TEXT_FORMAT))
    root.addHandler(file_handler)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(logging.Formatter(_TEXT_FORMAT))
    root.addHandler(stream_handler)


def _query_via_flight(location: str) -> pd.DataFrame:
    """Run ``TOP25_SQL`` through the Flight service's ``do_get``.

    Sends a JSON ``{"sql": ...}`` ticket — the same contract the Flight
    server's ``do_get`` implements — after a short
    ``wait_for_available`` probe.  The call is bounded by
    ``_FLIGHT_GET_TIMEOUT_S`` so a wedged server cannot hang the
    report.  Raises on any failure (unreachable server, rejected
    ticket, call timeout, mid-call drop); the caller decides whether
    to fall back.
    """
    client = flight.FlightClient(location)
    client.wait_for_available(timeout=_FLIGHT_CONNECT_TIMEOUT_S)
    ticket = flight.Ticket(json.dumps({"sql": TOP25_SQL}).encode("utf-8"))
    options = flight.FlightCallOptions(timeout=_FLIGHT_GET_TIMEOUT_S)
    reader = client.do_get(ticket, options=options)
    return reader.read_all().to_pandas()


def _query_via_duckdb(db_path: str) -> pd.DataFrame:
    """Run ``TOP25_SQL`` on a direct *read-only* DuckDB connection.

    Only usable when no process holds the database write lock (i.e. the
    Flight service / ingestion daemon is not running).
    """
    conn = duckdb.connect(db_path, read_only=True)
    try:
        return conn.execute(TOP25_SQL).df()
    finally:
        conn.close()


def _fetch_dataframe(config: PipelineConfig) -> Optional[pd.DataFrame]:
    """Fetch the report rows: Flight first, direct read-only DB second.

    Returns ``None`` when both paths fail (each failure is logged).
    """
    location = (
        f"grpc://{config.server.flight_host}:{config.server.flight_port}"
    )
    try:
        return _query_via_flight(location)
    except Exception as exc:
        logger.warning(
            "Flight server at %s unreachable or query failed (%s); "
            "falling back to a direct read-only DuckDB query.",
            location,
            exc,
        )

    try:
        return _query_via_duckdb(config.db_path)
    except Exception:
        logger.exception(
            "Direct read-only DuckDB query also failed "
            "(write lock held or database unavailable)."
        )
        return None


def _report_path(config: PipelineConfig) -> str:
    """Destination: ``dirname(config.logging.log_file)``."""
    log_dir = os.path.dirname(config.logging.log_file)
    return os.path.join(log_dir or ".", REPORT_FILENAME)


def _write_xlsx(df: pd.DataFrame, out_path: str) -> None:
    """Write ``df`` to ``out_path`` and auto-adjust column widths.

    Each column's width is set to the maximum rendered content length
    (header included) plus ``_WIDTH_PADDING``.
    """
    out_dir = os.path.dirname(out_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=_REPORT_SHEET)
        worksheet = writer.sheets[_REPORT_SHEET]
        # justified: openpyxl exposes no vectorized API for per-column
        # width sizing — each column's cells must be measured one column
        # at a time (25-row sheet, negligible cost).
        for column_cells in worksheet.columns:
            max_len = max(
                len(str(cell.value)) if cell.value is not None else 0
                for cell in column_cells
            )
            letter = column_cells[0].column_letter
            worksheet.column_dimensions[letter].width = (
                max_len + _WIDTH_PADDING
            )


def run_report(config: PipelineConfig) -> int:
    """Generate ``Report_Top_25_Active.xlsx``; return 0 on success, 1 on
    failure.

    Queries the running Flight server first (works while the service
    holds the DuckDB write lock); on any Flight-path failure falls back
    to a direct read-only DuckDB connection.  If both fail — or the xlsx
    cannot be written — the error is logged and 1 is returned.
    """
    df = _fetch_dataframe(config)
    if df is None:
        logger.error("Could not fetch report data; %s not written.",
                     REPORT_FILENAME)
        return 1

    out_path = _report_path(config)
    try:
        _write_xlsx(df, out_path)
    except Exception:
        logger.exception("Failed to write %s.", out_path)
        return 1

    logger.info("Wrote %s (%d rows).", out_path, len(df))
    return 0


def main() -> int:
    """Standalone entrypoint: load config, configure logging, run."""
    try:
        config = PipelineConfig.from_yaml(_CONFIG_PATH)
        _configure_logging(config)
    except Exception:
        # Config or log_file unavailable — fall back to a repo-local
        # file so the traceback is not lost to a discarded stderr.
        _configure_fallback_logging()
        logger.exception("report_top25_active failed during startup.")
        return 1

    try:
        return run_report(config)
    except Exception:
        # Uncaught crash: land the full traceback in the configured log
        # file, then exit non-zero.
        logger.exception(
            "report_top25_active crashed with an uncaught exception."
        )
        return 1


if __name__ == "__main__":
    sys.exit(main())
