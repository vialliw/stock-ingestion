"""Apache Arrow Flight daemon for the stock-ingestion pipeline.

Implements ``MarketDataFlightServer`` per spec §10 (reference
implementation): a ``flight.FlightServerBase`` subclass that exposes a
read-only ``do_get`` endpoint (SELECT-only SQL validated before reaching
DuckDB) and a ``do_action`` endpoint routing four administrative actions
(initial_load, daily_load, on_request_load, daily_report).

Spec reference: docs/tech_spec/gemini_tech_spec.1.0.2.md §5, §10.
"""

from __future__ import annotations

import json
import logging
import os
import queue
import threading
from datetime import datetime

import pandas as pd
import pyarrow as pa
import pyarrow.flight as flight

from src.r2_sync import R2SyncManager

logger = logging.getLogger(__name__)


# =====================================================================
# Module-level helpers
# =====================================================================

def _validate_select_only(sql: str) -> None:
    """Reject non-SELECT/non-WITH statements for ``do_get``.

    Strips leading/trailing whitespace, uppercases the string, and
    verifies it starts with ``SELECT`` or ``WITH`` (CTE).  Raises
    ``ValueError`` for any other statement type so that ``do_get`` can
    translate it into a ``FlightServerError``.
    """
    stripped = sql.strip().upper()
    if not stripped.startswith("SELECT") and not stripped.startswith("WITH"):
        raise ValueError(
            "Only SELECT and WITH (CTE) statements are allowed via do_get"
        )


def _truncate_sql(sql: str, max_len: int = 200) -> str:
    """Truncate SQL for logging to avoid large/sensitive log entries."""
    sql = sql.strip()
    if len(sql) <= max_len:
        return sql
    return sql[:max_len] + "...[truncated]"


class _ActionResult(flight.Result):
    """A ``flight.Result`` that is also iterable (yields itself).

    The pyarrow server-side ``_do_action`` callback calls ``iter()`` on
    the value returned by ``do_action``; a bare ``flight.Result`` is not
    iterable and raises ``TypeError: 'Result' object is not iterable``
    over real gRPC (F7-T17 / finding F-A).  Returning this subclass
    satisfies the wire contract while keeping ``.body`` accessible to
    direct (in-process) callers — several test helpers invoke
    ``server.do_action(...)`` without a gRPC transport.
    """

    def __iter__(self):
        return iter([self])


# Expected Arrow schema for do_put uploads to the daily_close table.
_DAILY_CLOSE_SCHEMA = pa.schema(
    [
        pa.field("Date", pa.date32()),
        pa.field("Ticker", pa.string()),
        pa.field("Open", pa.float64()),
        pa.field("High", pa.float64()),
        pa.field("Low", pa.float64()),
        pa.field("Close", pa.float64()),
        pa.field("Volume", pa.float64()),
    ]
)


def _validate_daily_close_schema(schema: pa.Schema) -> None:
    """Raise ``FlightServerError`` if ``schema`` does not exactly match
    the ``daily_close`` column layout and types."""
    expected_names = _DAILY_CLOSE_SCHEMA.names
    if schema.names != expected_names:
        raise flight.FlightServerError(
            f"Schema mismatch: expected columns {expected_names}, got {schema.names}"
        )
    for field, expected in zip(schema, _DAILY_CLOSE_SCHEMA):
        if not field.type.equals(expected.type):
            raise flight.FlightServerError(
                f"Schema mismatch for {field.name}: expected {expected.type}, "
                f"got {field.type}"
            )


# =====================================================================
# Arrow Flight server
# =====================================================================

class MarketDataFlightServer(flight.FlightServerBase):
    """Arrow Flight server exposing read-only SQL and admin actions.

    ``do_get`` accepts a JSON ticket containing an ``sql`` field,
    validates it is SELECT/WITH-only, executes it via
    ``DatabaseManager.execute_query`` and returns a
    ``flight.RecordBatchStream``.

    ``do_action`` routes four action types to the
    ``IngestionEngine`` / ``DatabaseManager`` and returns a
    ``flight.Result`` containing a JSON status payload.
    """

    def __init__(self, location: str, db_mgr, ingest_engine, cfg):
        super().__init__(location)
        self.db = db_mgr
        self.engine = ingest_engine
        self.cfg = cfg
        self._start_time = datetime.now()

    # ------------------------------------------------------------------
    # Embedded data-health check (F9-T5) — report-only
    # ------------------------------------------------------------------

    def _health_report(
        self, include_gaps: bool = False, gap_days=None
    ) -> dict:
        """Run the ``daily_close`` health checks; NEVER raises.

        Returns the report dict.  Any failure — including a non-dict
        return from the check itself — yields
        ``health_status == 'CHECK_FAILED'`` so a broken check can never
        mask or crash a completed load.  Logs a WARNING whenever the
        status is not 'OK'.
        """
        try:
            ingestion_cfg = getattr(self.cfg, "ingestion", None)
            tickers = list(getattr(ingestion_cfg, "tickers", None) or [])
            excluded = set(
                getattr(ingestion_cfg, "exclude_tickers", None) or []
            )
            configured = [t for t in tickers if t not in excluded]
            kwargs = {"tickers": configured, "include_gaps": include_gaps}
            if gap_days is not None:
                kwargs["gap_days"] = gap_days
            report = self.db.check_daily_close_health(**kwargs)
            if not isinstance(report, dict):
                raise TypeError(
                    "check_daily_close_health returned "
                    f"{type(report).__name__}, expected dict"
                )
        except Exception as exc:
            report = {"health_status": "CHECK_FAILED", "error": str(exc)}
        status = report.get("health_status")
        if status != "OK":
            logger.warning(
                "daily_close health check reported health_status=%s",
                status,
            )
        return report

    # ------------------------------------------------------------------
    # Read path
    # ------------------------------------------------------------------

    def _execute_query_with_timeout(self, sql_query: str, timeout_seconds: float):
        """Run ``sql_query`` through ``self.db`` with a hard timeout.

        The query is handed to DuckDB unchanged (no LIMIT injection).  A
        worker thread runs ``db.execute_query`` while a ``threading.Timer``
        enforces the deadline.  If the timer fires first, ``TimeoutError``
        is returned to the caller.
        """
        result_queue: queue.Queue = queue.Queue()

        def _worker():
            try:
                result_queue.put(self.db.execute_query(sql_query))
            except Exception as exc:
                result_queue.put(exc)

        def _timeout():
            result_queue.put(
                TimeoutError(
                    f"Query exceeded {timeout_seconds} second timeout"
                )
            )

        worker = threading.Thread(target=_worker, daemon=True)
        timer = threading.Timer(timeout_seconds, _timeout)
        worker.start()
        timer.start()
        try:
            result = result_queue.get()
        finally:
            timer.cancel()
        if isinstance(result, Exception):
            raise result
        return result

    def do_get(self, context, ticket):
        try:
            req = json.loads(ticket.ticket.decode("utf-8"))
            sql_query = req.get("sql")
            if not sql_query:
                raise flight.FlightServerError("Missing 'sql' field in ticket")
            _validate_select_only(sql_query)
            logger.info("Serving Arrow Flight query: %s", _truncate_sql(sql_query))
            timeout = getattr(self.cfg.server, "query_timeout_seconds", 120)
            if not isinstance(timeout, (int, float)) or isinstance(timeout, bool):
                timeout = 120
            arrow_table = self._execute_query_with_timeout(sql_query, timeout)
            return flight.RecordBatchStream(arrow_table)
        except flight.FlightServerError:
            raise
        except TimeoutError as e:
            logger.warning("do_get query timed out after %s seconds", timeout)
            raise flight.FlightServerError(
                f"Query timed out after {timeout} seconds"
            ) from e
        except ValueError as e:
            logger.warning("Rejected do_get query: %s", e)
            raise flight.FlightServerError(str(e))
        except Exception as e:
            logger.error("Failed to execute Flight do_get query: %s", e)
            raise flight.FlightServerError(f"Query Execution Error: {str(e)}")

    # ------------------------------------------------------------------
    # Action path
    # ------------------------------------------------------------------

    def do_action(self, context, action):
        action_type = action.type
        payload = (
            json.loads(action.body.to_pybytes().decode("utf-8"))
            if action.body
            else {}
        )

        if action_type == "initial_load":
            # years=None (absent key) -> the engine applies the
            # configured ingestion.initial_load_years depth.  An
            # invalid explicit value (non-int, bool, or outside
            # [1, 30]) raises ValueError in the engine — surface it as
            # a FlightServerError instead of a silent empty download.
            years = payload.get("years")
            try:
                result = self.engine.run_initial_load(years=years)
            except ValueError as e:
                raise flight.FlightServerError(str(e)) from e
            status = "SUCCESS" if not result["tickers_failed"] else "PARTIAL"
            # F9-T5: merge the light (no-gap) health report — report-only,
            # never changes the load's status.
            result["health"] = self._health_report()
            return _ActionResult(
                json.dumps({"status": status, **result}).encode("utf-8")
            )

        elif action_type == "daily_load":
            result = self.engine.run_daily_load(
                lookback_days=self.cfg.ingestion.daily_lookback_days,
                force=payload.get("force", False),
            )
            if result.get("status") == "SKIPPED":
                return _ActionResult(json.dumps(result).encode("utf-8"))
            # F9-T5: merge the light (no-gap) health report — report-only,
            # never changes the load's status.
            result["health"] = self._health_report()
            status = "SUCCESS" if not result["tickers_failed"] else "PARTIAL"
            return _ActionResult(
                json.dumps({"status": status, **result}).encode("utf-8")
            )

        elif action_type == "on_request_load":
            result = self.engine.run_on_request_load(
                tickers=payload["tickers"],
                start_date=payload["start_date"],
                end_date=payload["end_date"],
            )
            status = "SUCCESS" if not result["tickers_failed"] else "PARTIAL"
            return _ActionResult(
                json.dumps({"status": status, **result}).encode("utf-8")
            )

        elif action_type == "daily_report":
            start_time = datetime.now()
            target_date = payload.get("date")
            report_data = self.db.generate_top_20_dollar_volume(target_date)
            out_dir = self.cfg.reports.report_dir
            os.makedirs(out_dir, exist_ok=True)
            file_name = (
                f"dollar_volume_"
                f"{target_date or datetime.now().strftime('%Y%m%d')}.json"
            )
            with open(os.path.join(out_dir, file_name), "w") as f:
                json.dump(report_data, f, indent=2)
            duration_ms = int(
                (datetime.now() - start_time).total_seconds() * 1000
            )
            return _ActionResult(
                json.dumps(
                    {
                        "status": "SUCCESS",
                        "report": report_data,
                        "duration_ms": duration_ms,
                    }
                ).encode("utf-8")
            )

        elif action_type == "health_check":
            health = self.db.get_health_info()
            uptime_seconds = int(
                (datetime.now() - self._start_time).total_seconds()
            )
            status = "SUCCESS" if health.get("db_connected") else "DEGRADED"
            return _ActionResult(
                json.dumps(
                    {
                        "status": status,
                        "db_connected": health.get("db_connected", False),
                        "db_path": self.cfg.db_path,
                        "ticker_count": health.get("ticker_count", 0),
                        "last_run": health.get("last_run"),
                        "uptime_seconds": uptime_seconds,
                        "daily_close_count": health.get("daily_close_count", 0),
                        "splits_processed_count": health.get(
                            "splits_processed_count", 0
                        ),
                        "ingestion_runs_count": health.get(
                            "ingestion_runs_count", 0
                        ),
                        "schema_version": health.get(
                            "schema_version", 0
                        ),
                    }
                ).encode("utf-8")
            )

        elif action_type == "data_health":
            # F9-T5: FULL daily_close health report (incl. NYSE gap
            # detection).  Payload may carry "gap_days" (int >= 1) to
            # size the trailing-session gap window.
            gap_days = payload.get("gap_days")
            if gap_days is not None and (
                not isinstance(gap_days, int)
                or isinstance(gap_days, bool)
                or gap_days < 1
            ):
                raise flight.FlightServerError(
                    f"gap_days must be an integer >= 1, got {gap_days!r}"
                )
            health = self._health_report(
                include_gaps=True, gap_days=gap_days
            )
            return _ActionResult(
                json.dumps({"status": "SUCCESS", "health": health}).encode(
                    "utf-8"
                )
            )

        elif action_type == "get_run_history":
            arrow_table = self.db.get_ingestion_runs()
            # Convert the Arrow table to a JSON-serialisable list of rows.
            # Uses pandas vectorised conversion; no Python row loops.
            if arrow_table.num_rows > 0:
                run_history_json = arrow_table.to_pandas().to_json(
                    orient="records", date_format="iso"
                )
                run_history = json.loads(run_history_json)
            else:
                run_history = []
            return _ActionResult(
                json.dumps(
                    {"status": "SUCCESS", "run_history": run_history}
                ).encode("utf-8")
            )

        elif action_type == "sync_to_r2":
            result = R2SyncManager(self.cfg).sync(self.cfg.db_path)
            return _ActionResult(json.dumps(result).encode("utf-8"))

        else:
            raise flight.FlightServerError(f"Unknown action: {action_type}")

    # ------------------------------------------------------------------
    # Write path
    # ------------------------------------------------------------------

    def do_put(self, context, descriptor, reader, writer):
        """Receive an Arrow stream and upsert it into ``daily_close``.

        The stream is read in full, validated against the ``daily_close``
        schema, filtered for any tickers in ``cfg.ingestion.exclude_tickers``,
        and then upserted in a single vectorized operation via
        ``DatabaseManager.upsert_dataframe``.
        """
        try:
            table = reader.read_all()
        except Exception as e:
            logger.error("Failed to read Arrow Flight do_put stream: %s", e)
            raise flight.FlightServerError(
                f"Failed to read do_put stream: {e}"
            ) from e

        try:
            _validate_daily_close_schema(table.schema)
        except flight.FlightServerError:
            raise

        df = table.to_pandas()

        exclude_tickers = []
        ingestion_cfg = getattr(self.cfg, "ingestion", None)
        if ingestion_cfg is not None:
            raw_excluded = getattr(ingestion_cfg, "exclude_tickers", None)
            if isinstance(raw_excluded, (list, tuple, set)):
                exclude_tickers = list(raw_excluded)

        if exclude_tickers:
            mask = df["Ticker"].isin(exclude_tickers)
            excluded_count = int(mask.sum())
            if excluded_count:
                logger.warning(
                    "do_put filtered %s row(s) for excluded tickers %s",
                    excluded_count,
                    exclude_tickers,
                )
                df = df.loc[~mask].copy()

        rows_upserted = self.db.upsert_dataframe(df)
        logger.info("do_put upserted %s rows into daily_close", rows_upserted)


__all__ = [
    "MarketDataFlightServer",
    "_validate_select_only",
    "_truncate_sql",
    "_validate_daily_close_schema",
    "_DAILY_CLOSE_SCHEMA",
]
