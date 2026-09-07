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
import pyarrow.flight as flight

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
            years = payload.get("years", 6)
            result = self.engine.run_initial_load(years=years)
            status = "SUCCESS" if not result["tickers_failed"] else "PARTIAL"
            return flight.Result(
                json.dumps({"status": status, **result}).encode("utf-8")
            )

        elif action_type == "daily_load":
            result = self.engine.run_daily_load()
            if result.get("status") == "SKIPPED":
                return flight.Result(json.dumps(result).encode("utf-8"))
            status = "SUCCESS" if not result["tickers_failed"] else "PARTIAL"
            return flight.Result(
                json.dumps({"status": status, **result}).encode("utf-8")
            )

        elif action_type == "on_request_load":
            result = self.engine.run_on_request_load(
                tickers=payload["tickers"],
                start_date=payload["start_date"],
                end_date=payload["end_date"],
            )
            status = "SUCCESS" if not result["tickers_failed"] else "PARTIAL"
            return flight.Result(
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
            return flight.Result(
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
            return flight.Result(
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
            return flight.Result(
                json.dumps(
                    {"status": "SUCCESS", "run_history": run_history}
                ).encode("utf-8")
            )

        else:
            raise flight.FlightServerError(f"Unknown action: {action_type}")


__all__ = [
    "MarketDataFlightServer",
    "_validate_select_only",
    "_truncate_sql",
]
