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
import re
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

    # F17-T1: severity ordering for the composite worst-status rollup.
    # DISABLED sections are excluded; an unrecognized status is treated
    # as CHECK_FAILED so a malformed section can never silently pass.
    _HEALTH_STATUS_SEVERITY = {"OK": 0, "WARN": 1, "CHECK_FAILED": 2}

    # Composite section names -> DatabaseManager check methods (F17-T1).
    _HEALTH_SECTION_METHODS = {
        "fear_greed": "check_fear_greed_health",
        "weekly": "check_weekly_health",
        "company_metadata": "check_metadata_health",
    }

    def _health_section(self, name: str, kwargs: dict) -> dict:
        """Run one health-check method, isolating its failure (F17-T1).

        A raising check — or one returning a non-dict — yields
        ``health_status == 'CHECK_FAILED'`` for that section only; the
        sibling sections still run and the composite never raises.
        """
        try:
            section = getattr(
                self.db, self._HEALTH_SECTION_METHODS[name]
            )(**kwargs)
            if not isinstance(section, dict):
                raise TypeError(
                    f"{self._HEALTH_SECTION_METHODS[name]} returned "
                    f"{type(section).__name__}, expected dict"
                )
            return section
        except Exception as exc:
            return {
                "health_status": "CHECK_FAILED",
                "section": name,
                "error": str(exc),
            }

    def _health_report(
        self, include_gaps: bool = False, gap_days=None
    ) -> dict:
        """Run the composite data-health checks; NEVER raises.

        Returns the report dict.  F17-T1: the report is a composite —
        all existing ``daily_close`` keys stay at the top level (the
        daily report IS the base dict), plus three sub-reports under
        ``fear_greed``, ``weekly`` and ``company_metadata`` — each with
        its own ``health_status`` — and a ``daily_close`` key mirroring
        the daily report with its own status preserved.  The top-level
        ``health_status`` is the worst across all sections
        (``OK < WARN < CHECK_FAILED``; ``DISABLED`` is excluded).

        Any failure — including a non-dict return from a check —
        yields ``health_status == 'CHECK_FAILED'`` (per-section, or the
        whole report if the daily section/config resolution itself
        fails) so a broken check can never mask or crash a completed
        load.  Logs a WARNING whenever the status is not 'OK'.
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
            # The daily check is a section too: isolate its failure so
            # the other sections still run and only it reports
            # CHECK_FAILED.
            try:
                report = self.db.check_daily_close_health(**kwargs)
                if not isinstance(report, dict):
                    raise TypeError(
                        "check_daily_close_health returned "
                        f"{type(report).__name__}, expected dict"
                    )
            except Exception as exc:
                report = {
                    "health_status": "CHECK_FAILED",
                    "error": str(exc),
                }
            # Snapshot the daily report under its own section key before
            # the top-level health_status becomes the composite worst.
            # Composite keys are filtered out so a caller reusing the
            # same dict (e.g. a stubbed check) cannot nest sections.
            section_keys = set(self._HEALTH_SECTION_METHODS) | {
                "daily_close"
            }
            report["daily_close"] = {
                k: v for k, v in report.items() if k not in section_keys
            }
            fg_cfg = getattr(self.cfg, "fear_greed", None)
            fg_enabled = bool(getattr(fg_cfg, "enabled", True))
            report["fear_greed"] = self._health_section(
                "fear_greed", {"enabled": fg_enabled}
            )
            report["weekly"] = self._health_section("weekly", {})
            report["company_metadata"] = self._health_section(
                "company_metadata", {"tickers": configured}
            )
            # Worst-status rollup across the four sections; DISABLED is
            # excluded (the feature is simply off).
            ranked = [
                report[name].get("health_status")
                for name in self._HEALTH_SECTION_METHODS
            ]
            ranked.append(report["daily_close"].get("health_status"))
            ranked = [s for s in ranked if s not in (None, "DISABLED")]
            report["health_status"] = max(
                ranked,
                key=lambda s: self._HEALTH_STATUS_SEVERITY.get(s, 2),
                default="OK",
            )
        except Exception as exc:
            report = {"health_status": "CHECK_FAILED", "error": str(exc)}
        status = report.get("health_status")
        if status != "OK":
            logger.warning(
                "data-health composite reported health_status=%s",
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

        elif action_type == "metadata_load":
            # F10-T1: company_metadata backfill (rate-limit safe).
            # Numeric payload fields are validated like gap_days/years —
            # non-int/bool values are rejected, never silently coerced.
            max_tickers = payload.get("max_tickers")
            if max_tickers is not None and (
                not isinstance(max_tickers, int)
                or isinstance(max_tickers, bool)
                or max_tickers < 1
            ):
                raise flight.FlightServerError(
                    f"max_tickers must be an integer >= 1, got {max_tickers!r}"
                )
            delay_s = payload.get("delay_s")
            if delay_s is not None and (
                isinstance(delay_s, bool)
                or not isinstance(delay_s, (int, float))
                or delay_s < 0
            ):
                raise flight.FlightServerError(
                    f"delay_s must be a number >= 0, got {delay_s!r}"
                )
            refresh = payload.get("refresh", False)
            if not isinstance(refresh, bool):
                raise flight.FlightServerError(
                    f"refresh must be a bool, got {refresh!r}"
                )
            tickers = payload.get("tickers")
            if tickers is not None and not (
                isinstance(tickers, list)
                and all(isinstance(t, str) for t in tickers)
            ):
                raise flight.FlightServerError(
                    f"tickers must be a list of strings, got {tickers!r}"
                )
            kwargs = {"tickers": tickers, "refresh": refresh}
            if delay_s is not None:
                kwargs["delay_s"] = delay_s
            if max_tickers is not None:
                kwargs["max_tickers"] = max_tickers
            try:
                result = self.engine.run_metadata_load(**kwargs)
            except ValueError as e:
                raise flight.FlightServerError(str(e)) from e
            status = (
                "ABORTED"
                if result.get("aborted")
                else "PARTIAL" if result.get("tickers_failed") else "SUCCESS"
            )
            return _ActionResult(
                json.dumps({"status": status, **result}).encode("utf-8")
            )

        elif action_type == "weekly_refresh":
            # F12-T1: rebuild weekly_close + weekly_indicators from
            # daily_close.  Payload is optional/empty.  A failed rebuild
            # surfaces as status=FAILED rather than a transport error —
            # the daily tables are unaffected.
            try:
                result = self.engine.run_weekly_refresh()
            except Exception as e:
                logger.error("weekly_refresh failed: %s", e)
                result = {"status": "FAILED", "error": str(e)}
            return _ActionResult(json.dumps(result).encode("utf-8"))

        elif action_type == "fear_greed_refresh":
            # F16-T1: CNN Fear & Greed history refresh.  Payload is
            # optional/empty; an explicit "start_date" (YYYY-MM-DD)
            # overrides the configured fear_greed.start_date.  Engine
            # failures surface as status=FAILED in the JSON body — not
            # a transport error (mirrors weekly_refresh).
            start_date = payload.get("start_date")
            if start_date is not None:
                if not isinstance(start_date, str) or not re.fullmatch(
                    r"\d{4}-\d{2}-\d{2}", start_date
                ):
                    raise flight.FlightServerError(
                        f"start_date must be a YYYY-MM-DD string, "
                        f"got {start_date!r}"
                    )
                try:
                    datetime.strptime(start_date, "%Y-%m-%d")
                except ValueError as e:
                    raise flight.FlightServerError(
                        f"start_date is not a real calendar date, "
                        f"got {start_date!r}"
                    ) from e
            try:
                result = self.engine.run_fear_greed_load(
                    start_date=start_date
                )
            except Exception as e:
                logger.error("fear_greed_refresh failed: %s", e)
                result = {"status": "FAILED", "error": str(e)}
            return _ActionResult(json.dumps(result).encode("utf-8"))

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
