"""Apache Arrow Flight daemon for the stock-ingestion pipeline.

Implements ``MarketDataFlightServer`` per spec §10 (reference
implementation): a ``flight.FlightServerBase`` subclass that exposes a
read-only ``do_get`` endpoint (single-statement SELECT-only SQL
validated before reaching DuckDB) and a ``do_action`` endpoint routing
the administrative actions
(initial_load, daily_load, on_request_load, metadata_load,
weekly_refresh, fear_greed_refresh, signal_pipeline, signal_analysis,
dml_admin, delete_daily_rows, daily_report, health_check, data_health,
get_run_history, sync_to_r2).  ``do_put`` accepts either the legacy ``daily_close``
upsert or a JSON command descriptor for allowlisted-namespace writes
(F26-T1).

Spec reference: docs/tech_spec/gemini_tech_spec.1.0.2.md §5, §10.
"""

from __future__ import annotations

import json
import logging
import math
import os
import queue
import re
import threading
from datetime import datetime

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.flight as flight

from src.r2_sync import R2SyncManager

logger = logging.getLogger(__name__)


# =====================================================================
# Module-level helpers
# =====================================================================

def _validate_select_only(sql: str) -> None:
    """Reject anything but a single SELECT statement for ``do_get``.

    Two layers:

    1. Prefix fast-path — the stripped text must start with ``SELECT``
       or ``WITH`` (CTE).
    2. ``duckdb.extract_statements`` — the ticket must parse as
       *exactly one* statement whose type is ``StatementType.SELECT``
       (the same check ``client_dml.validate_source_sql`` applies).
       ``WITH ... SELECT`` classifies SELECT and passes;
       ``WITH ... INSERT`` classifies INSERT and is rejected.
       Trailing semicolons and comment-only tails still count as the
       single leading statement.

    Layer 2 closes the F26-T5 hole (challenger finding C-F26T4-1): a
    prefix-only check let multi-statement tickets through and
    ``conn.execute`` ran *every* statement — verified over real gRPC,
    ``SELECT 1; SET autoload_known_extensions=true`` re-armed the
    F26-T4 extension kill-switch instance-wide, and the same channel
    reached ``LOAD``/``INSTALL``/``SET enable_external_access`` (a
    one-way R2 breaker)/``SET memory_limit``/``SET threads``.

    Raises ``ValueError`` for any violation so that ``do_get`` can
    translate it into a ``FlightServerError``.
    """
    stripped = sql.strip().upper()
    if not stripped.startswith("SELECT") and not stripped.startswith("WITH"):
        raise ValueError(
            "Only SELECT and WITH (CTE) statements are allowed via do_get"
        )
    try:
        statements = duckdb.extract_statements(sql.strip())
    except Exception as exc:
        raise ValueError(f"do_get query does not parse: {exc}") from exc
    if len(statements) != 1:
        raise ValueError(
            "do_get accepts exactly one statement, "
            f"got {len(statements)}"
        )
    stmt_type = statements[0].type
    if stmt_type != duckdb.StatementType.SELECT:
        raise ValueError(
            f"do_get accepts only SELECT statements, got {stmt_type}"
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
        # R1A-T1: schema v9 — RawClose (unadjusted close) joined the
        # daily_close contract; the legacy 7-column payload is rejected.
        pa.field("RawClose", pa.float64()),
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


def _parse_dml_descriptor(descriptor):
    """Interpret a ``do_put`` descriptor as a generalized-DML request.

    F26-T1 wire contract: a **command** descriptor
    (``flight.FlightDescriptor.for_command``) whose body is a JSON
    object carrying a ``"table"`` key selects the generalized path —
    ``{"table": "pivot.t", "mode": "create_or_replace|append|replace",
    "namespace"?: ..., "pinned"?: ..., "client_tag"?: ...}``.

    Returns the parsed dict, or ``None`` for every other descriptor
    shape (UNKNOWN/PATH/empty/unparseable/missing ``table``) — those
    keep the legacy ``daily_close`` upsert path untouched, so existing
    callers that ignore the descriptor are unaffected.
    """
    if descriptor is None:
        return None
    if getattr(descriptor, "descriptor_type", None) != (
        flight.DescriptorType.CMD
    ):
        return None
    command = getattr(descriptor, "command", None)
    if command is None:
        return None
    try:
        obj = json.loads(bytes(command).decode("utf-8"))
    except Exception:
        return None
    if isinstance(obj, dict) and "table" in obj:
        return obj
    return None


# =====================================================================
# Arrow Flight server
# =====================================================================

class MarketDataFlightServer(flight.FlightServerBase):
    """Arrow Flight server exposing read-only SQL and admin actions.

    ``do_get`` accepts a JSON ticket containing an ``sql`` field,
    validates it is a single SELECT-type statement
    (``_validate_select_only``), executes it via
    ``DatabaseManager.execute_query`` and returns a
    ``flight.RecordBatchStream``.

    ``do_action`` routes administrative action types to the
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

        elif action_type == "signal_pipeline":
            # F24-T1: server-side signal pipeline — ingest xlsx signal
            # files, dedupe, repair closes from daily_close, prune,
            # compute signal_performance, populate cluster_metadata.
            # Optional payload keys override the ``signals:`` config
            # block; each is validated like the metadata_load
            # precedent (type/bool checks -> FlightServerError, never
            # silently coerced).
            backdate_days = payload.get("backdate_days")
            if backdate_days is not None and (
                not isinstance(backdate_days, int)
                or isinstance(backdate_days, bool)
                or backdate_days < 1
            ):
                raise flight.FlightServerError(
                    f"backdate_days must be an integer >= 1, "
                    f"got {backdate_days!r}"
                )
            max_holding_days = payload.get("max_holding_days")
            if max_holding_days is not None and (
                not isinstance(max_holding_days, int)
                or isinstance(max_holding_days, bool)
                or max_holding_days < 1
            ):
                raise flight.FlightServerError(
                    f"max_holding_days must be an integer >= 1, "
                    f"got {max_holding_days!r}"
                )
            overwrite_existing = payload.get("overwrite_existing")
            if overwrite_existing is not None and not isinstance(
                overwrite_existing, bool
            ):
                raise flight.FlightServerError(
                    f"overwrite_existing must be a bool, "
                    f"got {overwrite_existing!r}"
                )
            min_reward_risk = payload.get("min_reward_risk")
            # F24-T2: math.isfinite rejects NaN/Infinity/-Infinity —
            # json.loads accepts those literals and DuckDB
            # ``x < NaN``/``x < +Inf`` is TRUE for every row, which
            # would prune the entire signal_records table.
            if min_reward_risk is not None and (
                isinstance(min_reward_risk, bool)
                or not isinstance(min_reward_risk, (int, float))
                or not math.isfinite(min_reward_risk)
            ):
                raise flight.FlightServerError(
                    f"min_reward_risk must be a finite number, "
                    f"got {min_reward_risk!r}"
                )
            min_target_pct = payload.get("min_target_pct")
            if min_target_pct is not None and (
                isinstance(min_target_pct, bool)
                or not isinstance(min_target_pct, (int, float))
                or not math.isfinite(min_target_pct)
            ):
                raise flight.FlightServerError(
                    f"min_target_pct must be a finite number, "
                    f"got {min_target_pct!r}"
                )
            exclude_clusters = payload.get("exclude_clusters")
            if exclude_clusters is not None and not (
                isinstance(exclude_clusters, list)
                and all(
                    isinstance(c, int) and not isinstance(c, bool)
                    for c in exclude_clusters
                )
            ):
                raise flight.FlightServerError(
                    f"exclude_clusters must be a list of integers, "
                    f"got {exclude_clusters!r}"
                )
            exclude_models = payload.get("exclude_models")
            if exclude_models is not None and not (
                isinstance(exclude_models, list)
                and all(isinstance(m, str) for m in exclude_models)
            ):
                raise flight.FlightServerError(
                    f"exclude_models must be a list of strings, "
                    f"got {exclude_models!r}"
                )
            filter_below_open = payload.get("filter_below_open")
            if filter_below_open is not None and not isinstance(
                filter_below_open, bool
            ):
                raise flight.FlightServerError(
                    f"filter_below_open must be a bool, "
                    f"got {filter_below_open!r}"
                )
            folder_path = payload.get("folder_path")
            if folder_path is not None and not (
                isinstance(folder_path, str) and folder_path.strip()
            ):
                raise flight.FlightServerError(
                    f"folder_path must be a non-empty string, "
                    f"got {folder_path!r}"
                )

            kwargs = {}
            if folder_path is not None:
                kwargs["folder_path"] = folder_path
            if backdate_days is not None:
                kwargs["backdate_days"] = backdate_days
            if max_holding_days is not None:
                kwargs["max_holding_days"] = max_holding_days
            if overwrite_existing is not None:
                kwargs["overwrite_existing"] = overwrite_existing
            if min_reward_risk is not None:
                kwargs["min_reward_risk"] = min_reward_risk
            if min_target_pct is not None:
                kwargs["min_target_pct"] = min_target_pct
            if exclude_clusters is not None:
                kwargs["exclude_clusters"] = exclude_clusters
            if exclude_models is not None:
                kwargs["exclude_models"] = exclude_models
            if filter_below_open is not None:
                kwargs["filter_below_open"] = filter_below_open
            try:
                result = self.engine.run_signal_pipeline(**kwargs)
            except ValueError as e:
                raise flight.FlightServerError(str(e)) from e
            except Exception as e:
                # Mirrors weekly_refresh — engine failures surface as
                # status=FAILED in the JSON body, not a transport error.
                logger.error("signal_pipeline failed: %s", e)
                result = {"status": "FAILED", "error": str(e)}
            return _ActionResult(json.dumps(result).encode("utf-8"))

        elif action_type == "signal_analysis":
            # F25-T1: server-side 5-phase signal-analysis job — per-set
            # signals_<name> schema holding aly_-prefixed tables/views:
            # ingest xlsx -> dedupe -> repair closes from daily_close ->
            # 4 views -> 15-col aly_signal_performance + scorecard +
            # edge validator -> phase-5 matrix reports.
            # Optional payload keys override the ``signal_analysis:``
            # config block; each is validated like the signal_pipeline
            # precedent (type/bool/isfinite -> FlightServerError, never
            # silently coerced).
            set_name = payload.get("set")
            if set_name is not None:
                if not isinstance(set_name, str) or not set_name:
                    raise flight.FlightServerError(
                        f"set must be a non-empty string, "
                        f"got {set_name!r}"
                    )
                configured = {
                    s.name for s in self.cfg.signal_analysis.sets
                }
                if set_name not in configured:
                    raise flight.FlightServerError(
                        f"unknown signal_analysis set {set_name!r}; "
                        f"configured: {sorted(configured)}"
                    )

            def _positive_int(name):
                """Validate an optional positive-integer payload key.

                Numbers (int/float) must be finite (math.isfinite —
                json.loads accepts bare NaN/Infinity literals) and
                integral; bools/strings are rejected outright.
                """
                v = payload.get(name)
                if v is None:
                    return None
                if (
                    isinstance(v, bool)
                    or not isinstance(v, (int, float))
                    or not math.isfinite(v)
                    or v < 1
                    or int(v) != v
                ):
                    raise flight.FlightServerError(
                        f"{name} must be an integer >= 1, got {v!r}"
                    )
                return int(v)

            backdate_days = _positive_int("backdate_days")
            max_holding_days = _positive_int("max_holding_days")
            mc_iterations = _positive_int("mc_iterations")

            kwargs = {}
            if set_name is not None:
                kwargs["set_name"] = set_name
            if backdate_days is not None:
                kwargs["backdate_days"] = backdate_days
            if max_holding_days is not None:
                kwargs["max_holding_days"] = max_holding_days
            if mc_iterations is not None:
                kwargs["mc_iterations"] = mc_iterations
            try:
                result = self.engine.run_signal_analysis(**kwargs)
            except ValueError as e:
                raise flight.FlightServerError(str(e)) from e
            except Exception as e:
                # Mirrors weekly_refresh — engine failures surface as
                # status=FAILED in the JSON body, not a transport error.
                logger.error("signal_analysis failed: %s", e)
                result = {"status": "FAILED", "error": str(e)}
            return _ActionResult(json.dumps(result).encode("utf-8"))

        elif action_type == "dml_admin":
            # F26-T1: client-issued DML confined to
            # ``client_dml.allowed_namespaces`` (the ``pivot`` contract).
            # The engine validates + audits EVERY op — a REJECTED op
            # comes back in the JSON body with an audit row, not as a
            # transport error; only a non-object payload (which cannot
            # be audited meaningfully) is a FlightServerError.
            if not isinstance(payload, dict):
                raise flight.FlightServerError(
                    f"dml_admin payload must be a JSON object, "
                    f"got {type(payload).__name__}"
                )
            try:
                result = self.engine.run_dml_admin(payload)
            except ValueError as e:
                raise flight.FlightServerError(str(e)) from e
            except Exception as e:
                logger.error("dml_admin failed: %s", e)
                result = {"status": "FAILED", "error": str(e)}
            return _ActionResult(json.dumps(result).encode("utf-8"))

        elif action_type == "delete_daily_rows":
            # INC-T6: targeted eviction of vendor artifact rows (e.g.
            # delisting-terminal placeholder bars) from daily_close.
            # Payload: {"rows": [{"ticker": "QRVO",
            #                     "date": "2026-10-05"}, ...]}.
            if not isinstance(payload, dict):
                raise flight.FlightServerError(
                    f"delete_daily_rows payload must be a JSON object, "
                    f"got {type(payload).__name__}"
                )
            rows = payload.get("rows")
            if not isinstance(rows, list) or not rows:
                raise flight.FlightServerError(
                    "delete_daily_rows payload requires a non-empty "
                    "'rows' list of {ticker, date} objects"
                )
            pairs = []
            for i, entry in enumerate(rows):
                # justified: per-entry payload validation must raise on
                # the first malformed element with its index — cannot
                # be vectorized.
                if not isinstance(entry, dict):
                    raise flight.FlightServerError(
                        f"rows[{i}] must be an object with 'ticker' "
                        f"and 'date', got {type(entry).__name__}"
                    )
                ticker = entry.get("ticker")
                if not isinstance(ticker, str) or not ticker.strip():
                    raise flight.FlightServerError(
                        f"rows[{i}].ticker must be a non-empty string, "
                        f"got {ticker!r}"
                    )
                row_date = entry.get("date")
                if not isinstance(row_date, str) or not re.fullmatch(
                    r"\d{4}-\d{2}-\d{2}", row_date.strip()
                ):
                    raise flight.FlightServerError(
                        f"rows[{i}].date must be a YYYY-MM-DD string, "
                        f"got {row_date!r}"
                    )
                try:
                    datetime.strptime(row_date.strip(), "%Y-%m-%d")
                except ValueError:
                    raise flight.FlightServerError(
                        f"rows[{i}].date is not a real calendar date, "
                        f"got {row_date!r}"
                    )
                pairs.append((ticker.strip().upper(), row_date.strip()))
            try:
                deleted = self.db.delete_daily_rows(pairs)
            except Exception as e:
                # Mirrors dml_admin — a DB failure surfaces as
                # status=FAILED in the JSON body, not a transport error.
                logger.error("delete_daily_rows failed: %s", e)
                result = {
                    "status": "FAILED",
                    "error": str(e),
                    "requested": len(pairs),
                }
                return _ActionResult(json.dumps(result).encode("utf-8"))
            return _ActionResult(
                json.dumps(
                    {
                        "status": "SUCCESS",
                        "deleted": deleted,
                        "requested": len(pairs),
                    }
                ).encode("utf-8")
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
        """Receive an Arrow stream and write it per the descriptor.

        F26-T1: a JSON command descriptor ``{"table": ..., "mode": ...}``
        selects the generalized client-DML path — the engine validates,
        audits and executes it (confinement to
        ``client_dml.allowed_namespaces`` enforced there).  Every other
        descriptor shape keeps the legacy behaviour: validate against
        the ``daily_close`` schema, filter ``exclude_tickers`` and
        upsert via ``DatabaseManager.upsert_dataframe``.
        """
        try:
            table = reader.read_all()
        except Exception as e:
            logger.error("Failed to read Arrow Flight do_put stream: %s", e)
            raise flight.FlightServerError(
                f"Failed to read do_put stream: {e}"
            ) from e

        dml_descriptor = _parse_dml_descriptor(descriptor)
        if dml_descriptor is not None:
            try:
                result = self.engine.run_dml_put(dml_descriptor, table)
            except ValueError as e:
                # Includes DmlRejected — the audit row is already
                # written by the engine; surface as a transport error.
                raise flight.FlightServerError(str(e)) from e
            except Exception as e:
                raise flight.FlightServerError(
                    f"do_put write failed: {e}"
                ) from e
            logger.info(
                "do_put generalized write: %s", json.dumps(result)
            )
            return

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

        # INC-T6: the same enforced (ticker, date) exclusions apply to
        # the client write path — a do_put into daily_close must not
        # resurrect a filtered vendor artifact row.
        exclude_rows = getattr(ingestion_cfg, "exclude_rows", None)
        if isinstance(exclude_rows, (list, tuple)) and exclude_rows:
            keys = {
                f"{str(e.get('ticker', '')).strip().upper()}|"
                f"{str(e.get('date', '')).strip()}"
                for e in exclude_rows
                if isinstance(e, dict)
            }
            if keys and not df.empty:
                date_str = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")
                row_keys = (
                    df["Ticker"].astype(str).str.strip().str.upper()
                    + "|" + date_str
                )
                mask = row_keys.isin(keys)
                excluded_count = int(mask.sum())
                if excluded_count:
                    logger.warning(
                        "RUNTIME EVENT: do_put filtered %s row(s) "
                        "matching exclude_rows pairs %s",
                        excluded_count,
                        sorted(set(row_keys[mask])),
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
