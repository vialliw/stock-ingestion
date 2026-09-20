"""Challenger variant of the Apache Arrow Flight daemon.

Implements ``MarketDataFlightServer`` per spec §10 (reference implementation
lines 1145-1222) but with a STRUCTURALLY DIFFERENT internal design from the
executor's ``src/flight_server.py``:

  1. Dispatch table: a class-level ``_ACTION_HANDLERS`` registry maps action
     names to bound handler methods, replacing the spec's if/elif chain.
  2. Regex-based SQL validation: ``_validate_select_only`` uses a compiled
     ``re`` pattern anchored at start-of-string instead of a ``startswith``
     prefix check, tolerating leading whitespace/comments consistently.
  3. Custom exception hierarchy: ``FlightQueryError`` / ``FlightActionError``
     decouple validation/execution failures from the transport-level
     ``flight.FlightServerError``; a single ``_wrap_flight_error`` middleware
     converts them at the boundary.
  4. Result-construction helpers: ``_build_load_result`` and
     ``_build_report_result`` centralise JSON payload assembly so each
     handler stays tiny and the status-derivation rule (SUCCESS if no
     failures else PARTIAL, SKIPPED passthrough) lives in one place.
  5. Action handlers receive the parsed payload dict and return a dict that
     the dispatcher serialises — handlers never touch ``flight.Result``
     directly, making them trivially unit-testable without Flight types.

Same public API as spec §10:
  - module-level ``_validate_select_only(sql: str) -> None``
  - module-level ``_truncate_sql(sql: str, max_len: int = 200) -> str``
  - ``MarketDataFlightServer(flight.FlightServerBase)`` with
    ``__init__(location, db_mgr, ingest_engine, cfg)``,
    ``do_get(context, ticket)``, ``do_action(context, action)``.

Spec reference: docs/tech_spec/gemini_tech_spec.1.0.2.md §10 (lines 1145-1222).
Implemented independently from the spec only; ``src/flight_server.py`` was
NOT read.
"""

from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime
from typing import Any, Callable, Dict, Optional

import pyarrow.flight as flight

logger = logging.getLogger(__name__)


# =====================================================================
# Custom exception hierarchy (structural difference #3)
# =====================================================================

class FlightQueryError(Exception):
    """Raised for validation/execution failures in ``do_get``."""


class FlightActionError(Exception):
    """Raised for failures inside an action handler."""


# =====================================================================
# Module-level helpers
# =====================================================================

# Compiled once. Allows leading whitespace and SQL line comments ("-- ...")
# before the SELECT/WITH keyword, which a naive ``startswith`` would reject.
# Case-insensitive. Anchored at start of the (stripped) SQL.
_SELECT_ONLY_RE = re.compile(
    r"^\s*(?:--[^\n]*\n\s*)*(?:SELECT|WITH)\b",
    re.IGNORECASE,
)

# Statements that are never read-only. Matched after stripping leading
# whitespace/comments so that e.g. "  -- c\nDROP TABLE x" is still rejected
# even though the prefix regex above would not catch it (it would only catch
# SELECT/WITH). This is a defense-in-depth belt-and-braces check.
_FORBIDDEN_RE = re.compile(
    r"\b(?:INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE|GRANT|REVOKE|"
    r"ATTACH|DETACH|COPY|PRAGMA|VACUUM|EXPORT|IMPORT|CALL)\b",
    re.IGNORECASE,
)


def _validate_select_only(sql: str) -> None:
    """Validate that ``sql`` is a read-only SELECT or WITH (CTE) query.

    Accepts queries whose first significant token (after optional leading
    whitespace and ``--`` line comments) is ``SELECT`` or ``WITH``.
    Rejects everything else — including empty strings, DML/DDL, and
    statements that merely *contain* SELECT later on — with ``ValueError``.

    Raises:
        ValueError: if ``sql`` is empty or is not a SELECT/WITH query.
    """
    if not sql or not sql.strip():
        raise ValueError("Empty SQL query")
    if _SELECT_ONLY_RE.match(sql) is None:
        raise ValueError(
            f"Only SELECT / WITH queries are permitted; rejected: "
            f"{_truncate_sql(sql)}"
        )
    # Defense-in-depth: even a SELECT that embeds a forbidden verb
    # (e.g. "SELECT * FROM x; DROP TABLE y;") is rejected.
    if _FORBIDDEN_RE.search(sql):
        raise ValueError(
            f"Query contains a forbidden write/DDL keyword; rejected: "
            f"{_truncate_sql(sql)}"
        )


def _truncate_sql(sql: str, max_len: int = 200) -> str:
    """Truncate ``sql`` to ``max_len`` characters, appending ``...`` if cut.

    A query at or under the limit is returned unchanged. A query over the
    limit is sliced to ``max_len`` characters and ``"..."`` is appended so
    the returned string is always safe to log.
    """
    if len(sql) <= max_len:
        return sql
    return sql[:max_len] + "..."


class _ActionResult(flight.Result):
    """A ``flight.Result`` that is also iterable (yields itself).

    The pyarrow server-side ``_do_action`` callback calls ``iter()`` on
    the value returned by ``do_action``; a bare ``flight.Result`` is not
    iterable and raises ``TypeError: 'Result' object is not iterable``
    over real gRPC (F7-T17 / finding F-A).  Returning this subclass
    satisfies the wire contract while keeping ``.body`` accessible to
    direct (in-process) callers that invoke ``server.do_action(...)``
    without a gRPC transport.
    """

    def __iter__(self):
        return iter([self])


# =====================================================================
# Flight server
# =====================================================================

class MarketDataFlightServer(flight.FlightServerBase):
    """Arrow Flight server exposing read-only SQL (``do_get``) and four
    ingestion/reporting actions (``do_action``).

    Action dispatch is table-driven (structural difference #1): the class
    registry ``_ACTION_HANDLERS`` maps action-type strings to handler
    methods. Each handler has signature ``(payload: dict) -> dict`` and
    returns a plain dict; the dispatcher serialises it to ``flight.Result``.
    """

    # Populated after the class body so the names resolve.
    _ACTION_HANDLERS: Dict[str, Callable[..., Dict[str, Any]]] = {}

    def __init__(self, location: str, db_mgr, ingest_engine, cfg):
        super().__init__(location)
        self.db = db_mgr
        self.engine = ingest_engine
        self.cfg = cfg

    # ------------------------------------------------------------------
    # Error-boundary middleware (structural difference #3)
    # ------------------------------------------------------------------

    @staticmethod
    def _wrap_flight_error(exc: Exception, *, query: bool) -> flight.FlightServerError:
        """Convert internal exceptions to a ``FlightServerError``.

        ``query=True`` is used by ``do_get``; ``query=False`` by ``do_action``.
        """
        if isinstance(exc, flight.FlightServerError):
            return exc
        if isinstance(exc, (FlightQueryError, FlightActionError, ValueError)):
            return flight.FlightServerError(str(exc))
        return flight.FlightServerError(f"Query Execution Error: {exc}")

    # ------------------------------------------------------------------
    # do_get — read-only SQL
    # ------------------------------------------------------------------

    def do_get(self, context, ticket):
        try:
            req = json.loads(ticket.ticket.decode("utf-8"))
            sql_query = req.get("sql")
            if not sql_query:
                raise FlightQueryError("Missing 'sql' field in ticket")
            _validate_select_only(sql_query)
            logger.info("Serving Arrow Flight query: %s", _truncate_sql(sql_query))
            arrow_table = self.db.execute_query(sql_query)
            return flight.RecordBatchStream(arrow_table)
        except flight.FlightServerError:
            raise
        except FlightQueryError as e:
            logger.warning("Rejected do_get query: %s", e)
            raise self._wrap_flight_error(e, query=True)
        except Exception as e:
            logger.error("Failed to execute Flight do_get query: %s", e)
            raise self._wrap_flight_error(e, query=True)

    # ------------------------------------------------------------------
    # do_action — table-driven dispatch (structural difference #1)
    # ------------------------------------------------------------------

    def do_action(self, context, action):
        action_type = action.type
        payload: Dict[str, Any] = (
            json.loads(action.body.to_pybytes().decode("utf-8"))
            if action.body
            else {}
        )

        handler = self._ACTION_HANDLERS.get(action_type)
        if handler is None:
            raise flight.FlightServerError(f"Unknown action: {action_type}")

        try:
            result = handler(self, payload)
        except FlightActionError as e:
            logger.error("Action '%s' failed: %s", action_type, e)
            raise flight.FlightServerError(str(e))
        except Exception as e:
            logger.error("Action '%s' raised: %s", action_type, e)
            raise flight.FlightServerError(f"Action '{action_type}' failed: {e}")

        return _ActionResult(json.dumps(result).encode("utf-8"))

    # ------------------------------------------------------------------
    # Embedded data-health check (F9-T5) — report-only
    # ------------------------------------------------------------------

    def _health_report(
        self, include_gaps: bool = False, gap_days: Optional[int] = None
    ) -> Dict[str, Any]:
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
            kwargs: Dict[str, Any] = {
                "tickers": configured,
                "include_gaps": include_gaps,
            }
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
    # Result-construction helpers (structural difference #4)
    # ------------------------------------------------------------------

    @staticmethod
    def _derive_status(result: Dict[str, Any]) -> str:
        """Derive SUCCESS/PARTIAL from an ingestion result dict.

        A ``SKIPPED`` status (set by ``run_daily_load`` when the market is
        closed) is passed through unchanged.
        """
        if result.get("status") == "SKIPPED":
            return "SKIPPED"
        return "SUCCESS" if not result.get("tickers_failed") else "PARTIAL"

    @classmethod
    def _build_load_result(cls, result: Dict[str, Any]) -> Dict[str, Any]:
        """Build the JSON payload for an ingestion action."""
        status = cls._derive_status(result)
        return {"status": status, **result}

    @classmethod
    def _build_report_result(
        cls, report_data: Any, duration_ms: int
    ) -> Dict[str, Any]:
        """Build the JSON payload for the daily_report action."""
        return {
            "status": "SUCCESS",
            "report": report_data,
            "duration_ms": duration_ms,
        }

    # ------------------------------------------------------------------
    # Action handlers — each returns a dict; the dispatcher serialises.
    # ------------------------------------------------------------------

    def _handle_initial_load(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        # years=None (absent key) -> the engine applies the
        # configured ingestion.initial_load_years depth.  An invalid
        # explicit value (non-int, bool, or outside [1, 30]) raises
        # ValueError in the engine — convert to FlightActionError so
        # the dispatcher surfaces it as a FlightServerError instead
        # of a silent empty download.
        years = payload.get("years")
        try:
            result = self.engine.run_initial_load(years=years)
        except ValueError as e:
            raise FlightActionError(str(e)) from e
        # F9-T5: merge the light (no-gap) health report — report-only,
        # never changes the load's status.
        out = self._build_load_result(result)
        out["health"] = self._health_report()
        return out

    def _handle_daily_load(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        result = self.engine.run_daily_load(
            lookback_days=self.cfg.ingestion.daily_lookback_days,
            force=payload.get("force", False),
        )
        out = self._build_load_result(result)
        # F9-T5: merge the light (no-gap) health report only when the
        # load actually ran — a SKIPPED result is passed through bare.
        if out.get("status") != "SKIPPED":
            out["health"] = self._health_report()
        return out

    def _handle_on_request_load(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        result = self.engine.run_on_request_load(
            tickers=payload["tickers"],
            start_date=payload["start_date"],
            end_date=payload["end_date"],
        )
        return self._build_load_result(result)

    def _handle_daily_report(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        start_time = datetime.now()
        target_date: Optional[str] = payload.get("date")
        report_data = self.db.generate_top_20_dollar_volume(target_date)
        out_dir = self.cfg.reports.report_dir
        os.makedirs(out_dir, exist_ok=True)
        file_name = (
            f"dollar_volume_"
            f"{target_date or datetime.now().strftime('%Y%m%d')}.json"
        )
        with open(os.path.join(out_dir, file_name), "w") as f:
            json.dump(report_data, f, indent=2)
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        return self._build_report_result(report_data, duration_ms)

    def _handle_metadata_load(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        # F10-T1: company_metadata backfill (rate-limit safe).  Numeric
        # payload fields are validated like gap_days/years — non-int/bool
        # values are rejected, never silently coerced.
        max_tickers = payload.get("max_tickers")
        if max_tickers is not None and (
            not isinstance(max_tickers, int)
            or isinstance(max_tickers, bool)
            or max_tickers < 1
        ):
            raise FlightActionError(
                f"max_tickers must be an integer >= 1, got {max_tickers!r}"
            )
        delay_s = payload.get("delay_s")
        if delay_s is not None and (
            isinstance(delay_s, bool)
            or not isinstance(delay_s, (int, float))
            or delay_s < 0
        ):
            raise FlightActionError(
                f"delay_s must be a number >= 0, got {delay_s!r}"
            )
        refresh = payload.get("refresh", False)
        if not isinstance(refresh, bool):
            raise FlightActionError(
                f"refresh must be a bool, got {refresh!r}"
            )
        tickers = payload.get("tickers")
        if tickers is not None and not (
            isinstance(tickers, list)
            and all(isinstance(t, str) for t in tickers)
        ):
            raise FlightActionError(
                f"tickers must be a list of strings, got {tickers!r}"
            )
        kwargs: Dict[str, Any] = {"tickers": tickers, "refresh": refresh}
        if delay_s is not None:
            kwargs["delay_s"] = delay_s
        if max_tickers is not None:
            kwargs["max_tickers"] = max_tickers
        try:
            result = self.engine.run_metadata_load(**kwargs)
        except ValueError as e:
            raise FlightActionError(str(e)) from e
        status = (
            "ABORTED"
            if result.get("aborted")
            else "PARTIAL" if result.get("tickers_failed") else "SUCCESS"
        )
        return {"status": status, **result}

    def _handle_weekly_refresh(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        # F12-T1: rebuild weekly_close + weekly_indicators from
        # daily_close.  Payload is optional/empty.  A failed rebuild is
        # reported as status=FAILED rather than raising — the daily
        # tables are unaffected.
        try:
            return self.engine.run_weekly_refresh()
        except Exception as e:
            logger.error("weekly_refresh failed: %s", e)
            return {"status": "FAILED", "error": str(e)}

    def _handle_fear_greed_refresh(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        # F16-T1: CNN Fear & Greed history refresh.  Payload is
        # optional/empty; an explicit "start_date" (YYYY-MM-DD)
        # overrides the configured fear_greed.start_date — a malformed
        # value is rejected with FlightActionError (-> FlightServerError
        # over the wire).  Engine failures are reported as
        # status=FAILED in the body, not a transport error (mirrors
        # _handle_weekly_refresh).
        start_date = payload.get("start_date")
        if start_date is not None:
            if not isinstance(start_date, str) or not re.fullmatch(
                r"\d{4}-\d{2}-\d{2}", start_date
            ):
                raise FlightActionError(
                    f"start_date must be a YYYY-MM-DD string, "
                    f"got {start_date!r}"
                )
            try:
                datetime.strptime(start_date, "%Y-%m-%d")
            except ValueError as e:
                raise FlightActionError(
                    f"start_date is not a real calendar date, "
                    f"got {start_date!r}"
                ) from e
        try:
            return self.engine.run_fear_greed_load(start_date=start_date)
        except Exception as e:
            logger.error("fear_greed_refresh failed: %s", e)
            return {"status": "FAILED", "error": str(e)}

    def _handle_data_health(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        # F9-T5: FULL daily_close health report (incl. NYSE gap
        # detection).  Payload may carry "gap_days" (int >= 1) to size
        # the trailing-session gap window.
        gap_days = payload.get("gap_days")
        if gap_days is not None and (
            not isinstance(gap_days, int)
            or isinstance(gap_days, bool)
            or gap_days < 1
        ):
            raise FlightActionError(
                f"gap_days must be an integer >= 1, got {gap_days!r}"
            )
        return {
            "status": "SUCCESS",
            "health": self._health_report(
                include_gaps=True, gap_days=gap_days
            ),
        }


# =====================================================================
# Dispatch table registration (structural difference #1)
# =====================================================================

MarketDataFlightServer._ACTION_HANDLERS = {
    "initial_load": MarketDataFlightServer._handle_initial_load,
    "daily_load": MarketDataFlightServer._handle_daily_load,
    "on_request_load": MarketDataFlightServer._handle_on_request_load,
    "daily_report": MarketDataFlightServer._handle_daily_report,
    "data_health": MarketDataFlightServer._handle_data_health,
    "metadata_load": MarketDataFlightServer._handle_metadata_load,
    "weekly_refresh": MarketDataFlightServer._handle_weekly_refresh,
    "fear_greed_refresh": MarketDataFlightServer._handle_fear_greed_refresh,
}


__all__ = [
    "MarketDataFlightServer",
    "FlightQueryError",
    "FlightActionError",
    "_validate_select_only",
    "_truncate_sql",
]
