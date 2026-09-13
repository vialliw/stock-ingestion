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

        return flight.Result(json.dumps(result).encode("utf-8"))

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
        years = payload.get("years", 6)
        result = self.engine.run_initial_load(years=years)
        return self._build_load_result(result)

    def _handle_daily_load(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        result = self.engine.run_daily_load(force=payload.get("force", False))
        return self._build_load_result(result)

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


# =====================================================================
# Dispatch table registration (structural difference #1)
# =====================================================================

MarketDataFlightServer._ACTION_HANDLERS = {
    "initial_load": MarketDataFlightServer._handle_initial_load,
    "daily_load": MarketDataFlightServer._handle_daily_load,
    "on_request_load": MarketDataFlightServer._handle_on_request_load,
    "daily_report": MarketDataFlightServer._handle_daily_report,
}


__all__ = [
    "MarketDataFlightServer",
    "FlightQueryError",
    "FlightActionError",
    "_validate_select_only",
    "_truncate_sql",
]
