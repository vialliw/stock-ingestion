"""Critic tests for the Arrow Flight server implementations.

Parametrized over BOTH ``src.flight_server`` (executor, F4-T1) and
``src.flight_server_challenger`` (challenger, F4-T2).

Tests cover the F4-T3 acceptance criteria:
  1-6. ``_validate_select_only`` accepts SELECT/WITH, rejects DML/DDL/empty.
  7.   ``_truncate_sql`` truncates at 200 chars with a ``...`` marker.
  8.   ``do_action`` "initial_load" returns correct status structure.
  9.   ``do_action`` "daily_load" returns correct status structure.
  10.  ``do_action`` "daily_load" with SKIPPED passthrough.
  11.  ``do_action`` "on_request_load" returns correct status structure.
  12.  ``do_action`` "daily_report" returns report array + writes JSON file.
  13.  ``do_action`` unknown action raises ``FlightServerError``.

Uses mocked ``DatabaseManager`` and ``IngestionEngine`` — no real Flight
server is started.
"""

from __future__ import annotations

import importlib
import json
import os
import socket
import threading
import time
import types
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pyarrow.flight as flight
import pytest


# =====================================================================
# Fixtures
# =====================================================================

@pytest.fixture(
    params=[
        "src.flight_server",
        "src.flight_server_challenger",
    ],
    ids=["executor:src.flight_server", "challenger:src.flight_server_challenger"],
)
def module(request):
    """Parametrized fixture yielding each Flight server module."""
    return importlib.import_module(request.param)


def make_server(module, report_dir="/tmp/test_reports"):
    """Create a ``MarketDataFlightServer`` with mocked dependencies.

    ``FlightServerBase.__init__`` binds a real gRPC port, which we don't
    want in unit tests.  We subclass and override ``__init__`` to skip the
    base initialiser, then set the attributes that ``do_action`` uses.
    No real Flight server is started.

    Returns ``(server, db_mgr, ingest_engine, cfg)``.
    """
    db_mgr = MagicMock()
    ingest_engine = MagicMock()
    cfg = MagicMock()
    cfg.reports.report_dir = report_dir
    # F9-T5: give the health check a clean default so load-action tests
    # exercise the normal embed path; tests override it as needed.
    db_mgr.check_daily_close_health.return_value = {
        "health_status": "OK",
        "mode": "light",
        "checks": {},
    }
    # F17-T1: same for the three new composite sections — a bare
    # MagicMock return is non-dict and would mark the section
    # CHECK_FAILED, dragging the composite worst-status with it.
    db_mgr.check_fear_greed_health.return_value = {"health_status": "OK"}
    db_mgr.check_weekly_health.return_value = {"health_status": "OK"}
    db_mgr.check_metadata_health.return_value = {"health_status": "OK"}

    class _TestServer(module.MarketDataFlightServer):
        """Subclass that skips the port-binding base ``__init__``."""

        def __init__(self, db_mgr, ingest_engine, cfg):
            self.db = db_mgr
            self.engine = ingest_engine
            self.cfg = cfg

    server = _TestServer(db_mgr, ingest_engine, cfg)
    return server, db_mgr, ingest_engine, cfg


def make_action(action_type, payload=None):
    """Build a mock Flight action object with the given type and payload."""
    action = MagicMock()
    action.type = action_type
    if payload is not None:
        action.body = MagicMock()
        action.body.to_pybytes.return_value = json.dumps(payload).encode("utf-8")
    else:
        action.body = None
    return action


def parse_result(result):
    """Decode a ``flight.Result`` body to a Python dict."""
    return json.loads(result.body.to_pybytes().decode("utf-8"))


# Each Flight server is paired with its sibling ingestion engine.
_ENGINE_MODULES = {
    "src.flight_server": "src.ingestion",
    "src.flight_server_challenger": "src.ingestion_challenger",
}


# =====================================================================
# _validate_select_only — acceptance criteria 1-6
# =====================================================================

class TestValidateSelectOnly:
    """Tests for the module-level ``_validate_select_only`` function."""

    def test_accepts_simple_select(self, module):
        # AC 1: accepts 'SELECT * FROM daily_close'
        module._validate_select_only("SELECT * FROM daily_close")

    def test_accepts_with_cte(self, module):
        # AC 2: accepts 'WITH cte AS (SELECT * FROM daily_close) SELECT * FROM cte'
        module._validate_select_only(
            "WITH cte AS (SELECT * FROM daily_close) SELECT * FROM cte"
        )

    def test_rejects_delete(self, module):
        # AC 3: rejects 'DELETE FROM daily_close'
        with pytest.raises(ValueError):
            module._validate_select_only("DELETE FROM daily_close")

    def test_rejects_drop(self, module):
        # AC 4: rejects 'DROP TABLE daily_close'
        with pytest.raises(ValueError):
            module._validate_select_only("DROP TABLE daily_close")

    def test_rejects_insert(self, module):
        # AC 5: rejects 'INSERT INTO daily_close VALUES (...)'
        with pytest.raises(ValueError):
            module._validate_select_only(
                "INSERT INTO daily_close VALUES (1, 'AAPL', 100, 200)"
            )

    def test_rejects_empty_string(self, module):
        # AC 6: rejects empty string
        with pytest.raises(ValueError):
            module._validate_select_only("")


# =====================================================================
# _truncate_sql — acceptance criterion 7
# =====================================================================

class TestTruncateSql:
    """Tests for the module-level ``_truncate_sql`` function."""

    def test_truncates_at_200_chars(self, module):
        # AC 7: create a 250-char SQL string, verify truncation
        sql = "SELECT " + "x" * 243  # 7 + 243 = 250 chars
        assert len(sql) == 250
        result = module._truncate_sql(sql)
        # Result must be longer than 200 (marker appended)
        assert len(result) > 200
        # First 200 chars are preserved from the original
        assert result[:200] == sql[:200]
        # Truncation marker contains "..."
        assert "..." in result
        # Total length is reasonable (200 + marker; both implementations
        # use short markers: executor "...[truncated]"=14, challenger "..."=3)
        assert len(result) <= 220

    def test_short_sql_not_truncated(self, module):
        result = module._truncate_sql("SELECT 1")
        assert "SELECT 1" in result


# =====================================================================
# do_action routing — acceptance criteria 8-13
# =====================================================================

class TestDoAction:
    """Tests for ``do_action`` action routing with mocked dependencies."""

    def test_initial_load_success(self, module):
        # AC 8: initial_load returns correct status structure
        server, db_mgr, ingest_engine, cfg = make_server(module)
        ingest_engine.run_initial_load.return_value = {
            "rows_upserted": 100,
            "tickers_succeeded": ["AAPL"],
            "tickers_failed": [],
            "errors": {},
            "duration_ms": 500,
        }
        action = make_action("initial_load", {"years": 6})
        data = parse_result(server.do_action(MagicMock(), action))

        assert data["status"] == "SUCCESS"
        assert data["rows_upserted"] == 100
        assert data["tickers_succeeded"] == ["AAPL"]
        assert data["tickers_failed"] == []
        assert data["errors"] == {}
        assert data["duration_ms"] == 500
        ingest_engine.run_initial_load.assert_called_once_with(years=6)

    def test_initial_load_forwards_explicit_years(self, module):
        # F8-T1: an explicit "years" in the payload is forwarded to the
        # engine unchanged.
        server, db_mgr, ingest_engine, cfg = make_server(module)
        ingest_engine.run_initial_load.return_value = {
            "rows_upserted": 100,
            "tickers_succeeded": ["AAPL"],
            "tickers_failed": [],
            "errors": {},
            "duration_ms": 500,
        }
        action = make_action("initial_load", {"years": 12})
        data = parse_result(server.do_action(MagicMock(), action))

        assert data["status"] == "SUCCESS"
        ingest_engine.run_initial_load.assert_called_once_with(years=12)

    def test_initial_load_absent_years_passes_none(self, module):
        # F8-T1: payload without "years" (or no body) -> years=None so
        # the engine applies cfg.ingestion.initial_load_years.
        server, db_mgr, ingest_engine, cfg = make_server(module)
        ingest_engine.run_initial_load.return_value = {
            "rows_upserted": 100,
            "tickers_succeeded": ["AAPL"],
            "tickers_failed": [],
            "errors": {},
            "duration_ms": 500,
        }
        action = make_action("initial_load")  # no body at all
        data = parse_result(server.do_action(MagicMock(), action))

        assert data["status"] == "SUCCESS"
        ingest_engine.run_initial_load.assert_called_once_with(years=None)

    def test_initial_load_empty_payload_passes_none(self, module):
        # F8-T1: an empty JSON body also yields years=None.
        server, db_mgr, ingest_engine, cfg = make_server(module)
        ingest_engine.run_initial_load.return_value = {
            "rows_upserted": 100,
            "tickers_succeeded": ["AAPL"],
            "tickers_failed": [],
            "errors": {},
            "duration_ms": 500,
        }
        action = make_action("initial_load", {})
        data = parse_result(server.do_action(MagicMock(), action))

        assert data["status"] == "SUCCESS"
        ingest_engine.run_initial_load.assert_called_once_with(years=None)

    @pytest.mark.parametrize("bad_years", [0, -1, 31, "10", True])
    def test_initial_load_invalid_years_surfaces_error(
        self, module, bad_years
    ):
        # F9-T3: an invalid "years" payload must surface as a
        # FlightServerError — never a silent empty download.  Uses the
        # REAL engine (validation raises before it touches yfinance or
        # the DB) so the check is end-to-end, not a mocked side_effect.
        engine_mod = importlib.import_module(
            _ENGINE_MODULES[module.__name__]
        )
        real_engine = engine_mod.IngestionEngine(MagicMock(), MagicMock())
        server, db_mgr, _, cfg = make_server(module)
        server.engine = real_engine

        action = make_action("initial_load", {"years": bad_years})
        with pytest.raises(flight.FlightServerError):
            server.do_action(MagicMock(), action)

    def test_initial_load_valid_years_bounds_forwarded(self, module):
        # F9-T3: boundary values 1 and 30 are valid and forwarded.
        server, db_mgr, ingest_engine, cfg = make_server(module)
        ingest_engine.run_initial_load.return_value = {
            "rows_upserted": 0,
            "tickers_succeeded": [],
            "tickers_failed": [],
            "errors": {},
            "duration_ms": 1,
        }
        for bound in (1, 30):
            action = make_action("initial_load", {"years": bound})
            data = parse_result(server.do_action(MagicMock(), action))
            assert data["status"] == "SUCCESS"
        assert ingest_engine.run_initial_load.call_count == 2

    def test_daily_load_success(self, module):
        # AC 9: daily_load returns correct status structure
        server, db_mgr, ingest_engine, cfg = make_server(module)
        ingest_engine.run_daily_load.return_value = {
            "rows_upserted": 50,
            "tickers_succeeded": ["AAPL"],
            "tickers_failed": [],
            "errors": {},
            "duration_ms": 300,
        }
        action = make_action("daily_load")
        data = parse_result(server.do_action(MagicMock(), action))

        assert data["status"] == "SUCCESS"
        assert data["rows_upserted"] == 50
        assert data["tickers_succeeded"] == ["AAPL"]
        assert data["tickers_failed"] == []
        ingest_engine.run_daily_load.assert_called_once()

    def test_daily_load_forwards_force_true(self, module):
        # F7-T16: do_action("daily_load", {"force": true}) reaches
        # run_daily_load(force=True) so the service can run catch-up
        # loads on behalf of the Windows scheduled task.
        server, db_mgr, ingest_engine, cfg = make_server(module)
        ingest_engine.run_daily_load.return_value = {
            "rows_upserted": 50,
            "tickers_succeeded": ["AAPL"],
            "tickers_failed": [],
            "errors": {},
            "duration_ms": 300,
        }
        action = make_action("daily_load", {"force": True})
        data = parse_result(server.do_action(MagicMock(), action))

        assert data["status"] == "SUCCESS"
        ingest_engine.run_daily_load.assert_called_once()
        assert ingest_engine.run_daily_load.call_args.kwargs["force"] is True

    def test_daily_load_default_preserves_force_false(self, module):
        # F7-T16: a payload without "force" (or with no body at all)
        # keeps the force=False default — market-closed days are still
        # skipped for clients that do not opt in.
        server, db_mgr, ingest_engine, cfg = make_server(module)
        ingest_engine.run_daily_load.return_value = {
            "rows_upserted": 50,
            "tickers_succeeded": ["AAPL"],
            "tickers_failed": [],
            "errors": {},
            "duration_ms": 300,
        }
        action = make_action("daily_load")
        data = parse_result(server.do_action(MagicMock(), action))

        assert data["status"] == "SUCCESS"
        ingest_engine.run_daily_load.assert_called_once()
        assert ingest_engine.run_daily_load.call_args.kwargs["force"] is False

    def test_daily_load_skipped(self, module):
        # AC 10: daily_load with SKIPPED status passthrough
        server, db_mgr, ingest_engine, cfg = make_server(module)
        ingest_engine.run_daily_load.return_value = {
            "status": "SKIPPED",
            "reason": "Market closed",
            "duration_ms": 0,
        }
        action = make_action("daily_load")
        data = parse_result(server.do_action(MagicMock(), action))

        assert data["status"] == "SKIPPED"
        assert data["reason"] == "Market closed"
        assert data["duration_ms"] == 0

    def test_on_request_load_success(self, module):
        # AC 11: on_request_load returns correct status structure
        server, db_mgr, ingest_engine, cfg = make_server(module)
        ingest_engine.run_on_request_load.return_value = {
            "rows_upserted": 30,
            "tickers_succeeded": ["AAPL"],
            "tickers_failed": [],
            "errors": {},
            "duration_ms": 200,
        }
        payload = {
            "tickers": ["AAPL"],
            "start_date": "2026-01-01",
            "end_date": "2026-01-10",
        }
        action = make_action("on_request_load", payload)
        data = parse_result(server.do_action(MagicMock(), action))

        assert data["status"] == "SUCCESS"
        assert data["rows_upserted"] == 30
        assert data["tickers_succeeded"] == ["AAPL"]
        assert data["tickers_failed"] == []
        assert data["errors"] == {}
        assert data["duration_ms"] == 200
        ingest_engine.run_on_request_load.assert_called_once_with(
            tickers=["AAPL"], start_date="2026-01-01", end_date="2026-01-10"
        )

    def test_daily_report_writes_json_and_returns_report(self, module, tmp_path):
        # AC 12: daily_report returns report array + writes JSON file
        server, db_mgr, ingest_engine, cfg = make_server(
            module, report_dir=str(tmp_path)
        )
        db_mgr.generate_top_20_dollar_volume.return_value = [
            {"Ticker": "AAPL", "dollar_volume": 1000}
        ]
        action = make_action("daily_report", {"date": "2026-01-01"})
        data = parse_result(server.do_action(MagicMock(), action))

        assert data["status"] == "SUCCESS"
        assert data["report"] == [{"Ticker": "AAPL", "dollar_volume": 1000}]
        assert "duration_ms" in data
        assert isinstance(data["duration_ms"], int)

        # Verify JSON file was written to report_dir
        expected_file = os.path.join(str(tmp_path), "dollar_volume_2026-01-01.json")
        assert os.path.exists(expected_file)
        with open(expected_file) as f:
            written = json.load(f)
        assert written == [{"Ticker": "AAPL", "dollar_volume": 1000}]

        db_mgr.generate_top_20_dollar_volume.assert_called_once_with("2026-01-01")

    def test_unknown_action_raises_flight_server_error(self, module):
        # AC 13: unknown action raises FlightServerError
        server, db_mgr, ingest_engine, cfg = make_server(module)
        action = make_action("unknown_action")
        with pytest.raises(flight.FlightServerError):
            server.do_action(MagicMock(), action)

    def test_initial_load_partial_when_tickers_failed(self, module):
        # Bonus: initial_load with failures -> PARTIAL status
        server, db_mgr, ingest_engine, cfg = make_server(module)
        ingest_engine.run_initial_load.return_value = {
            "rows_upserted": 50,
            "tickers_succeeded": ["AAPL"],
            "tickers_failed": ["BAD"],
            "errors": {"BAD": "download error"},
            "duration_ms": 100,
        }
        action = make_action("initial_load")
        data = parse_result(server.do_action(MagicMock(), action))
        assert data["status"] == "PARTIAL"
        assert data["tickers_failed"] == ["BAD"]
        # No "years" key -> None -> engine applies the config default.
        ingest_engine.run_initial_load.assert_called_once_with(years=None)


# =====================================================================
# F9-T5: embedded light health + data_health action
# =====================================================================

_OK_HEALTH = {"health_status": "OK", "mode": "light", "checks": {}}


def _load_result(**overrides):
    result = {
        "rows_upserted": 10,
        "tickers_succeeded": ["AAPL"],
        "tickers_failed": [],
        "errors": {},
        "duration_ms": 50,
    }
    result.update(overrides)
    return result


class TestEmbeddedHealth:
    """Light (no-gap) health block merged into completed loads."""

    def test_daily_load_result_contains_health(self, module):
        server, db_mgr, ingest_engine, cfg = make_server(module)
        ingest_engine.run_daily_load.return_value = _load_result()
        db_mgr.check_daily_close_health.return_value = {
            "health_status": "WARN",
            "mode": "light",
            "checks": {"staleness": {"stale_count": 1}},
        }
        data = parse_result(
            server.do_action(MagicMock(), make_action("daily_load"))
        )
        # Report-only: findings never change the load's status.
        assert data["status"] == "SUCCESS"
        assert data["health"]["health_status"] == "WARN"
        # Light mode: no gap detection.
        call = db_mgr.check_daily_close_health.call_args
        assert call.kwargs["include_gaps"] is False
        # Configured tickers minus exclusions are passed for coverage.
        cfg.ingestion.tickers = ["AAPL", "MSFT"]
        cfg.ingestion.exclude_tickers = ["MSFT"]
        server.do_action(MagicMock(), make_action("daily_load"))
        call = db_mgr.check_daily_close_health.call_args
        assert call.kwargs["tickers"] == ["AAPL"]

    def test_daily_load_skipped_has_no_health(self, module):
        server, db_mgr, ingest_engine, cfg = make_server(module)
        ingest_engine.run_daily_load.return_value = {
            "status": "SKIPPED",
            "reason": "Market closed",
            "duration_ms": 0,
        }
        data = parse_result(
            server.do_action(MagicMock(), make_action("daily_load"))
        )
        assert data["status"] == "SKIPPED"
        assert "health" not in data
        db_mgr.check_daily_close_health.assert_not_called()

    def test_initial_load_result_contains_health(self, module):
        server, db_mgr, ingest_engine, cfg = make_server(module)
        ingest_engine.run_initial_load.return_value = _load_result()
        data = parse_result(
            server.do_action(MagicMock(), make_action("initial_load"))
        )
        assert data["status"] == "SUCCESS"
        assert data["health"]["health_status"] == "OK"
        db_mgr.check_daily_close_health.assert_called_once()

    def test_on_request_load_has_no_health(self, module):
        server, db_mgr, ingest_engine, cfg = make_server(module)
        ingest_engine.run_on_request_load.return_value = _load_result()
        payload = {
            "tickers": ["AAPL"],
            "start_date": "2026-01-01",
            "end_date": "2026-01-10",
        }
        data = parse_result(
            server.do_action(
                MagicMock(), make_action("on_request_load", payload)
            )
        )
        assert data["status"] == "SUCCESS"
        assert "health" not in data
        db_mgr.check_daily_close_health.assert_not_called()

    def test_health_exception_yields_check_failed(self, module):
        """A raising check -> health CHECK_FAILED; load status intact."""
        server, db_mgr, ingest_engine, cfg = make_server(module)
        ingest_engine.run_daily_load.return_value = _load_result()
        db_mgr.check_daily_close_health.side_effect = RuntimeError(
            "db exploded"
        )
        data = parse_result(
            server.do_action(MagicMock(), make_action("daily_load"))
        )
        assert data["status"] == "SUCCESS"
        assert data["health"]["health_status"] == "CHECK_FAILED"
        assert "db exploded" in data["health"]["error"]

    def test_health_non_dict_return_yields_check_failed(self, module):
        """A malformed (non-dict) check return -> CHECK_FAILED too."""
        server, db_mgr, ingest_engine, cfg = make_server(module)
        ingest_engine.run_daily_load.return_value = _load_result()
        db_mgr.check_daily_close_health.return_value = object()
        data = parse_result(
            server.do_action(MagicMock(), make_action("daily_load"))
        )
        assert data["status"] == "SUCCESS"
        assert data["health"]["health_status"] == "CHECK_FAILED"

    def test_health_check_failed_still_success(self, module):
        """A CHECK_FAILED report from the DB layer is passed through."""
        server, db_mgr, ingest_engine, cfg = make_server(module)
        ingest_engine.run_initial_load.return_value = _load_result()
        db_mgr.check_daily_close_health.return_value = {
            "health_status": "CHECK_FAILED",
            "error": "catalog error",
        }
        data = parse_result(
            server.do_action(MagicMock(), make_action("initial_load"))
        )
        assert data["status"] == "SUCCESS"
        assert data["health"]["health_status"] == "CHECK_FAILED"


class TestDataHealthAction:
    """The data_health action returns the FULL report (incl. gaps)."""

    def test_data_health_returns_full_report(self, module):
        server, db_mgr, ingest_engine, cfg = make_server(module)
        full_report = {
            "health_status": "WARN",
            "mode": "full",
            "checks": {"gaps": {"total_missing_days": 2}},
        }
        db_mgr.check_daily_close_health.return_value = full_report
        data = parse_result(
            server.do_action(MagicMock(), make_action("data_health"))
        )
        assert data["status"] == "SUCCESS"
        assert data["health"] == full_report
        call = db_mgr.check_daily_close_health.call_args
        assert call.kwargs["include_gaps"] is True
        # No gap_days in payload -> the DB default (30) is used.
        assert "gap_days" not in call.kwargs

    def test_data_health_forwards_gap_days(self, module):
        server, db_mgr, ingest_engine, cfg = make_server(module)
        db_mgr.check_daily_close_health.return_value = _OK_HEALTH
        data = parse_result(
            server.do_action(
                MagicMock(), make_action("data_health", {"gap_days": 14})
            )
        )
        assert data["status"] == "SUCCESS"
        call = db_mgr.check_daily_close_health.call_args
        assert call.kwargs["gap_days"] == 14

    @pytest.mark.parametrize("bad", [0, -1, "30", 2.5, True])
    def test_data_health_invalid_gap_days_raises(self, module, bad):
        server, db_mgr, ingest_engine, cfg = make_server(module)
        with pytest.raises(flight.FlightServerError):
            server.do_action(
                MagicMock(), make_action("data_health", {"gap_days": bad})
            )
        db_mgr.check_daily_close_health.assert_not_called()

    def test_data_health_check_raises_check_failed(self, module):
        """A raising check -> CHECK_FAILED report, action still succeeds."""
        server, db_mgr, ingest_engine, cfg = make_server(module)
        db_mgr.check_daily_close_health.side_effect = RuntimeError("boom")
        data = parse_result(
            server.do_action(MagicMock(), make_action("data_health"))
        )
        assert data["status"] == "SUCCESS"
        assert data["health"]["health_status"] == "CHECK_FAILED"
        assert "boom" in data["health"]["error"]


# =====================================================================
# F17-T1: composite health report (daily_close + fear_greed + weekly +
# company_metadata) — worst-status rollup, additive shape
# =====================================================================


class TestCompositeHealth:
    """The composite report composes per-section sub-reports (F17-T1)."""

    def test_composite_shape_backward_compatible(self, module):
        """Existing daily keys stay top-level; new keys are additive."""
        server, db_mgr, ingest_engine, cfg = make_server(module)
        daily = {
            "health_status": "OK",
            "mode": "light",
            "as_of": "2026-01-08",
            "latest_trading_day": "2026-01-08",
            "pending_session": None,
            "checks": {"staleness": {"stale_count": 0}},
        }
        db_mgr.check_daily_close_health.return_value = daily
        ingest_engine.run_daily_load.return_value = _load_result()
        data = parse_result(
            server.do_action(MagicMock(), make_action("daily_load"))
        )
        health = data["health"]
        # Every original daily_close key survives at the top level.
        for key, value in daily.items():
            assert health[key] == value, key
        # The composite sections each carry their own status.
        assert health["daily_close"]["health_status"] == "OK"
        assert health["fear_greed"]["health_status"] == "OK"
        assert health["weekly"]["health_status"] == "OK"
        assert health["company_metadata"]["health_status"] == "OK"
        assert health["health_status"] == "OK"

    def test_composite_worst_status_warn(self, module):
        """A WARN section drags the top-level status to WARN."""
        server, db_mgr, ingest_engine, cfg = make_server(module)
        db_mgr.check_weekly_health.return_value = {
            "health_status": "WARN",
            "checks": {"staleness": {"stale": True}},
        }
        ingest_engine.run_daily_load.return_value = _load_result()
        data = parse_result(
            server.do_action(MagicMock(), make_action("daily_load"))
        )
        health = data["health"]
        assert health["daily_close"]["health_status"] == "OK"
        assert health["weekly"]["health_status"] == "WARN"
        assert health["health_status"] == "WARN"

    def test_composite_worst_status_check_failed(self, module):
        """CHECK_FAILED outranks WARN — a section failure is isolated."""
        server, db_mgr, ingest_engine, cfg = make_server(module)
        db_mgr.check_weekly_health.return_value = {
            "health_status": "WARN",
        }
        db_mgr.check_fear_greed_health.side_effect = RuntimeError(
            "fg exploded"
        )
        ingest_engine.run_daily_load.return_value = _load_result()
        data = parse_result(
            server.do_action(MagicMock(), make_action("daily_load"))
        )
        health = data["health"]
        # The failed section is CHECK_FAILED with an error; the others
        # still ran (weekly kept its own WARN).
        assert health["fear_greed"]["health_status"] == "CHECK_FAILED"
        assert "fg exploded" in health["fear_greed"]["error"]
        assert health["weekly"]["health_status"] == "WARN"
        assert health["daily_close"]["health_status"] == "OK"
        assert health["health_status"] == "CHECK_FAILED"

    def test_composite_disabled_excluded_from_rollup(self, module):
        """A DISABLED section (fear_greed off) does not drag the worst."""
        server, db_mgr, ingest_engine, cfg = make_server(module)
        db_mgr.check_fear_greed_health.return_value = {
            "health_status": "DISABLED",
        }
        ingest_engine.run_daily_load.return_value = _load_result()
        data = parse_result(
            server.do_action(MagicMock(), make_action("daily_load"))
        )
        health = data["health"]
        assert health["fear_greed"]["health_status"] == "DISABLED"
        assert health["health_status"] == "OK"

    def test_composite_forwards_fear_greed_enabled_flag(self, module):
        """cfg.fear_greed.enabled is forwarded to the F&G check."""
        server, db_mgr, ingest_engine, cfg = make_server(module)
        cfg.fear_greed.enabled = False
        ingest_engine.run_daily_load.return_value = _load_result()
        server.do_action(MagicMock(), make_action("daily_load"))
        call = db_mgr.check_fear_greed_health.call_args
        assert call.kwargs["enabled"] is False

    def test_composite_metadata_gets_configured_tickers(self, module):
        """company_metadata coverage uses configured-minus-excluded."""
        server, db_mgr, ingest_engine, cfg = make_server(module)
        cfg.ingestion.tickers = ["AAPL", "MSFT"]
        cfg.ingestion.exclude_tickers = ["MSFT"]
        ingest_engine.run_daily_load.return_value = _load_result()
        server.do_action(MagicMock(), make_action("daily_load"))
        call = db_mgr.check_metadata_health.call_args
        assert call.kwargs["tickers"] == ["AAPL"]

    def test_composite_data_health_action_carries_sections(self, module):
        """The data_health action returns the full composite report."""
        server, db_mgr, ingest_engine, cfg = make_server(module)
        data = parse_result(
            server.do_action(MagicMock(), make_action("data_health"))
        )
        assert data["status"] == "SUCCESS"
        health = data["health"]
        # The mocked daily check returns the light stub — the action
        # still passes include_gaps=True down (asserted elsewhere).
        assert health["mode"] == "light"
        for section in (
            "daily_close", "fear_greed", "weekly", "company_metadata"
        ):
            assert section in health
            assert health[section]["health_status"] == "OK"


# =====================================================================
# Over-the-wire do_action — F7-T17 regression (finding F-A)
# =====================================================================
#
# All tests above call ``server.do_action`` directly in-process.  That
# blind spot allowed a bare ``flight.Result`` return (not iterable) to
# survive for months — pyarrow's server-side ``_do_action`` calls
# ``iter()`` on the handler's return value, so every action failed over
# real gRPC with ``TypeError: 'Result' object is not iterable``.
# The tests below run a REAL ``MarketDataFlightServer`` bound to an
# ephemeral port and a REAL ``FlightClient`` so the wire contract is
# exercised end-to-end.


def _ephemeral_location():
    """Return ``grpc://127.0.0.1:<free-port>`` for a throwaway server."""
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return f"grpc://127.0.0.1:{port}"


@pytest.fixture
def wire_server(module):
    """A real ``MarketDataFlightServer`` in a daemon thread + real client.

    The engine and db are mocked (no real DB or yfinance calls); only the
    gRPC transport is real.  Parametrized over both implementations via
    the ``module`` fixture.
    """
    db_mgr = MagicMock()
    ingest_engine = MagicMock()
    cfg = MagicMock()
    cfg.ingestion.daily_lookback_days = 4
    cfg.reports.report_dir = "/tmp/test_reports"

    location = _ephemeral_location()
    server = module.MarketDataFlightServer(location, db_mgr, ingest_engine, cfg)
    thread = threading.Thread(target=server.serve, daemon=True)
    thread.start()

    client = None
    try:
        client = flight.FlightClient(location)
        client.wait_for_available(timeout=10)
        yield server, client, db_mgr, ingest_engine, cfg
    finally:
        try:
            server.shutdown()
        except Exception:
            pass
        try:
            thread.join(timeout=2)
        except Exception:
            pass


class TestDoActionOverWire:
    """Real-gRPC ``do_action`` tests — catch the bare-Result TypeError."""

    def test_do_action_result_is_iterable_result(self, module):
        # Direct-call contract: the return value must be iterable (wire)
        # AND expose .body (in-process callers/tests).
        server, db_mgr, ingest_engine, cfg = make_server(module)
        ingest_engine.run_daily_load.return_value = {
            "rows_upserted": 1,
            "tickers_succeeded": ["AAPL"],
            "tickers_failed": [],
            "errors": {},
            "duration_ms": 5,
        }
        result = server.do_action(MagicMock(), make_action("daily_load"))
        assert isinstance(result, flight.Result)
        items = list(result)  # must be iterable for pyarrow _do_action
        assert len(items) == 1
        assert items[0] is result
        assert json.loads(result.body.to_pybytes())["status"] == "SUCCESS"

    def test_daily_load_over_wire(self, wire_server):
        # Real FlightClient -> do_action("daily_load", {"force": true})
        # must return one parseable JSON Result, not a TypeError.
        server, client, db_mgr, ingest_engine, cfg = wire_server
        ingest_engine.run_daily_load.return_value = {
            "rows_upserted": 12,
            "tickers_succeeded": ["AAPL", "MSFT"],
            "tickers_failed": [],
            "errors": {},
            "duration_ms": 42,
        }
        action = flight.Action(
            "daily_load", json.dumps({"force": True}).encode("utf-8")
        )
        results = list(client.do_action(action))
        assert len(results) == 1
        data = json.loads(results[0].body.to_pybytes().decode("utf-8"))
        assert data["status"] == "SUCCESS"
        assert data["rows_upserted"] == 12
        assert data["tickers_succeeded"] == ["AAPL", "MSFT"]
        ingest_engine.run_daily_load.assert_called_once()
        assert ingest_engine.run_daily_load.call_args.kwargs["force"] is True

    def test_daily_load_skipped_over_wire(self, wire_server):
        # SKIPPED passthrough must also survive the wire.
        server, client, db_mgr, ingest_engine, cfg = wire_server
        ingest_engine.run_daily_load.return_value = {
            "status": "SKIPPED",
            "reason": "Market closed",
            "duration_ms": 0,
        }
        results = list(
            client.do_action(flight.Action("daily_load", b"{}"))
        )
        assert len(results) == 1
        data = json.loads(results[0].body.to_pybytes().decode("utf-8"))
        assert data["status"] == "SKIPPED"
        assert data["reason"] == "Market closed"

    def test_unknown_action_over_wire_raises(self, wire_server):
        # Unknown action -> FlightServerError must propagate to client.
        _, client, _, _, _ = wire_server
        with pytest.raises(flight.FlightError):
            list(client.do_action(flight.Action("no_such_action", b"{}")))


# =====================================================================
# F10-T1: metadata_load action
# =====================================================================

def _metadata_result(**overrides):
    result = {
        "tickers_fetched": 2,
        "tickers_skipped": 0,
        "tickers_failed": [],
        "rows_upserted": 2,
        "aborted": False,
        "reason": None,
        "errors": {},
        "duration_ms": 10,
    }
    result.update(overrides)
    return result


class TestMetadataLoadAction:
    """The ``metadata_load`` action routes to engine.run_metadata_load
    with validated payload options (F10-T1)."""

    def test_metadata_load_success(self, module):
        server, db_mgr, ingest_engine, cfg = make_server(module)
        ingest_engine.run_metadata_load.return_value = _metadata_result()
        data = parse_result(
            server.do_action(MagicMock(), make_action("metadata_load"))
        )
        assert data["status"] == "SUCCESS"
        assert data["tickers_fetched"] == 2
        assert data["rows_upserted"] == 2
        assert data["aborted"] is False
        # No payload -> engine defaults (tickers=None -> configured,
        # refresh=False -> skip-existing).
        ingest_engine.run_metadata_load.assert_called_once_with(
            tickers=None, refresh=False
        )

    def test_metadata_load_forwards_options(self, module):
        server, db_mgr, ingest_engine, cfg = make_server(module)
        ingest_engine.run_metadata_load.return_value = _metadata_result()
        payload = {
            "tickers": ["AAPL", "MSFT"],
            "refresh": True,
            "delay_s": 1.5,
            "max_tickers": 10,
        }
        data = parse_result(
            server.do_action(
                MagicMock(), make_action("metadata_load", payload)
            )
        )
        assert data["status"] == "SUCCESS"
        ingest_engine.run_metadata_load.assert_called_once_with(
            tickers=["AAPL", "MSFT"],
            refresh=True,
            delay_s=1.5,
            max_tickers=10,
        )

    def test_metadata_load_partial_on_failures(self, module):
        server, db_mgr, ingest_engine, cfg = make_server(module)
        ingest_engine.run_metadata_load.return_value = _metadata_result(
            tickers_fetched=1,
            tickers_failed=["BAD"],
            errors={"BAD": "HTTP 429"},
            rows_upserted=1,
        )
        data = parse_result(
            server.do_action(MagicMock(), make_action("metadata_load"))
        )
        assert data["status"] == "PARTIAL"
        assert data["tickers_failed"] == ["BAD"]

    def test_metadata_load_aborted_status(self, module):
        """Circuit-breaker abort -> ABORTED status + reason."""
        server, db_mgr, ingest_engine, cfg = make_server(module)
        ingest_engine.run_metadata_load.return_value = _metadata_result(
            tickers_fetched=0,
            tickers_failed=["A", "B"],
            aborted=True,
            reason="circuit breaker: 15 consecutive fetch failures",
            rows_upserted=0,
        )
        data = parse_result(
            server.do_action(MagicMock(), make_action("metadata_load"))
        )
        assert data["status"] == "ABORTED"
        assert data["aborted"] is True
        assert "circuit breaker" in data["reason"]

    @pytest.mark.parametrize("bad", [0, -1, "5", 2.5, True])
    def test_metadata_load_invalid_max_tickers_raises(self, module, bad):
        """Non-int / bool / <1 max_tickers -> FlightServerError."""
        server, db_mgr, ingest_engine, cfg = make_server(module)
        with pytest.raises(flight.FlightServerError):
            server.do_action(
                MagicMock(),
                make_action("metadata_load", {"max_tickers": bad}),
            )
        ingest_engine.run_metadata_load.assert_not_called()

    @pytest.mark.parametrize("bad", ["fast", True, -0.5])
    def test_metadata_load_invalid_delay_s_raises(self, module, bad):
        """Non-numeric / bool / negative delay_s -> FlightServerError."""
        server, db_mgr, ingest_engine, cfg = make_server(module)
        with pytest.raises(flight.FlightServerError):
            server.do_action(
                MagicMock(),
                make_action("metadata_load", {"delay_s": bad}),
            )
        ingest_engine.run_metadata_load.assert_not_called()

    @pytest.mark.parametrize("bad", ["yes", 1, 0])
    def test_metadata_load_invalid_refresh_raises(self, module, bad):
        """Non-bool refresh -> FlightServerError (no silent coercion)."""
        server, db_mgr, ingest_engine, cfg = make_server(module)
        with pytest.raises(flight.FlightServerError):
            server.do_action(
                MagicMock(),
                make_action("metadata_load", {"refresh": bad}),
            )
        ingest_engine.run_metadata_load.assert_not_called()

    @pytest.mark.parametrize("bad", ["AAPL", {"t": "AAPL"}, [1, 2]])
    def test_metadata_load_invalid_tickers_raises(self, module, bad):
        """tickers must be a list of strings — anything else is rejected."""
        server, db_mgr, ingest_engine, cfg = make_server(module)
        with pytest.raises(flight.FlightServerError):
            server.do_action(
                MagicMock(),
                make_action("metadata_load", {"tickers": bad}),
            )
        ingest_engine.run_metadata_load.assert_not_called()

    def test_metadata_load_over_wire(self, wire_server):
        # Real gRPC: metadata_load must return one parseable JSON Result.
        server, client, db_mgr, ingest_engine, cfg = wire_server
        ingest_engine.run_metadata_load.return_value = _metadata_result(
            tickers_fetched=3, rows_upserted=3
        )
        action = flight.Action(
            "metadata_load",
            json.dumps({"refresh": True, "delay_s": 0.1}).encode("utf-8"),
        )
        results = list(client.do_action(action))
        assert len(results) == 1
        data = json.loads(results[0].body.to_pybytes().decode("utf-8"))
        assert data["status"] == "SUCCESS"
        assert data["tickers_fetched"] == 3
        ingest_engine.run_metadata_load.assert_called_once_with(
            tickers=None, refresh=True, delay_s=0.1
        )

    def test_metadata_load_validation_error_over_wire(self, wire_server):
        # Bad payload -> FlightServerError propagates to the client.
        _, client, _, ingest_engine, _ = wire_server
        action = flight.Action(
            "metadata_load",
            json.dumps({"max_tickers": "5"}).encode("utf-8"),
        )
        with pytest.raises(flight.FlightError):
            list(client.do_action(action))
        ingest_engine.run_metadata_load.assert_not_called()


# =====================================================================
# SEC-T3: metadata_load `overrides` payload (executor module only — the
# challenger variant does not implement overrides; same divergence
# precedent as the F24 signal_pipeline tests below)
# =====================================================================

_exec_module = importlib.import_module("src.flight_server")


class TestMetadataLoadOverridesAction:
    """The ``overrides`` key on ``metadata_load`` is shape-validated and
    forwarded to ``engine.run_metadata_load`` (SEC-T3)."""

    def _server(self):
        return make_server(_exec_module)

    def test_overrides_forwarded_to_engine(self):
        server, _, ingest_engine, _ = self._server()
        ingest_engine.run_metadata_load.return_value = _metadata_result(
            overrides_applied=2,
            overrides_report=[
                {
                    "ticker": "GPN",
                    "applied_fields": ["sector"],
                    "created": False,
                }
            ],
        )
        overrides = [
            {
                "ticker": "GPN",
                "sector": "Industrials",
                "industry": "Information Technology Services",
            },
            {"ticker": "NBIS", "sector": "Technology"},
        ]
        data = parse_result(
            server.do_action(
                MagicMock(),
                make_action("metadata_load", {"overrides": overrides}),
            )
        )
        assert data["status"] == "SUCCESS"
        assert data["overrides_applied"] == 2
        assert data["overrides_report"][0]["ticker"] == "GPN"
        ingest_engine.run_metadata_load.assert_called_once_with(
            tickers=None, refresh=False, overrides=overrides
        )

    def test_overrides_absent_not_forwarded(self):
        """No overrides key -> engine called without the kwarg (legacy
        call signature preserved)."""
        server, _, ingest_engine, _ = self._server()
        ingest_engine.run_metadata_load.return_value = _metadata_result()
        data = parse_result(
            server.do_action(MagicMock(), make_action("metadata_load"))
        )
        assert data["status"] == "SUCCESS"
        ingest_engine.run_metadata_load.assert_called_once_with(
            tickers=None, refresh=False
        )

    def test_overrides_combined_with_fetch_options(self):
        server, _, ingest_engine, _ = self._server()
        ingest_engine.run_metadata_load.return_value = _metadata_result()
        overrides = [{"ticker": "AAA", "sector": "Energy"}]
        data = parse_result(
            server.do_action(
                MagicMock(),
                make_action(
                    "metadata_load",
                    {
                        "tickers": ["AAA"],
                        "refresh": True,
                        "overrides": overrides,
                    },
                ),
            )
        )
        assert data["status"] == "SUCCESS"
        ingest_engine.run_metadata_load.assert_called_once_with(
            tickers=["AAA"], refresh=True, overrides=overrides
        )

    @pytest.mark.parametrize(
        "bad",
        [
            "not-a-list",
            {"ticker": "AAA"},
            [123],
            [["AAA"]],
            [{"sector": "X"}],  # missing ticker
            [{"ticker": ""}],  # empty ticker
            [{"ticker": "   "}],  # whitespace ticker
            [{"ticker": 5}],  # non-string ticker
            [{"ticker": "AAA", "sector": 7}],  # non-string field
            [{"ticker": "AAA", "industry": ["x"]}],
            [{"ticker": "AAA", "quote_type": True}],
            [{"ticker": "AAA", "bogus": "x"}],  # unknown key
        ],
    )
    def test_malformed_overrides_raise_before_engine(self, bad):
        """Malformed overrides -> FlightServerError; engine never runs."""
        server, _, ingest_engine, _ = self._server()
        with pytest.raises(flight.FlightServerError):
            server.do_action(
                MagicMock(),
                make_action("metadata_load", {"overrides": bad}),
            )
        ingest_engine.run_metadata_load.assert_not_called()

    def test_null_field_values_accepted(self):
        """Explicit null field values pass validation (engine treats them
        as absent — merge semantics)."""
        server, _, ingest_engine, _ = self._server()
        ingest_engine.run_metadata_load.return_value = _metadata_result()
        overrides = [{"ticker": "AAA", "sector": None, "industry": "Y"}]
        data = parse_result(
            server.do_action(
                MagicMock(),
                make_action("metadata_load", {"overrides": overrides}),
            )
        )
        assert data["status"] == "SUCCESS"
        ingest_engine.run_metadata_load.assert_called_once_with(
            tickers=None, refresh=False, overrides=overrides
        )

    def _wire_server(self):
        """Spin up the EXECUTOR server on an ephemeral port + real client.

        Same pattern as ``TestSignalPipelineAction._wire_server`` —
        returns ``(server, client, db_mgr, ingest_engine, cfg, thread)``;
        callers must shut down in a finally block.
        """
        db_mgr = MagicMock()
        ingest_engine = MagicMock()
        cfg = MagicMock()
        cfg.ingestion.daily_lookback_days = 4
        cfg.reports.report_dir = "/tmp/test_reports"
        location = _ephemeral_location()
        server = _exec_module.MarketDataFlightServer(
            location, db_mgr, ingest_engine, cfg
        )
        thread = threading.Thread(target=server.serve, daemon=True)
        thread.start()
        client = flight.FlightClient(location)
        client.wait_for_available(timeout=10)
        return server, client, db_mgr, ingest_engine, cfg, thread

    def test_malformed_overrides_over_wire(self):
        """Bad overrides over real gRPC -> FlightError at the client."""
        server, client, db_mgr, ingest_engine, cfg, thread = (
            self._wire_server()
        )
        try:
            action = flight.Action(
                "metadata_load",
                json.dumps({"overrides": [{"sector": "X"}]}).encode(
                    "utf-8"
                ),
            )
            with pytest.raises(flight.FlightError):
                list(client.do_action(action))
            ingest_engine.run_metadata_load.assert_not_called()
        finally:
            try:
                server.shutdown()
            except Exception:
                pass
            try:
                thread.join(timeout=2)
            except Exception:
                pass


# =====================================================================
# F12-T1: weekly_refresh action
# =====================================================================


def _weekly_refresh_result(**overrides):
    result = {
        "status": "SUCCESS",
        "weekly_close_rows": 10,
        "weekly_indicators_rows": 10,
        "duration_ms": 15,
    }
    result.update(overrides)
    return result


class TestWeeklyRefreshAction:
    """The ``weekly_refresh`` action routes to
    engine.run_weekly_refresh (F12-T1).  Empty/absent payload OK."""

    def test_weekly_refresh_success(self, module):
        server, db_mgr, ingest_engine, cfg = make_server(module)
        ingest_engine.run_weekly_refresh.return_value = (
            _weekly_refresh_result()
        )
        data = parse_result(
            server.do_action(MagicMock(), make_action("weekly_refresh"))
        )
        assert data["status"] == "SUCCESS"
        assert data["weekly_close_rows"] == 10
        assert data["weekly_indicators_rows"] == 10
        ingest_engine.run_weekly_refresh.assert_called_once_with()

    def test_weekly_refresh_empty_payload(self, module):
        """An explicit empty JSON payload is accepted."""
        server, db_mgr, ingest_engine, cfg = make_server(module)
        ingest_engine.run_weekly_refresh.return_value = (
            _weekly_refresh_result(weekly_close_rows=0)
        )
        data = parse_result(
            server.do_action(
                MagicMock(), make_action("weekly_refresh", {})
            )
        )
        assert data["status"] == "SUCCESS"
        assert data["weekly_close_rows"] == 0
        ingest_engine.run_weekly_refresh.assert_called_once_with()

    def test_weekly_refresh_failure_returns_failed(self, module):
        """A raising engine -> status FAILED with error, not a crash."""
        server, db_mgr, ingest_engine, cfg = make_server(module)
        ingest_engine.run_weekly_refresh.side_effect = RuntimeError(
            "weekly boom"
        )
        data = parse_result(
            server.do_action(MagicMock(), make_action("weekly_refresh"))
        )
        assert data["status"] == "FAILED"
        assert "weekly boom" in data["error"]

    def test_weekly_refresh_over_wire(self, wire_server):
        # Real gRPC: weekly_refresh must return one parseable JSON Result.
        server, client, db_mgr, ingest_engine, cfg = wire_server
        ingest_engine.run_weekly_refresh.return_value = (
            _weekly_refresh_result(weekly_close_rows=7)
        )
        results = list(
            client.do_action(flight.Action("weekly_refresh", b"{}"))
        )
        assert len(results) == 1
        data = json.loads(results[0].body.to_pybytes().decode("utf-8"))
        assert data["status"] == "SUCCESS"
        assert data["weekly_close_rows"] == 7
        ingest_engine.run_weekly_refresh.assert_called_once_with()


# =====================================================================
# F24-T1: signal_pipeline action (executor module only — the challenger
# variant is out of scope for F24, so these tests do NOT use the
# parametrized ``module``/``wire_server`` fixtures)
# =====================================================================

_executor_module = importlib.import_module("src.flight_server")


def _signal_pipeline_result(**overrides):
    result = {
        "status": "SUCCESS",
        "steps": {
            "ingest": {"status": "SUCCESS", "inserted": 2},
            "dedupe": {"deleted": 0},
            "repair": {"repaired": 0, "still_null": 0},
            "prune": {"pruned_count": 1, "remaining": 1},
            "performance": {"rows_processed": 1},
            "repair_sweep": {"repaired": 0},
            "null_verification": {"null_close_remaining": 0},
            "cluster_metadata": {"rows": 6},
        },
        "duration_ms": 12,
    }
    result.update(overrides)
    return result


class TestSignalPipelineAction:
    """The ``signal_pipeline`` action routes to
    engine.run_signal_pipeline with validated payload overrides."""

    def _server(self):
        return make_server(_executor_module)

    def test_signal_pipeline_success_no_overrides(self):
        server, db_mgr, ingest_engine, cfg = self._server()
        ingest_engine.run_signal_pipeline.return_value = (
            _signal_pipeline_result()
        )
        data = parse_result(
            server.do_action(MagicMock(), make_action("signal_pipeline"))
        )
        assert data["status"] == "SUCCESS"
        assert data["steps"]["ingest"]["inserted"] == 2
        assert data["steps"]["cluster_metadata"]["rows"] == 6
        # Empty payload -> engine is called with no overrides (config
        # defaults apply).
        ingest_engine.run_signal_pipeline.assert_called_once_with()

    def test_signal_pipeline_forwards_overrides(self):
        server, db_mgr, ingest_engine, cfg = self._server()
        ingest_engine.run_signal_pipeline.return_value = (
            _signal_pipeline_result()
        )
        payload = {
            "folder_path": r"C:\tmp\signals",
            "backdate_days": 5,
            "max_holding_days": 10,
            "overwrite_existing": False,
            "min_reward_risk": 2.0,
            "min_target_pct": 8.0,
            "exclude_clusters": [0, 4],
            "exclude_models": ["XRT"],
            "filter_below_open": True,
        }
        data = parse_result(
            server.do_action(
                MagicMock(), make_action("signal_pipeline", payload)
            )
        )
        assert data["status"] == "SUCCESS"
        ingest_engine.run_signal_pipeline.assert_called_once_with(
            **payload
        )

    def test_signal_pipeline_engine_failure_returns_failed(self):
        """Engine exceptions surface as status=FAILED (weekly_refresh
        precedent), not a transport error."""
        server, db_mgr, ingest_engine, cfg = self._server()
        ingest_engine.run_signal_pipeline.side_effect = RuntimeError(
            "pipeline boom"
        )
        data = parse_result(
            server.do_action(MagicMock(), make_action("signal_pipeline"))
        )
        assert data["status"] == "FAILED"
        assert "pipeline boom" in data["error"]

    @pytest.mark.parametrize("bad", ["3", 2.5, True, 0, -1])
    def test_invalid_backdate_days_raises(self, bad):
        server, db_mgr, ingest_engine, cfg = self._server()
        with pytest.raises(flight.FlightServerError):
            server.do_action(
                MagicMock(),
                make_action("signal_pipeline", {"backdate_days": bad}),
            )
        ingest_engine.run_signal_pipeline.assert_not_called()

    @pytest.mark.parametrize("bad", ["8", 2.5, True, 0, -3])
    def test_invalid_max_holding_days_raises(self, bad):
        server, db_mgr, ingest_engine, cfg = self._server()
        with pytest.raises(flight.FlightServerError):
            server.do_action(
                MagicMock(),
                make_action(
                    "signal_pipeline", {"max_holding_days": bad}
                ),
            )
        ingest_engine.run_signal_pipeline.assert_not_called()

    @pytest.mark.parametrize("bad", ["yes", 1, 0])
    def test_invalid_overwrite_existing_raises(self, bad):
        server, db_mgr, ingest_engine, cfg = self._server()
        with pytest.raises(flight.FlightServerError):
            server.do_action(
                MagicMock(),
                make_action(
                    "signal_pipeline", {"overwrite_existing": bad}
                ),
            )
        ingest_engine.run_signal_pipeline.assert_not_called()

    @pytest.mark.parametrize(
        "key", ["min_reward_risk", "min_target_pct"]
    )
    @pytest.mark.parametrize("bad", ["high", True, [1.0]])
    def test_invalid_thresholds_raise(self, key, bad):
        server, db_mgr, ingest_engine, cfg = self._server()
        with pytest.raises(flight.FlightServerError):
            server.do_action(
                MagicMock(), make_action("signal_pipeline", {key: bad})
            )
        ingest_engine.run_signal_pipeline.assert_not_called()

    @pytest.mark.parametrize(
        "key", ["min_reward_risk", "min_target_pct"]
    )
    @pytest.mark.parametrize(
        "bad", [float("nan"), float("inf"), float("-inf")]
    )
    def test_non_finite_thresholds_raise(self, key, bad):
        """F24-T2: NaN/Infinity/-Infinity pass ``isinstance(x, float)``
        and survive the json.dumps/json.loads round-trip as bare
        literals — but DuckDB ``x < NaN``/``x < +Inf`` is TRUE for
        every row, which would prune ALL of signal_records."""
        server, db_mgr, ingest_engine, cfg = self._server()
        with pytest.raises(flight.FlightServerError):
            server.do_action(
                MagicMock(), make_action("signal_pipeline", {key: bad})
            )
        ingest_engine.run_signal_pipeline.assert_not_called()

    @pytest.mark.parametrize(
        "key", ["min_reward_risk", "min_target_pct"]
    )
    @pytest.mark.parametrize("good", [0.0, 1.39, 5])
    def test_finite_thresholds_accepted(self, key, good):
        """Finite floats (incl. 0.0) and ints still pass validation."""
        server, db_mgr, ingest_engine, cfg = self._server()
        ingest_engine.run_signal_pipeline.return_value = (
            _signal_pipeline_result()
        )
        data = parse_result(
            server.do_action(
                MagicMock(), make_action("signal_pipeline", {key: good})
            )
        )
        assert data["status"] == "SUCCESS"
        ingest_engine.run_signal_pipeline.assert_called_once_with(
            **{key: good}
        )

    def test_non_finite_threshold_over_wire(self):
        """A literal ``NaN`` token in the JSON body reaches the client
        as a FlightError over real gRPC."""
        server, client, db_mgr, ingest_engine, cfg, thread = (
            self._wire_server()
        )
        try:
            action = flight.Action(
                "signal_pipeline",
                b'{"min_reward_risk": NaN}',
            )
            with pytest.raises(flight.FlightError):
                list(client.do_action(action))
            ingest_engine.run_signal_pipeline.assert_not_called()
        finally:
            try:
                server.shutdown()
            except Exception:
                pass
            try:
                thread.join(timeout=2)
            except Exception:
                pass

    @pytest.mark.parametrize(
        "bad", [0, "4", [True], ["a"], {"c": 4}]
    )
    def test_invalid_exclude_clusters_raises(self, bad):
        server, db_mgr, ingest_engine, cfg = self._server()
        with pytest.raises(flight.FlightServerError):
            server.do_action(
                MagicMock(),
                make_action(
                    "signal_pipeline", {"exclude_clusters": bad}
                ),
            )
        ingest_engine.run_signal_pipeline.assert_not_called()

    @pytest.mark.parametrize("bad", ["XRT", [1, 2], {"m": "X"}])
    def test_invalid_exclude_models_raises(self, bad):
        server, db_mgr, ingest_engine, cfg = self._server()
        with pytest.raises(flight.FlightServerError):
            server.do_action(
                MagicMock(),
                make_action("signal_pipeline", {"exclude_models": bad}),
            )
        ingest_engine.run_signal_pipeline.assert_not_called()

    @pytest.mark.parametrize("bad", ["yes", 1])
    def test_invalid_filter_below_open_raises(self, bad):
        server, db_mgr, ingest_engine, cfg = self._server()
        with pytest.raises(flight.FlightServerError):
            server.do_action(
                MagicMock(),
                make_action(
                    "signal_pipeline", {"filter_below_open": bad}
                ),
            )
        ingest_engine.run_signal_pipeline.assert_not_called()

    @pytest.mark.parametrize("bad", ["", "   ", 123, ["x"]])
    def test_invalid_folder_path_raises(self, bad):
        server, db_mgr, ingest_engine, cfg = self._server()
        with pytest.raises(flight.FlightServerError):
            server.do_action(
                MagicMock(),
                make_action("signal_pipeline", {"folder_path": bad}),
            )
        ingest_engine.run_signal_pipeline.assert_not_called()

    def _wire_server(self):
        """Spin up the executor server on an ephemeral port + real client.

        Returns ``(server, client, db_mgr, ingest_engine, cfg, thread)``;
        callers must shut down in a finally block (see tests below).
        """
        db_mgr = MagicMock()
        ingest_engine = MagicMock()
        cfg = MagicMock()
        cfg.ingestion.daily_lookback_days = 4
        cfg.reports.report_dir = "/tmp/test_reports"
        location = _ephemeral_location()
        server = _executor_module.MarketDataFlightServer(
            location, db_mgr, ingest_engine, cfg
        )
        thread = threading.Thread(target=server.serve, daemon=True)
        thread.start()
        client = flight.FlightClient(location)
        client.wait_for_available(timeout=10)
        return server, client, db_mgr, ingest_engine, cfg, thread

    def test_signal_pipeline_over_wire(self):
        """Real gRPC: the action returns one parseable JSON Result."""
        server, client, db_mgr, ingest_engine, cfg, thread = (
            self._wire_server()
        )
        ingest_engine.run_signal_pipeline.return_value = (
            _signal_pipeline_result()
        )
        try:
            results = list(
                client.do_action(
                    flight.Action(
                        "signal_pipeline",
                        json.dumps({"backdate_days": 7}).encode("utf-8"),
                    )
                )
            )
            assert len(results) == 1
            data = json.loads(
                results[0].body.to_pybytes().decode("utf-8")
            )
            assert data["status"] == "SUCCESS"
            ingest_engine.run_signal_pipeline.assert_called_once_with(
                backdate_days=7
            )
        finally:
            try:
                server.shutdown()
            except Exception:
                pass
            try:
                thread.join(timeout=2)
            except Exception:
                pass

    def test_signal_pipeline_validation_error_over_wire(self):
        """Bad payload -> FlightServerError propagates to the client."""
        server, client, db_mgr, ingest_engine, cfg, thread = (
            self._wire_server()
        )
        try:
            action = flight.Action(
                "signal_pipeline",
                json.dumps({"backdate_days": "3"}).encode("utf-8"),
            )
            with pytest.raises(flight.FlightError):
                list(client.do_action(action))
            ingest_engine.run_signal_pipeline.assert_not_called()
        finally:
            try:
                server.shutdown()
            except Exception:
                pass
            try:
                thread.join(timeout=2)
            except Exception:
                pass


# =====================================================================
# F25-T1: signal_analysis action (executor module only — mirrors
# TestSignalPipelineAction; the challenger variant is out of scope)
# =====================================================================


def _signal_analysis_result(**overrides):
    result = {
        "status": "SUCCESS",
        "sets": {
            "version_p": {
                "status": "SUCCESS",
                "schema": "signals_version_p",
                "folder": r"C:\Intel\parquet\quant_strategy_building\version_p",
                "report_dir": r"C:\Intel\parquet\signal_analysis\version_p",
                "steps": {
                    "ingest": {"status": "SUCCESS", "inserted": 3},
                    "dedupe": {"deleted": 0, "remaining": 3},
                    "repair": {"repaired": 0, "still_null": 0},
                    "views": {"status": "SUCCESS", "views": {}},
                    "performance": {"rows_processed": 2},
                    "phase5": {"status": "SUCCESS"},
                },
                "duration_ms": 42,
            }
        },
        "duration_ms": 42,
    }
    result.update(overrides)
    return result


class TestSignalAnalysisAction:
    """The ``signal_analysis`` action routes to
    engine.run_signal_analysis with validated payload overrides."""

    def _server(self, set_names=("version_p", "version_q")):
        import types

        server, db_mgr, ingest_engine, cfg = make_server(_executor_module)
        # SimpleNamespace: MagicMock(name=...) clashes with the mock's
        # own ``name`` kwarg, so use real namespaces for set entries.
        cfg.signal_analysis.sets = [
            types.SimpleNamespace(name=n) for n in set_names
        ]
        return server, db_mgr, ingest_engine, cfg

    def test_signal_analysis_success_no_overrides(self):
        server, db_mgr, ingest_engine, cfg = self._server()
        ingest_engine.run_signal_analysis.return_value = (
            _signal_analysis_result()
        )
        data = parse_result(
            server.do_action(
                MagicMock(), make_action("signal_analysis")
            )
        )
        assert data["status"] == "SUCCESS"
        assert data["sets"]["version_p"]["schema"] == "signals_version_p"
        ingest_engine.run_signal_analysis.assert_called_once_with()

    def test_signal_analysis_forwards_overrides(self):
        server, db_mgr, ingest_engine, cfg = self._server()
        ingest_engine.run_signal_analysis.return_value = (
            _signal_analysis_result()
        )
        payload = {
            "set": "version_q",
            "backdate_days": 30,
            "max_holding_days": 10,
            "mc_iterations": 20,
        }
        data = parse_result(
            server.do_action(
                MagicMock(), make_action("signal_analysis", payload)
            )
        )
        assert data["status"] == "SUCCESS"
        ingest_engine.run_signal_analysis.assert_called_once_with(
            set_name="version_q",
            backdate_days=30,
            max_holding_days=10,
            mc_iterations=20,
        )

    def test_signal_analysis_engine_failure_returns_failed(self):
        """Engine exceptions surface as status=FAILED (weekly_refresh
        precedent), not a transport error."""
        server, db_mgr, ingest_engine, cfg = self._server()
        ingest_engine.run_signal_analysis.side_effect = RuntimeError(
            "analysis boom"
        )
        data = parse_result(
            server.do_action(MagicMock(), make_action("signal_analysis"))
        )
        assert data["status"] == "FAILED"
        assert "analysis boom" in data["error"]

    def test_signal_analysis_engine_valueerror_raises(self):
        """A ValueError from the engine (unknown set) becomes a
        FlightServerError, not a FAILED status."""
        server, db_mgr, ingest_engine, cfg = self._server()
        ingest_engine.run_signal_analysis.side_effect = ValueError(
            "unknown signal_analysis set 'x'"
        )
        with pytest.raises(flight.FlightServerError):
            server.do_action(
                MagicMock(), make_action("signal_analysis")
            )

    @pytest.mark.parametrize("bad", ["nope", "Version_P", "", " other"])
    def test_unknown_or_invalid_set_rejected(self, bad):
        """A ``set`` payload must match a configured set name."""
        server, db_mgr, ingest_engine, cfg = self._server()
        with pytest.raises(flight.FlightServerError):
            server.do_action(
                MagicMock(),
                make_action("signal_analysis", {"set": bad}),
            )
        ingest_engine.run_signal_analysis.assert_not_called()

    @pytest.mark.parametrize("bad", [123, ["version_p"], {"s": 1}, True])
    def test_non_string_set_rejected(self, bad):
        server, db_mgr, ingest_engine, cfg = self._server()
        with pytest.raises(flight.FlightServerError):
            server.do_action(
                MagicMock(),
                make_action("signal_analysis", {"set": bad}),
            )
        ingest_engine.run_signal_analysis.assert_not_called()

    @pytest.mark.parametrize(
        "key", ["backdate_days", "max_holding_days", "mc_iterations"]
    )
    @pytest.mark.parametrize(
        "bad", ["90", 2.5, True, 0, -3, [5], {"x": 1}]
    )
    def test_invalid_numeric_params_raise(self, key, bad):
        server, db_mgr, ingest_engine, cfg = self._server()
        with pytest.raises(flight.FlightServerError):
            server.do_action(
                MagicMock(), make_action("signal_analysis", {key: bad})
            )
        ingest_engine.run_signal_analysis.assert_not_called()

    @pytest.mark.parametrize(
        "key", ["backdate_days", "max_holding_days", "mc_iterations"]
    )
    @pytest.mark.parametrize(
        "bad", [float("nan"), float("inf"), float("-inf")]
    )
    def test_non_finite_numeric_params_raise(self, key, bad):
        """NaN/Infinity survive the json round-trip as bare literals —
        math.isfinite must reject them (F24-T2 lesson carried forward:
        a NaN bound silently poisons DuckDB comparisons)."""
        server, db_mgr, ingest_engine, cfg = self._server()
        with pytest.raises(flight.FlightServerError):
            server.do_action(
                MagicMock(), make_action("signal_analysis", {key: bad})
            )
        ingest_engine.run_signal_analysis.assert_not_called()

    @pytest.mark.parametrize(
        "key", ["backdate_days", "max_holding_days", "mc_iterations"]
    )
    def test_integral_float_accepted(self, key):
        """An integral float (e.g. ``90.0``) is finite + integral —
        forwarded to the engine as a real int."""
        server, db_mgr, ingest_engine, cfg = self._server()
        ingest_engine.run_signal_analysis.return_value = (
            _signal_analysis_result()
        )
        data = parse_result(
            server.do_action(
                MagicMock(),
                make_action("signal_analysis", {key: 90.0}),
            )
        )
        assert data["status"] == "SUCCESS"
        ingest_engine.run_signal_analysis.assert_called_once_with(
            **{key: 90}
        )

    def _wire_server(self):
        """Spin up the executor server on an ephemeral port + real client."""
        import types

        db_mgr = MagicMock()
        ingest_engine = MagicMock()
        cfg = MagicMock()
        cfg.ingestion.daily_lookback_days = 4
        cfg.reports.report_dir = "/tmp/test_reports"
        cfg.signal_analysis.sets = [
            types.SimpleNamespace(name="version_p")
        ]
        location = _ephemeral_location()
        server = _executor_module.MarketDataFlightServer(
            location, db_mgr, ingest_engine, cfg
        )
        thread = threading.Thread(target=server.serve, daemon=True)
        thread.start()
        client = flight.FlightClient(location)
        client.wait_for_available(timeout=10)
        return server, client, db_mgr, ingest_engine, cfg,


# =====================================================================
# dml_admin action — Flight surface over IngestionEngine.run_dml_admin
# (executor-module only — mirrors the F24/F25 signal-pipeline precedent)
# =====================================================================

@pytest.mark.skipif(
    _executor_module is None, reason="executor module not importable"
)
class TestDmlAdminAction:
    """``dml_admin`` dispatches a JSON payload to
    ``IngestionEngine.run_dml_admin`` and returns its structured result
    verbatim — REJECTED/FAILED travel in the body, not as transport
    errors (the whole file's convention)."""

    def _server(self):
        return make_server(_executor_module)

    def _dml_result(self, status="SUCCESS", **extra):
        result = {
            "status": status,
            "op": "register_namespace",
            "target": "pivot",
        }
        result.update(extra)
        return result

    def test_dml_admin_success_passthrough(self):
        server, db_mgr, ingest_engine, cfg = self._server()
        ingest_engine.run_dml_admin.return_value = self._dml_result()
        payload = {
            "op": "register_namespace",
            "namespace": "pivot",
            "client_tag": "test",
        }
        data = parse_result(
            server.do_action(
                MagicMock(), make_action("dml_admin", payload)
            )
        )
        assert data["status"] == "SUCCESS"
        ingest_engine.run_dml_admin.assert_called_once_with(payload)

    def test_dml_admin_rejected_passthrough(self):
        """A REJECTED result is a normal response body, not a
        FlightServerError."""
        server, db_mgr, ingest_engine, cfg = self._server()
        ingest_engine.run_dml_admin.return_value = self._dml_result(
            status="REJECTED", error="not allowlisted"
        )
        data = parse_result(
            server.do_action(
                MagicMock(),
                make_action(
                    "dml_admin",
                    {"op": "register_namespace", "namespace": "other"},
                ),
            )
        )
        assert data["status"] == "REJECTED"
        assert data["error"] == "not allowlisted"

    def test_dml_admin_engine_failure_passthrough(self):
        server, db_mgr, ingest_engine, cfg = self._server()
        ingest_engine.run_dml_admin.return_value = self._dml_result(
            status="FAILED", error="constraint violation"
        )
        data = parse_result(
            server.do_action(
                MagicMock(),
                make_action(
                    "dml_admin",
                    {"op": "insert_select", "table": "pivot.t",
                     "mode": "append", "sql": "SELECT 1"},
                ),
            )
        )
        assert data["status"] == "FAILED"

    def test_dml_admin_engine_exception_returns_failed(self):
        """An engine raise (not a result dict) becomes a FAILED body —
        DmlRejected/ValueError are caught inside run_dml_admin, so an
        escape here means an infrastructure bug."""
        server, db_mgr, ingest_engine, cfg = self._server()
        ingest_engine.run_dml_admin.side_effect = RuntimeError("boom")
        data = parse_result(
            server.do_action(
                MagicMock(),
                make_action("dml_admin", {"op": "list_tables"}),
            )
        )
        assert data["status"] == "FAILED"
        assert "boom" in data["error"]

    def test_dml_admin_non_dict_payload_raises(self):
        server, db_mgr, ingest_engine, cfg = self._server()
        with pytest.raises(flight.FlightServerError):
            server.do_action(
                MagicMock(),
                make_action("dml_admin", [1, 2, 3]),
            )
        ingest_engine.run_dml_admin.assert_not_called()

    def test_dml_admin_over_wire(self):
        """End-to-end: real FlightClient -> do_action -> JSON result."""
        db_mgr = MagicMock()
        ingest_engine = MagicMock()
        cfg = MagicMock()
        ingest_engine.run_dml_admin.return_value = self._dml_result()
        location = _ephemeral_location()
        srv = _executor_module.MarketDataFlightServer(
            location, db_mgr, ingest_engine, cfg
        )
        thread = threading.Thread(target=srv.serve, daemon=True)
        thread.start()
        try:
            client = flight.FlightClient(location)
            client.wait_for_available(timeout=10)
            action = flight.Action(
                "dml_admin",
                json.dumps(
                    {"op": "register_namespace", "namespace": "pivot"}
                ).encode("utf-8"),
            )
            result = client.do_action(action)
            buf = next(iter(result)).body.to_pybytes()
            data = json.loads(buf.decode("utf-8"))
            assert data["status"] == "SUCCESS"
        finally:
            srv.shutdown()


# =====================================================================
# Generalized do_put — descriptor-driven DML vs legacy daily_close
# =====================================================================

@pytest.mark.skipif(
    _executor_module is None, reason="executor module not importable"
)
class TestDoPutDescriptor:
    """The ``do_put`` descriptor contract: a CMD descriptor carrying
    ``{"table": ..., "mode": ...}`` routes to ``run_dml_put``; anything
    else (PATH descriptor, unknown type, non-JSON, missing 'table')
    falls through to the legacy daily_close upsert path."""

    def _cmd_descriptor(self, body):
        d = MagicMock()
        d.descriptor_type = flight.DescriptorType.CMD
        d.command = json.dumps(body).encode("utf-8")
        return d

    def _path_descriptor(self):
        d = MagicMock()
        d.descriptor_type = flight.DescriptorType.PATH
        d.path = [b"pivot.t"]
        return d

    def _reader(self, table):
        r = MagicMock()
        r.read_all.return_value = table
        return r

    def test_parse_descriptor_happy(self):
        parsed = _executor_module._parse_dml_descriptor(
            self._cmd_descriptor({"table": "pivot.t", "mode": "append"})
        )
        assert parsed == {"table": "pivot.t", "mode": "append"}
        # A non-str 'table' still routes to the DML path — the engine
        # validates the value and rejects+audits it there.
        routed = _executor_module._parse_dml_descriptor(
            self._cmd_descriptor({"table": 5})
        )
        assert routed == {"table": 5}

    @pytest.mark.parametrize(
        "descriptor",
        [
            # PATH descriptor -> legacy
            flight.FlightDescriptor.for_path(b"pivot.t"),
            # CMD with non-JSON command -> legacy
            flight.FlightDescriptor.for_command(b"not json"),
            # CMD with JSON but no 'table' -> legacy
            flight.FlightDescriptor.for_command(b'{"mode": "append"}'),
        ],
    )
    def test_parse_descriptor_falls_through(self, descriptor):
        parsed = _executor_module._parse_dml_descriptor(descriptor)
        assert parsed is None

    def test_parse_descriptor_none_and_unknown(self):
        parse = _executor_module._parse_dml_descriptor
        assert parse(None) is None
        d = MagicMock()
        d.descriptor_type = flight.DescriptorType.UNKNOWN
        d.command = b'{"table": "pivot.t"}'
        assert parse(d) is None

    def test_do_put_descriptor_routes_to_run_dml_put(self):
        server, db_mgr, ingest_engine, cfg = make_server(
            _executor_module
        )
        ingest_engine.run_dml_put.return_value = {
            "status": "SUCCESS", "rows_written": 1,
        }
        table = pa.table({"a": [1]})
        descriptor = self._cmd_descriptor(
            {"table": "pivot.t", "mode": "create_or_replace"}
        )
        server.do_put(
            MagicMock(), descriptor, self._reader(table), MagicMock()
        )
        ingest_engine.run_dml_put.assert_called_once_with(
            {"table": "pivot.t", "mode": "create_or_replace"}, table
        )
        db_mgr.upsert_dataframe.assert_not_called()

    def test_do_put_descriptor_rejection_raises(self):
        from src.client_dml import DmlRejected

        server, db_mgr, ingest_engine, cfg = make_server(
            _executor_module
        )
        ingest_engine.run_dml_put.side_effect = DmlRejected(
            "not allowlisted"
        )
        table = pa.table({"a": [1]})
        with pytest.raises(flight.FlightServerError):
            server.do_put(
                MagicMock(),
                self._cmd_descriptor(
                    {"table": "main.x", "mode": "append"}
                ),
                self._reader(table),
                MagicMock(),
            )
        db_mgr.upsert_dataframe.assert_not_called()

    def _daily_close_table(self):
        import datetime as _dt

        return pa.table(
            {
                "Date": pa.array([_dt.date(2026, 1, 2)],
                                 type=pa.date32()),
                "Ticker": ["AAPL"],
                "Open": [1.0],
                "High": [2.0],
                "Low": [0.5],
                "Close": [1.5],
                "Volume": [100.0],
                "RawClose": [1.5],
            },
            schema=_executor_module._DAILY_CLOSE_SCHEMA,
        )

    def test_do_put_no_descriptor_stays_legacy(self):
        """Backward compat: batches with no/unknown descriptor still
        use the daily_close upsert path (the historical contract)."""
        server, db_mgr, ingest_engine, cfg = make_server(
            _executor_module
        )
        db_mgr.upsert_dataframe.return_value = {
            "total_rows": 1, "rows_inserted": 1, "rows_updated": 0,
        }
        table = self._daily_close_table()
        server.do_put(
            MagicMock(), MagicMock(), self._reader(table), MagicMock()
        )
        ingest_engine.run_dml_put.assert_not_called()
        db_mgr.upsert_dataframe.assert_called_once()

    def test_do_put_path_descriptor_stays_legacy(self):
        server, db_mgr, ingest_engine, cfg = make_server(
            _executor_module
        )
        db_mgr.upsert_dataframe.return_value = {
            "total_rows": 1, "rows_inserted": 1, "rows_updated": 0,
        }
        table = self._daily_close_table()
        server.do_put(
            MagicMock(), self._path_descriptor(),
            self._reader(table), MagicMock(),
        )
        ingest_engine.run_dml_put.assert_not_called()
        db_mgr.upsert_dataframe.assert_called_once()


# =====================================================================
# F26-T5: do_get single-statement enforcement (challenger C-F26T4-1)
# =====================================================================
#
# The challenger's round-4 live probe proved that a prefix-only
# ``_validate_select_only`` let multi-statement tickets through and
# ``conn.execute`` ran EVERY statement: 'SELECT 1; SET
# autoload_known_extensions=true' re-armed the F26-T4 kill-switch
# instance-wide, 'SELECT 1; LOAD inet' loaded an installed extension,
# and the same channel reached SET enable_external_access=false (a
# one-way R2 breaker), INSTALL and SET memory_limit/threads.  The
# executor impl now enforces exactly one SELECT-type statement via
# ``duckdb.extract_statements`` — the same check
# ``client_dml.validate_source_sql`` applies.
#
# Scope note: these tests pin the EXECUTOR implementation only
# (``_executor_module`` = src.flight_server).  The challenger variant
# keeps its own regex-prefix validator and is outside F26-T5's
# output_files.


def _do_get_ticket(sql):
    """Build a ``do_get`` ticket carrying ``{"sql": sql}``."""
    return flight.Ticket(json.dumps({"sql": sql}).encode("utf-8"))


# The challenger's exact smuggle vectors plus second-statement variants.
_REARM_VECTORS = [
    # The proven re-arm: one ticket flips the kill-switch server-wide.
    "SELECT 1; SET autoload_known_extensions=true",
    "SELECT 1; SET autoinstall_known_extensions=true",
    # Direct LOAD of an installed extension (no autoload needed).
    "SELECT 1; LOAD inet",
    # INSTALL = network egress for not-on-disk extensions.
    "SELECT 1; INSTALL tpch",
    # One-way breaker: kills the server's R2/httpfs until restart.
    "SELECT 1; SET enable_external_access=false",
    # Performance degradation knobs.
    "SELECT 1; SET memory_limit='64MB'",
    "SELECT 1; SET threads=1",
    # Three-statement re-arm + exploit chain.
    "SELECT 1; SET autoload_known_extensions=true; "
    "SELECT stem('running','english')",
    # Plain second statements (any kind) must die too.
    "SELECT 1; SELECT 2",
    "SELECT 1; DROP TABLE daily_close",
    "SELECT 1; DELETE FROM daily_close",
    "SET autoload_known_extensions=true; SELECT 1",
    "SELECT 1;\nSET autoload_known_extensions = true",
    # A trailing unparsable fragment is still not a clean single
    # statement — extract_statements raises and we reject.
    "SELECT 1; garbage$$$",
]

# Legit single-statement SELECTs that must keep passing — the shapes
# real consumers (and the report scripts) send.
_LEGIT_SELECTS = [
    "SELECT 1",
    "SELECT 42 AS answer",
    "SELECT * FROM daily_close",
    "SELECT 1;",
    "SELECT 1;;",
    "  SELECT 1  ;  ",
    "SELECT 1\n;\n",
    # Comment-only tails count as the single statement.
    "SELECT 1 -- trailing comment",
    "SELECT 1; -- comment-only tail",
    "SELECT 1; /* comment-only tail */",
    "SELECT 1;\n-- comment-only tail\n",
    "SELECT 1 /* block comment */",
    # WITH ... SELECT classifies SELECT.
    "WITH cte AS (SELECT * FROM daily_close) SELECT * FROM cte",
    "WITH x AS (SELECT 1 AS a) SELECT * FROM x;",
    # Multi-line (the §7 recipe style).
    "SELECT Ticker,\n       Close\nFROM daily_close\nORDER BY Date",
    # A semicolon inside a string literal is not a second statement.
    "SELECT '; SET x=1' AS s",
]


class TestDoGetSingleStatementValidation:
    """``_validate_select_only`` — single-statement SELECT enforcement."""

    @pytest.mark.parametrize("sql", _REARM_VECTORS)
    def test_multi_statement_rejected(self, sql):
        with pytest.raises(ValueError):
            _executor_module._validate_select_only(sql)

    def test_with_insert_rejected(self):
        # A single statement that classifies INSERT (not SELECT) —
        # passes the WITH prefix but must die on the type check.
        with pytest.raises(ValueError):
            _executor_module._validate_select_only(
                "WITH x AS (SELECT 1) INSERT INTO t SELECT * FROM x"
            )

    @pytest.mark.parametrize("sql", _LEGIT_SELECTS)
    def test_legit_single_select_accepted(self, sql):
        # Must not raise.
        _executor_module._validate_select_only(sql)


@pytest.fixture
def exec_wire_db(tmp_path):
    """REAL executor server + real client + REAL DatabaseManager.

    Unlike ``wire_server`` (mocked db), a real DB is required here so a
    successful smuggle would leave real evidence — flipped settings or
    a loaded extension — and the post-attempt assertions can read the
    actual server state back.
    """
    from src.db import DatabaseManager

    db_mgr = DatabaseManager(str(tmp_path / "f26t5.duckdb"))
    cfg = types.SimpleNamespace(
        server=types.SimpleNamespace(query_timeout_seconds=30),
    )
    location = _ephemeral_location()
    server = _executor_module.MarketDataFlightServer(
        location, db_mgr, MagicMock(), cfg
    )
    thread = threading.Thread(target=server.serve, daemon=True)
    thread.start()
    try:
        client = flight.FlightClient(location)
        client.wait_for_available(timeout=10)
        yield server, client, db_mgr
    finally:
        try:
            server.shutdown()
        except Exception:
            pass
        try:
            thread.join(timeout=2)
        except Exception:
            pass
        try:
            db_mgr.close()
        except Exception:
            pass


class TestDoGetMultiStatementOverWire:
    """Real-gRPC + real-DuckDB proof that the re-arm vectors die at
    validation — never reaching the conn — and leave no trace."""

    @pytest.mark.parametrize("sql", _REARM_VECTORS)
    def test_smuggle_ticket_rejected_before_conn(self, exec_wire_db, sql):
        _, client, db_mgr = exec_wire_db
        spy = MagicMock(wraps=db_mgr.execute_query)
        db_mgr.execute_query = spy
        with pytest.raises(flight.FlightError):
            client.do_get(_do_get_ticket(sql)).read_all()
        # The ticket never reached DuckDB — rejected at validation.
        spy.assert_not_called()

    def test_server_state_untouched_after_rearm_attempts(self, exec_wire_db):
        """Fire every re-arm vector over real gRPC, then prove the
        F26-T4 kill-switch is still armed, the one-way
        ``enable_external_access`` breaker is NOT tripped, and an
        autoloadable extension fn still cannot bind."""
        _, client, db_mgr = exec_wire_db
        # justified: each smuggle vector must be replayed verbatim over
        # the wire — no vectorizable shortcut exists for a gRPC call.
        for sql in _REARM_VECTORS:
            with pytest.raises(flight.FlightError):
                client.do_get(_do_get_ticket(sql)).read_all()
        for setting in (
            "autoinstall_known_extensions",
            "autoload_known_extensions",
        ):
            value = db_mgr.conn.execute(
                f"SELECT current_setting('{setting}')"
            ).fetchone()[0]
            assert value is False, f"{setting} re-armed to {value!r}"
        assert (
            db_mgr.conn.execute(
                "SELECT current_setting('enable_external_access')"
            ).fetchone()[0]
            is True
        )
        # Autoload is still dead end-to-end: stem() (fts) fails at bind.
        with pytest.raises(Exception) as excinfo:
            db_mgr.execute_query("SELECT stem('running','english')")
        assert "extension" in str(excinfo.value).lower()

    @pytest.mark.parametrize(
        "sql",
        [
            "SELECT 42 AS answer",
            "SELECT 42 AS answer;",
            "WITH x AS (SELECT 7 AS v) SELECT v FROM x",
            "SELECT 1 AS a,\n       2 AS b",
            "SELECT 1 AS a; -- comment-only tail",
        ],
    )
    def test_legit_queries_still_served(self, exec_wire_db, sql):
        """The read path itself is unbroken over the wire."""
        _, client, _ = exec_wire_db
        table = client.do_get(_do_get_ticket(sql)).read_all()
        assert table.num_rows == 1

    def test_legit_query_returns_data(self, exec_wire_db):
        _, client, _ = exec_wire_db
        table = client.do_get(
            _do_get_ticket("SELECT 42 AS answer")
        ).read_all()
        assert table.to_pandas().iloc[0, 0] == 42
