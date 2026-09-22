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
from unittest.mock import MagicMock, patch

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
