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


# =====================================================================
# _validate_select_only — acceptance criteria 1-6
# =====================================================================

class TestValidateSelectOnly:
    """Tests for the module-level ``_validate_select_only`` function."""

    def test_accepts_simple_select(self, module):
        # AC 1: accepts 'SELECT * FROM daily_quotes'
        module._validate_select_only("SELECT * FROM daily_quotes")

    def test_accepts_with_cte(self, module):
        # AC 2: accepts 'WITH cte AS (SELECT * FROM daily_quotes) SELECT * FROM cte'
        module._validate_select_only(
            "WITH cte AS (SELECT * FROM daily_quotes) SELECT * FROM cte"
        )

    def test_rejects_delete(self, module):
        # AC 3: rejects 'DELETE FROM daily_quotes'
        with pytest.raises(ValueError):
            module._validate_select_only("DELETE FROM daily_quotes")

    def test_rejects_drop(self, module):
        # AC 4: rejects 'DROP TABLE daily_quotes'
        with pytest.raises(ValueError):
            module._validate_select_only("DROP TABLE daily_quotes")

    def test_rejects_insert(self, module):
        # AC 5: rejects 'INSERT INTO daily_quotes VALUES (...)'
        with pytest.raises(ValueError):
            module._validate_select_only(
                "INSERT INTO daily_quotes VALUES (1, 'AAPL', 100, 200)"
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
