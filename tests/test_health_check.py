"""Tests for the health_check and get_run_history Flight actions (F7-T2)."""

from __future__ import annotations

import json
import socket
import threading
from datetime import datetime
from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.flight as flight
import pytest

from src.db import DatabaseManager
from src.flight_server import MarketDataFlightServer


def _make_server(db_mgr, cfg):
    """Return a MarketDataFlightServer whose base __init__ is skipped."""

    class _TestServer(MarketDataFlightServer):
        def __init__(self, db_mgr, ingest_engine, cfg):
            self.db = db_mgr
            self.engine = ingest_engine
            self.cfg = cfg
            self._start_time = datetime.now()

    return _TestServer(db_mgr, MagicMock(), cfg)


def _make_action(action_type, payload=None):
    action = MagicMock()
    action.type = action_type
    if payload is not None:
        action.body = MagicMock()
        action.body.to_pybytes.return_value = json.dumps(payload).encode("utf-8")
    else:
        action.body = None
    return action


def _parse_result(result):
    return json.loads(result.body.to_pybytes().decode("utf-8"))


def test_health_check_returns_db_connectivity_and_row_counts():
    db = MagicMock()
    db.get_health_info.return_value = {
        "db_connected": True,
        "ticker_count": 5,
        "daily_close_count": 100,
        "splits_processed_count": 2,
        "ingestion_runs_count": 3,
        "last_run": "2026-09-07T12:00:00",
        "schema_version": 3,
    }
    cfg = MagicMock()
    cfg.db_path = "C:\\Intel\\parquet\\stock_ingestion.duckdb"

    server = _make_server(db, cfg)
    result = server.do_action(MagicMock(), _make_action("health_check"))
    data = _parse_result(result)

    assert data["status"] == "SUCCESS"
    assert data["db_connected"] is True
    assert data["db_path"] == cfg.db_path
    assert data["ticker_count"] == 5
    assert data["daily_close_count"] == 100
    assert data["splits_processed_count"] == 2
    assert data["ingestion_runs_count"] == 3
    assert data["last_run"] == "2026-09-07T12:00:00"
    assert data["schema_version"] == 3
    assert isinstance(data["uptime_seconds"], int)
    assert data["uptime_seconds"] >= 0

    db.get_health_info.assert_called_once()


def test_health_check_degraded_when_db_not_connected():
    db = MagicMock()
    db.get_health_info.return_value = {"db_connected": False}
    cfg = MagicMock()
    cfg.db_path = "C:\\Intel\\parquet\\stock_ingestion.duckdb"

    server = _make_server(db, cfg)
    result = server.do_action(MagicMock(), _make_action("health_check"))
    data = _parse_result(result)

    assert data["status"] == "DEGRADED"
    assert data["db_connected"] is False


def test_get_run_history_returns_arrow_table_as_json():
    db = MagicMock()
    db.get_ingestion_runs.return_value = pa.table({
        "run_id": [1],
        "run_type": ["daily_load"],
        "rows_upserted": [10],
        "tickers_succeeded": [1],
        "tickers_failed": [0],
    })
    cfg = MagicMock()
    cfg.db_path = "C:\\Intel\\parquet\\stock_ingestion.duckdb"

    server = _make_server(db, cfg)
    result = server.do_action(MagicMock(), _make_action("get_run_history"))
    data = _parse_result(result)

    assert data["status"] == "SUCCESS"
    assert len(data["run_history"]) == 1
    assert data["run_history"][0]["run_id"] == 1
    assert data["run_history"][0]["run_type"] == "daily_load"
    assert data["run_history"][0]["rows_upserted"] == 10
    assert data["run_history"][0]["tickers_succeeded"] == 1
    db.get_ingestion_runs.assert_called_once()


def test_get_run_history_empty_table():
    db = MagicMock()
    db.get_ingestion_runs.return_value = pa.table({
        "run_id": pa.array([], type=pa.int64()),
        "run_type": pa.array([], type=pa.string()),
        "rows_upserted": pa.array([], type=pa.int64()),
    })
    cfg = MagicMock()
    cfg.db_path = "C:\\Intel\\parquet\\stock_ingestion.duckdb"

    server = _make_server(db, cfg)
    result = server.do_action(MagicMock(), _make_action("get_run_history"))
    data = _parse_result(result)

    assert data["status"] == "SUCCESS"
    assert data["run_history"] == []


def test_database_schema_version_is_three(tmp_path):
    """A fresh DatabaseManager initializes the schema to version 3."""
    db_path = tmp_path / "schema_test.duckdb"
    db = DatabaseManager(str(db_path))
    try:
        row = db.conn.execute(
            "SELECT MAX(version) FROM schema_version"
        ).fetchone()
        assert row[0] == 3
    finally:
        db.close()


def test_get_ingestion_runs_returns_arrow_table(tmp_path):
    """DatabaseManager.get_ingestion_runs returns a pyarrow Table."""
    db_path = tmp_path / "runs_test.duckdb"
    db = DatabaseManager(str(db_path))
    try:
        table = db.get_ingestion_runs()
        assert isinstance(table, pa.Table)
        assert hasattr(table, "schema")
        assert "run_id" in table.schema.names
        assert "run_type" in table.schema.names

        db.record_ingestion_run(
            "daily_load",
            {
                "rows_upserted": 5,
                "tickers_succeeded": ["AAPL"],
                "tickers_failed": [],
                "errors": {},
                "duration_ms": 100,
            },
        )
        table = db.get_ingestion_runs()
        assert table.num_rows == 1
        assert table.column("run_type").to_pylist() == ["daily_load"]
    finally:
        db.close()


# =====================================================================
# F7-T17 regressions
# =====================================================================
#
# F-B: ``DatabaseManager.get_health_info`` mapped ``last_run`` to the
# schema-version column (row[4]) and ``schema_version`` to the
# MAX(completed_at) datetime column (row[5]) — a silent swap that made
# ``json.dumps`` crash in the health_check action.  The tests below use
# a REAL DatabaseManager so the swap cannot hide behind mocks.
#
# F-A: ``do_action`` returned a bare ``flight.Result`` (not iterable),
# which raised ``TypeError`` over real gRPC.  The wire test spins a real
# server + real ``FlightClient`` on an ephemeral port.


def _record_run(db):
    """Insert one ingestion_runs audit row so last_run is non-NULL."""
    return db.record_ingestion_run(
        "daily_load",
        {
            "status": "SUCCESS",
            "rows_upserted": 5,
            "tickers_succeeded": ["AAPL"],
            "tickers_failed": [],
            "errors": {},
            "duration_ms": 100,
        },
    )


def test_get_health_info_field_mapping(tmp_path):
    """F-B regression: last_run <- run timestamp, schema_version <- int."""
    db = DatabaseManager(str(tmp_path / "health_map.duckdb"))
    try:
        _record_run(db)
        health = db.get_health_info()

        assert health["db_connected"] is True
        assert health["schema_version"] == 3
        assert isinstance(health["schema_version"], int)
        # MAX(completed_at) is a datetime — must be stringified, not the int
        assert isinstance(health["last_run"], str)
        assert health["ingestion_runs_count"] == 1
        # The whole dict must be JSON-serialisable (the wire action does
        # json.dumps on it).
        json.dumps(health)
    finally:
        db.close()


def test_health_check_over_wire(tmp_path):
    """F-A+F-B regression: health_check exercised through a real
    ``FlightClient`` against a real server on an ephemeral port.

    Catches the bare-Result TypeError (F-A) AND the get_health_info index
    swap (F-B) — the action calls ``json.dumps`` on the real health dict.
    """
    db = DatabaseManager(str(tmp_path / "health_wire.duckdb"))
    _record_run(db)

    cfg = MagicMock()
    cfg.db_path = str(tmp_path / "health_wire.duckdb")
    cfg.ingestion.daily_lookback_days = 4
    cfg.reports.report_dir = str(tmp_path / "reports")

    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    location = f"grpc://127.0.0.1:{port}"

    server = MarketDataFlightServer(location, db, MagicMock(), cfg)
    thread = threading.Thread(target=server.serve, daemon=True)
    thread.start()
    try:
        client = flight.FlightClient(location)
        client.wait_for_available(timeout=10)

        results = list(client.do_action(flight.Action("health_check", b"{}")))
        assert len(results) == 1
        data = json.loads(results[0].body.to_pybytes().decode("utf-8"))

        assert data["status"] == "SUCCESS"
        assert data["db_connected"] is True
        assert data["db_path"] == cfg.db_path
        assert data["schema_version"] == 3
        assert isinstance(data["schema_version"], int)
        assert isinstance(data["last_run"], str)
        assert data["ingestion_runs_count"] == 1
        assert isinstance(data["uptime_seconds"], int)
        assert data["uptime_seconds"] >= 0
    finally:
        try:
            server.shutdown()
        except Exception:
            pass
        try:
            thread.join(timeout=2)
        except Exception:
            pass
        db.close()


def test_get_run_history_over_wire(tmp_path):
    """F-A regression: get_run_history over real gRPC returns parseable
    JSON (previously raised TypeError: Result object is not iterable)."""
    db = DatabaseManager(str(tmp_path / "runs_wire.duckdb"))
    _record_run(db)

    cfg = MagicMock()
    cfg.db_path = str(tmp_path / "runs_wire.duckdb")
    cfg.ingestion.daily_lookback_days = 4
    cfg.reports.report_dir = str(tmp_path / "reports")

    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    location = f"grpc://127.0.0.1:{port}"

    server = MarketDataFlightServer(location, db, MagicMock(), cfg)
    thread = threading.Thread(target=server.serve, daemon=True)
    thread.start()
    try:
        client = flight.FlightClient(location)
        client.wait_for_available(timeout=10)

        results = list(
            client.do_action(flight.Action("get_run_history", b"{}"))
        )
        assert len(results) == 1
        data = json.loads(results[0].body.to_pybytes().decode("utf-8"))
        assert data["status"] == "SUCCESS"
        assert len(data["run_history"]) == 1
        assert data["run_history"][0]["run_type"] == "daily_load"
    finally:
        try:
            server.shutdown()
        except Exception:
            pass
        try:
            thread.join(timeout=2)
        except Exception:
            pass
        db.close()
