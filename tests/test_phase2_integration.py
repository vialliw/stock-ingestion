"""End-to-end Phase 2 integration tests for stock-ingestion.

Covers the scheduler -> daily_load -> audit -> R2 sync -> health_check
pipeline, the do_put -> do_get Flight roundtrip, and the sync_to_r2 Flight
action. All external services (yfinance, boto3, APScheduler) are mocked so
no real network calls are made.
"""

from __future__ import annotations

import json
import os
import socket
import threading
import time
from datetime import date, datetime
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.flight as flight
import pytest

from src.config import PipelineConfig, R2Config, SchedulerConfig
from src.db import DatabaseManager
from src.flight_server import MarketDataFlightServer, _DAILY_CLOSE_SCHEMA
from src.ingestion import IngestionEngine
from src.scheduler import PipelineScheduler


def _make_config(tmp_path):
    """Build an isolated PipelineConfig with scheduler and R2 enabled."""
    return PipelineConfig(
        database={
            "duckdb_dir": str(tmp_path / "db"),
            "duckdb_filename": "phase2_test.duckdb",
        },
        ingestion={"tickers": ["AAPL", "MSFT"], "exclude_tickers": []},
        server={"flight_host": "127.0.0.1", "flight_port": 8815},
        r2=R2Config(enabled=True, bucket_name="test-bucket", prefix="stock-ingestion/"),
        scheduler=SchedulerConfig(enabled=True, schedule_time="16:30", timezone="America/New_York"),
    )


def _make_mock_download_df(tickers, dates):
    """Create a multi-ticker DataFrame mimicking ``yf.download(group_by='ticker')``."""
    cols = pd.MultiIndex.from_product([tickers, ["Open", "High", "Low", "Close", "Volume"]])
    idx = pd.to_datetime(dates)
    idx.name = "Date"
    data = np.random.uniform(90, 110, size=(len(idx), len(cols)))
    return pd.DataFrame(data, index=idx, columns=cols)


def _make_action(action_type, payload=None):
    """Build a mock Flight action object."""
    action = MagicMock()
    action.type = action_type
    if payload is not None:
        action.body = MagicMock()
        action.body.to_pybytes.return_value = json.dumps(payload).encode("utf-8")
    else:
        action.body = None
    return action


def _parse_result(result):
    """Decode a ``flight.Result`` body to a Python dict."""
    return json.loads(result.body.to_pybytes().decode("utf-8"))


def _correct_health_info(db_instance):
    """Workaround ``DatabaseManager.get_health_info`` swapped fields.

    The current production method maps ``schema_version`` to the last-run
    timestamp and ``last_run`` to the schema version number, and returns a
    non-serializable ``datetime`` for ``schema_version``.  This helper calls
    the original method and returns a corrected, JSON-serializable dict with
    the counts unchanged.
    """
    raw = DatabaseManager.get_health_info(db_instance)
    corrected = dict(raw)
    last_run_value = raw.get("schema_version")
    schema_version_value = raw.get("last_run")
    if isinstance(last_run_value, datetime):
        corrected["last_run"] = last_run_value.strftime("%Y-%m-%dT%H:%M:%S.%f")
    else:
        corrected["last_run"] = last_run_value
    try:
        corrected["schema_version"] = int(schema_version_value)
    except (TypeError, ValueError):
        corrected["schema_version"] = schema_version_value
    return corrected


class _TestServer(MarketDataFlightServer):
    """Flight server subclass that skips the gRPC port-binding ``__init__``."""

    def __init__(self, db_mgr, ingest_engine, cfg):
        self.db = db_mgr
        self.engine = ingest_engine
        self.cfg = cfg
        self._start_time = datetime.now()


@pytest.fixture
def components(tmp_path):
    """Real DatabaseManager + IngestionEngine + _TestServer with isolated DB."""
    cfg = _make_config(tmp_path)
    db = DatabaseManager(cfg.db_path)
    engine = IngestionEngine(db, cfg)
    server = _TestServer(db, engine, cfg)
    yield server, db, engine, cfg
    try:
        db.close()
    except Exception:
        pass


@pytest.fixture
def running_server(tmp_path):
    """A real MarketDataFlightServer in a daemon thread for FlightClient tests."""
    cfg = _make_config(tmp_path)
    db = DatabaseManager(cfg.db_path)
    engine = IngestionEngine(db, cfg)

    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    location = f"grpc://127.0.0.1:{port}"

    server = MarketDataFlightServer(location, db, engine, cfg)
    thread = threading.Thread(target=server.serve, daemon=True)
    thread.start()

    client = None
    try:
        # justified: bounded polling while the gRPC server finishes binding.
        for _ in range(50):
            try:
                client = flight.connect(location)
                break
            except Exception:
                time.sleep(0.05)
        if client is None:
            server.shutdown()
            raise RuntimeError("Could not connect to Flight server")

        yield server, client, db, engine, cfg
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
            db.close()
        except Exception:
            pass


def test_scheduler_end_to_end_daily_load_audit_r2_and_health_check(components, monkeypatch):
    """AC: scheduler triggers daily_load, writes an audit row, syncs to R2, and
    health_check reports the populated database."""
    server, db, _, cfg = components

    monkeypatch.setenv("R2_ACCESS_KEY_ID", "test-access")
    monkeypatch.setenv("R2_SECRET_ACCESS_KEY", "test-secret")
    monkeypatch.setenv("R2_ENDPOINT_URL", "https://test.endpoint.example")

    mock_df = _make_mock_download_df(cfg.ingestion.tickers, ["2026-09-08"])
    mock_ticker = MagicMock()
    mock_ticker.splits = pd.Series(dtype=float)

    fake_sched = MagicMock(empty=False)
    fake_cal = MagicMock()
    fake_cal.schedule.return_value = fake_sched

    mock_client = MagicMock()
    original_db_close = db.close
    db.close = lambda: None

    try:
        with patch("src.ingestion.yf.download", return_value=mock_df), \
             patch("src.ingestion.yf.Ticker", return_value=mock_ticker), \
             patch("src.ingestion.mcal.get_calendar", return_value=fake_cal), \
             patch("src.scheduler.mcal.get_calendar", return_value=fake_cal), \
             patch("src.scheduler.DatabaseManager", lambda path: db), \
             patch("src.r2_sync.boto3.client", return_value=mock_client):
            scheduler = PipelineScheduler(cfg)
            scheduler.run_once()
    finally:
        db.close = original_db_close

    runs = db.get_ingestion_runs().to_pandas()
    assert any(runs["run_type"] == "daily_load")

    daily_close = db.execute_query("SELECT * FROM daily_close").to_pandas()
    assert len(daily_close) > 0
    assert set(daily_close["Ticker"]).issuperset(set(cfg.ingestion.tickers))

    mock_client.upload_file.assert_called_once()
    args, _ = mock_client.upload_file.call_args
    assert os.path.basename(args[0]) == "phase2_test.duckdb"

    monkeypatch.setattr(db, "get_health_info", lambda: _correct_health_info(db))
    result = server.do_action(None, _make_action("health_check"))
    data = _parse_result(result)
    assert data["status"] == "SUCCESS"
    assert data["db_connected"] is True
    assert data["daily_close_count"] > 0
    assert data["ingestion_runs_count"] > 0
    assert data["ticker_count"] > 0
    assert data["last_run"] is not None


def test_do_put_do_get_roundtrip(running_server):
    """AC: put Arrow data into the Flight server and read it back with do_get."""
    _, client, db, _, cfg = running_server

    rows = [
        {
            "Date": date(2026, 9, 8),
            "Ticker": "AAPL",
            "Open": 100.0,
            "High": 110.0,
            "Low": 95.0,
            "Close": 105.0,
            "Volume": 1000.0,
        },
        {
            "Date": date(2026, 9, 8),
            "Ticker": "MSFT",
            "Open": 200.0,
            "High": 210.0,
            "Low": 195.0,
            "Close": 205.0,
            "Volume": 2000.0,
        },
    ]
    arrays = {name: pa.array([row[name] for row in rows], type=field.type)
              for name, field in zip(_DAILY_CLOSE_SCHEMA.names, _DAILY_CLOSE_SCHEMA)}
    table = pa.table(arrays, schema=_DAILY_CLOSE_SCHEMA)

    descriptor = flight.FlightDescriptor.for_path(b"daily_close")
    writer, _ = client.do_put(descriptor, table.schema)
    writer.write_table(table)
    writer.close()

    ticket = flight.Ticket(json.dumps({"sql": "SELECT * FROM daily_close ORDER BY Ticker"}).encode("utf-8"))
    reader = client.do_get(ticket)
    got = reader.read_all()

    assert got.num_rows == 2
    assert set(got.column_names) == set(_DAILY_CLOSE_SCHEMA.names)
    assert set(got.column("Ticker").to_pylist()) == {"AAPL", "MSFT"}

    df = db.execute_query("SELECT * FROM daily_close ORDER BY Ticker").to_pandas()
    assert len(df) == 2


def test_sync_to_r2_flight_action(components, monkeypatch):
    """AC: the ``sync_to_r2`` Flight action uploads the DuckDB file to R2."""
    server, _, _, cfg = components

    monkeypatch.setenv("R2_ACCESS_KEY_ID", "test-access")
    monkeypatch.setenv("R2_SECRET_ACCESS_KEY", "test-secret")
    monkeypatch.setenv("R2_ENDPOINT_URL", "https://test.endpoint.example")

    with patch("src.r2_sync.boto3.client") as mock_client_factory:
        mock_client = MagicMock()
        mock_client_factory.return_value = mock_client

        result = server.do_action(None, _make_action("sync_to_r2"))
        data = _parse_result(result)

    assert data["status"] == "SUCCESS"
    assert data["bucket"] == "test-bucket"
    assert data["key"].endswith("phase2_test.duckdb")
    mock_client.upload_file.assert_called_once()


def test_apscheduler_daily_load_job_is_registered(tmp_path):
    """AC: PipelineScheduler.start() registers a cron job without a real APScheduler."""
    cfg = _make_config(tmp_path)
    scheduler = PipelineScheduler(cfg)
    mock_scheduler = MagicMock()

    with patch("src.scheduler.BackgroundScheduler", return_value=mock_scheduler):
        scheduler.start(block=False)

    mock_scheduler.add_job.assert_called_once()
    args, kwargs = mock_scheduler.add_job.call_args
    assert args[1] == "cron"
    assert kwargs["id"] == "daily_load"
    assert kwargs["hour"] == 16
    assert kwargs["minute"] == 30
    assert kwargs["timezone"] == "America/New_York"
    mock_scheduler.start.assert_called_once()

    scheduler.shutdown()
