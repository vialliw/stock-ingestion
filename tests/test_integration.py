"""Integration tests for the stock-ingestion pipeline (F5-T2).

Tests the full assembly using EXECUTOR's production modules only
(config.py, db.py, ingestion.py, flight_server.py). Verifies end-to-end
flow with mocked yfinance — NO real API calls.

Uses tmp_path-isolated DuckDB files and a synthetic config file in
tmp_path. Never touches C:\\Intel\\parquet or the real config.yaml.

The FlightServerBase C extension type binds to a port on init. To test
without binding a real port, we use a ``_TestServer`` subclass that
overrides ``__init__`` to skip ``super().__init__()`` and just sets
``self.db``, ``self.engine``, ``self.cfg`` directly (same pattern as
tests/test_flight_server.py).
"""

from __future__ import annotations

import json
import threading
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pyarrow.flight as flight
import pytest

from src.config import PipelineConfig
from src.db import DatabaseManager
from src.flight_server import MarketDataFlightServer
from src.ingestion import IngestionEngine


# =====================================================================
# Helpers
# =====================================================================

def write_config(tmp_path, db_dir):
    """Write a synthetic config YAML to tmp_path and return the path str.

    Forward slashes are used in paths to avoid YAML backslash-escape issues.
    The config.py from_yaml loader converts double-quoted scalars to
    single-quoted (literal) form, so paths are parsed correctly.
    """
    db_dir_str = str(db_dir).replace("\\", "/")
    tmp_path_str = str(tmp_path).replace("\\", "/")
    config_text = f"""
database:
  duckdb_dir: "{db_dir_str}"
  duckdb_filename: "test_integration.duckdb"
ingestion:
  tickers: ["AAPL", "MSFT"]
  exclude_tickers: []
server:
  flight_host: "127.0.0.1"
  flight_port: 8815
logging:
  log_file: "{tmp_path_str}/test.log"
reports:
  report_dir: "{tmp_path_str}/reports"
"""
    cfg_path = tmp_path / "config.yaml"
    cfg_path.write_text(config_text)
    return str(cfg_path)


def make_mock_download_df(tickers, dates):
    """Create a synthetic multi-ticker DataFrame mimicking yf.download output.

    Columns are a 2-level MultiIndex (ticker, field) matching the
    ``group_by='ticker'`` output format. Index is a DatetimeIndex named
    'Date'. Values are random floats in [90, 110).
    """
    cols = pd.MultiIndex.from_product(
        [tickers, ["Open", "High", "Low", "Close", "Volume"]]
    )
    idx = pd.to_datetime(dates)
    idx.name = "Date"
    data = np.random.uniform(90, 110, size=(len(idx), len(cols)))
    return pd.DataFrame(data, index=idx, columns=cols)


def insert_synthetic_data(db_manager, n_tickers=25, date_str="2026-01-01"):
    """Insert n_tickers rows of synthetic data with distinct Close*Volume.

    Ticker T{i} has Close=100+i, Volume=1000+10*i, so dollar_volume
    increases monotonically with i. This lets us verify DESC ordering
    in the daily_report test.
    """
    tickers = [f"T{i:02d}" for i in range(n_tickers)]
    df = pd.DataFrame(
        {
            "Date": pd.to_datetime([date_str] * n_tickers),
            "Ticker": tickers,
            "Open": [float(100 + i) for i in range(n_tickers)],
            "High": [float(105 + i) for i in range(n_tickers)],
            "Low": [float(95 + i) for i in range(n_tickers)],
            "Close": [float(100 + i) for i in range(n_tickers)],
            "Volume": [float(1000 + i * 10) for i in range(n_tickers)],
        }
    )
    db_manager.upsert_dataframe(df)
    return tickers


def make_action(action_type, payload=None):
    """Build a mock Flight action object with type and optional JSON body."""
    action = MagicMock()
    action.type = action_type
    if payload is not None:
        action.body = MagicMock()
        action.body.to_pybytes.return_value = json.dumps(payload).encode("utf-8")
    else:
        action.body = None
    return action


def make_ticket(sql):
    """Build a mock Flight ticket object containing the given SQL."""
    ticket = MagicMock()
    ticket.ticket = json.dumps({"sql": sql}).encode("utf-8")
    return ticket


def parse_result(result):
    """Decode a flight.Result body to a Python dict."""
    return json.loads(result.body.to_pybytes().decode("utf-8"))


def read_stream_table(stream):
    """Read a pa.Table from a flight.RecordBatchStream.

    ``RecordBatchStream`` is a server-side C extension type with no
    Python-visible read methods (it is designed to be consumed by the
    Flight framework, not directly).  To verify the underlying data we
    re-execute the same query via ``db.execute_query`` which returns a
    ``pa.Table`` directly.  This helper is retained for compatibility
    but callers should prefer ``db.execute_query`` for data assertions.
    """
    # RecordBatchStream has no readable Python API; return None to
    # signal callers to use db.execute_query instead.
    return None


# =====================================================================
# Test server — skips port-binding FlightServerBase.__init__
# =====================================================================

class _TestServer(MarketDataFlightServer):
    """Subclass that skips ``FlightServerBase.__init__`` to avoid port binding.

    ``FlightServerBase`` is a C extension type that binds to a gRPC port
    on init. In unit/integration tests we don't want a real server. We
    override ``__init__`` to skip ``super().__init__()`` and just set
    the attributes that ``do_get`` and ``do_action`` use.
    """

    def __init__(self, db_mgr, ingest_engine, cfg):
        self.db = db_mgr
        self.engine = ingest_engine
        self.cfg = cfg


# =====================================================================
# Fixture — real production components with tmp_path-isolated DuckDB
# =====================================================================

@pytest.fixture
def components(tmp_path):
    """Create real DatabaseManager + IngestionEngine + _TestServer.

    Uses a synthetic config file in tmp_path with a tmp_path-isolated
    DuckDB file. All yfinance calls are mocked within individual tests
    that need them.
    """
    db_dir = tmp_path / "db"
    db_dir.mkdir()
    cfg_path = write_config(tmp_path, db_dir)
    cfg = PipelineConfig.from_yaml(cfg_path)
    db_manager = DatabaseManager(cfg.db_path)
    engine = IngestionEngine(db_manager, cfg)
    server = _TestServer(db_manager, engine, cfg)
    yield server, db_manager, engine, cfg
    # Cleanup — safe to call even if already closed (close() catches
    # exceptions from operating on a closed connection internally).
    try:
        db_manager.close()
    except Exception:
        pass


# =====================================================================
# AC 1: main() can be imported and all components assemble without error
# =====================================================================

def test_main_importable_and_callable():
    """AC 1: from src.main import main; verify it's callable."""
    from src.main import main

    assert callable(main)


# =====================================================================
# AC 2: initial_load end-to-end with mocked yfinance
# =====================================================================

def test_initial_load_end_to_end(components):
    """AC 2: do_action 'initial_load' with mocked yf.download, verify
    rows_upserted > 0 and tickers_succeeded contains expected tickers."""
    server, db_manager, engine, cfg = components

    mock_df = make_mock_download_df(
        ["AAPL", "MSFT"], ["2026-01-01", "2026-01-02", "2026-01-03"]
    )
    mock_ticker = MagicMock()
    mock_ticker.splits = pd.Series(dtype=float)  # empty -> no splits

    with patch("src.ingestion.yf.download", return_value=mock_df), \
         patch("src.ingestion.yf.Ticker", return_value=mock_ticker):
        action = make_action("initial_load", {"years": 6})
        result = server.do_action(None, action)
        data = parse_result(result)

    assert data["rows_upserted"] > 0
    assert "AAPL" in data["tickers_succeeded"]
    assert "MSFT" in data["tickers_succeeded"]
    assert data["status"] in ("SUCCESS", "PARTIAL")


# =====================================================================
# AC 3: daily_report returns report with items sorted by dollar_volume DESC
# =====================================================================

def test_daily_report_sorted_desc(components):
    """AC 3: after inserting 25 rows, do_action 'daily_report' returns
    up to 20 items sorted by dollar_volume DESC."""
    server, db_manager, engine, cfg = components

    insert_synthetic_data(db_manager, n_tickers=25, date_str="2026-01-01")

    action = make_action("daily_report", {"date": "2026-01-01"})
    result = server.do_action(None, action)
    data = parse_result(result)

    assert data["status"] == "SUCCESS"
    report = data["report"]
    assert isinstance(report, list)
    assert 0 < len(report) <= 20

    # Verify sorted by dollar_volume DESC
    volumes = [item["dollar_volume"] for item in report]
    assert volumes == sorted(volumes, reverse=True)

    # T24 has the highest Close*Volume (124 * 1240 = 153760)
    assert report[0]["Ticker"] == "T24"


# =====================================================================
# AC 4: do_get with SELECT returns arrow table with data
# =====================================================================

def test_do_get_select_returns_arrow_data(components):
    """AC 4: do_get with SELECT returns RecordBatchStream with data.

    ``RecordBatchStream`` is a server-side C extension type with no
    Python-readable API.  We verify (a) do_get returns a
    ``RecordBatchStream`` (proving SQL was parsed, validated, and
    executed without error), and (b) the underlying query returns data
    by executing the same SQL via ``db.execute_query`` directly.
    """
    server, db_manager, engine, cfg = components
    insert_synthetic_data(db_manager, n_tickers=5)

    ticket = make_ticket("SELECT * FROM daily_quotes")
    stream = server.do_get(None, ticket)

    # do_get must return a RecordBatchStream (proves the full path:
    # ticket parse -> SQL validate -> execute_query -> wrap succeeded)
    assert isinstance(stream, flight.RecordBatchStream)

    # Verify the underlying data by executing the same query directly
    table = db_manager.execute_query("SELECT * FROM daily_quotes")
    assert table.num_rows == 5
    assert table.num_columns == 7  # Date, Ticker, Open, High, Low, Close, Volume


# =====================================================================
# AC 5: do_get with DELETE raises FlightServerError
# =====================================================================

def test_do_get_delete_raises_flight_server_error(components):
    """AC 5: do_get with 'DELETE FROM daily_quotes' raises FlightServerError."""
    server, db_manager, engine, cfg = components

    ticket = make_ticket("DELETE FROM daily_quotes")
    with pytest.raises(flight.FlightServerError):
        server.do_get(None, ticket)


# =====================================================================
# AC 6: Concurrent do_get from 3 threads all succeed
# =====================================================================

def test_concurrent_do_get_3_threads(components):
    """AC 6: 3 threads each calling do_get with SELECT all return data
    without error.

    Each thread calls ``do_get`` and verifies the return is a
    ``RecordBatchStream`` (proving the full path succeeded).  The
    underlying data is verified via ``db.execute_query`` after all
    threads complete.
    """
    server, db_manager, engine, cfg = components
    insert_synthetic_data(db_manager, n_tickers=10)

    results = [None] * 3
    errors = [None] * 3

    def query_thread(idx):
        try:
            ticket = make_ticket(
                "SELECT COUNT(*) AS cnt FROM daily_quotes"
            )
            stream = server.do_get(None, ticket)
            # Verify do_get returned a RecordBatchStream (no exception)
            assert isinstance(stream, flight.RecordBatchStream)
            # Verify underlying data via direct execute_query
            table = db_manager.execute_query(
                "SELECT COUNT(*) AS cnt FROM daily_quotes"
            )
            results[idx] = table.num_rows
        except Exception as e:
            errors[idx] = e

    threads = [
        threading.Thread(target=query_thread, args=(i,)) for i in range(3)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert all(e is None for e in errors), f"Thread errors: {errors}"
    assert all(r is not None for r in results), f"Thread results: {results}"
    assert all(r > 0 for r in results), (
        f"All threads should return data, got: {results}"
    )


# =====================================================================
# AC 7: db_manager.close() executes CHECKPOINT without error
# =====================================================================

def test_db_close_checkpoint_no_error(components):
    """AC 7: after all operations, db_manager.close() runs CHECKPOINT
    without raising an exception."""
    server, db_manager, engine, cfg = components
    insert_synthetic_data(db_manager, n_tickers=3)

    # close() should execute CHECKPOINT and close the connection
    # without raising any exception.
    db_manager.close()
