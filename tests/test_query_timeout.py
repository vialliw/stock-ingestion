"""Tests for the do_get query timeout (F7-T3)."""

from __future__ import annotations

import json
import time
from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.flight as flight
import pytest

from src.config import PipelineConfig
from src import flight_server as module


def make_server(db_mgr, cfg):
    """Build a test server that skips the port-binding base __init__."""
    class _TestServer(module.MarketDataFlightServer):
        def __init__(self, db_mgr, cfg):
            self.db = db_mgr
            self.engine = None
            self.cfg = cfg

    return _TestServer(db_mgr, cfg)


def make_ticket(sql: str) -> flight.Ticket:
    return flight.Ticket(json.dumps({"sql": sql}).encode("utf-8"))


def test_default_query_timeout_is_120():
    """ServerConfig.query_timeout_seconds defaults to 120 and loads from YAML."""
    cfg = PipelineConfig()
    assert cfg.server.query_timeout_seconds == 120

    cfg = PipelineConfig.from_yaml("src/config.yaml")
    assert cfg.server.query_timeout_seconds == 120


def test_do_get_query_under_timeout_succeeds():
    """A query that finishes before the timeout returns a RecordBatchStream."""
    db = MagicMock()
    arrow_table = pa.table({"Ticker": ["AAPL"]})
    db.execute_query.return_value = arrow_table

    cfg = MagicMock()
    cfg.server.query_timeout_seconds = 1

    server = make_server(db, cfg)
    stream = server.do_get(None, make_ticket("SELECT * FROM daily_close"))

    assert isinstance(stream, flight.RecordBatchStream)
    db.execute_query.assert_called_once_with("SELECT * FROM daily_close")


def test_do_get_timeout_fires():
    """A slow query raises FlightServerError with a timeout message."""
    db = MagicMock()
    db.execute_query.side_effect = lambda sql: time.sleep(0.3)

    cfg = MagicMock()
    cfg.server.query_timeout_seconds = 0.05

    server = make_server(db, cfg)

    with pytest.raises(flight.FlightServerError) as exc_info:
        server.do_get(None, make_ticket("SELECT * FROM daily_close"))

    assert "timed out" in str(exc_info.value).lower()


def test_do_get_does_not_inject_limit():
    """do_get passes the original SQL through without appending LIMIT."""
    db = MagicMock()
    db.execute_query.return_value = pa.table({"a": [1]})

    cfg = MagicMock()
    cfg.server.query_timeout_seconds = 1

    server = make_server(db, cfg)
    sql = "SELECT * FROM daily_close"
    server.do_get(None, make_ticket(sql))

    called_sql = db.execute_query.call_args[0][0]
    assert called_sql == sql
    assert "LIMIT" not in called_sql.upper()
