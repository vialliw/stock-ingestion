"""Unit tests for ``MarketDataFlightServer.do_put``.

Validates schema enforcement, excluded-ticker filtering, and the single
vectorized upsert of external Arrow record batches into ``daily_close``.
"""

from __future__ import annotations

import datetime as dt
from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.flight as flight
import pytest

from src.config import IngestionConfig, PipelineConfig
from src.db import DatabaseManager
from src.flight_server import MarketDataFlightServer, _DAILY_CLOSE_SCHEMA


def _daily_close_table(rows):
    """Build an Arrow table with the daily_close schema for do_put tests."""
    arrays = {
        name: pa.array([row[name] for row in rows], type=field.type)
        for name, field in zip(_DAILY_CLOSE_SCHEMA.names, _DAILY_CLOSE_SCHEMA)
    }
    return pa.table(arrays, schema=_DAILY_CLOSE_SCHEMA)


def _make_reader(table):
    """Return a MagicMock reader whose ``read_all`` returns ``table``."""
    reader = MagicMock()
    reader.read_all.return_value = table
    return reader


@pytest.fixture
def cfg():
    """PipelineConfig with ``AVB`` in the excluded-ticker list."""
    return PipelineConfig(ingestion=IngestionConfig(exclude_tickers=["AVB"]))


@pytest.fixture
def db(tmp_path):
    """Fresh DatabaseManager backed by a temporary DuckDB file."""
    return DatabaseManager(str(tmp_path / "test.duckdb"))


@pytest.fixture
def server(db, cfg):
    """A MarketDataFlightServer whose dependencies are wired to real objects.

    ``FlightServerBase.__init__`` is bypassed so the tests do not bind a port.
    """
    class _TestServer(MarketDataFlightServer):
        def __init__(self, db_mgr, cfg):
            self.db = db_mgr
            self.engine = None
            self.cfg = cfg

    return _TestServer(db, cfg)


def test_do_put_upserts_rows(server, db):
    """A valid do_put stream is upserted into daily_close."""
    table = _daily_close_table(
        [
            {
                "Date": dt.date(2022, 1, 1),
                "Ticker": "AAPL",
                "Open": 1.0,
                "High": 2.0,
                "Low": 0.5,
                "Close": 1.5,
                "Volume": 100.0,
            },
            {
                "Date": dt.date(2022, 1, 2),
                "Ticker": "MSFT",
                "Open": 10.0,
                "High": 11.0,
                "Low": 9.0,
                "Close": 10.5,
                "Volume": 200.0,
            },
        ]
    )
    reader = _make_reader(table)
    server.do_put(None, None, reader, MagicMock())

    result = db.execute_query(
        "SELECT * FROM daily_close ORDER BY Ticker"
    ).to_pandas()
    assert len(result) == 2
    assert set(result["Ticker"]) == {"AAPL", "MSFT"}
    reader.read_all.assert_called_once()


def test_do_put_concatenates_batches_and_upserts(server, db):
    """read_all concatenates all incoming batches into one upsert."""
    rows = [
        {
            "Date": dt.date(2022, 1, i),
            "Ticker": "TICK",
            "Open": float(i),
            "High": float(i) + 1.0,
            "Low": float(i) - 1.0,
            "Close": float(i) + 0.5,
            "Volume": float(i) * 10.0,
        }
        for i in range(1, 4)
    ]
    table = _daily_close_table(rows)
    reader = _make_reader(table)
    server.do_put(None, None, reader, MagicMock())

    count = db.execute_query(
        "SELECT COUNT(*) FROM daily_close WHERE Ticker = 'TICK'"
    ).to_pandas().iloc[0, 0]
    assert count == 3


def test_do_put_updates_existing_rows(server, db):
    """Duplicate (Date, Ticker) rows update existing values via MERGE."""
    first = _daily_close_table(
        [
            {
                "Date": dt.date(2022, 1, 1),
                "Ticker": "AAPL",
                "Open": 1.0,
                "High": 2.0,
                "Low": 0.5,
                "Close": 1.5,
                "Volume": 100.0,
            }
        ]
    )
    server.do_put(None, None, _make_reader(first), MagicMock())

    second = _daily_close_table(
        [
            {
                "Date": dt.date(2022, 1, 1),
                "Ticker": "AAPL",
                "Open": 2.0,
                "High": 3.0,
                "Low": 1.5,
                "Close": 2.5,
                "Volume": 200.0,
            }
        ]
    )
    server.do_put(None, None, _make_reader(second), MagicMock())

    close = db.execute_query(
        "SELECT Close FROM daily_close WHERE Ticker = 'AAPL'"
    ).to_pandas().iloc[0, 0]
    assert close == 2.5


def test_do_put_rejects_missing_column(server):
    """A schema missing a required column raises FlightServerError."""
    bad_schema = pa.schema(
        [
            pa.field("Date", pa.date32()),
            pa.field("Ticker", pa.string()),
            pa.field("Open", pa.float64()),
            pa.field("High", pa.float64()),
            pa.field("Low", pa.float64()),
            pa.field("Close", pa.float64()),
        ]
    )
    bad_table = pa.table(
        {
            "Date": pa.array([dt.date(2022, 1, 1)], type=pa.date32()),
            "Ticker": pa.array(["AAPL"], type=pa.string()),
            "Open": pa.array([1.0], type=pa.float64()),
            "High": pa.array([2.0], type=pa.float64()),
            "Low": pa.array([0.5], type=pa.float64()),
            "Close": pa.array([1.5], type=pa.float64()),
        },
        schema=bad_schema,
    )
    with pytest.raises(flight.FlightServerError):
        server.do_put(None, None, _make_reader(bad_table), MagicMock())


def test_do_put_rejects_wrong_type(server):
    """A schema with a wrong column type raises FlightServerError."""
    bad_schema = pa.schema(
        [
            pa.field("Date", pa.date32()),
            pa.field("Ticker", pa.string()),
            pa.field("Open", pa.float64()),
            pa.field("High", pa.float64()),
            pa.field("Low", pa.float64()),
            pa.field("Close", pa.float64()),
            pa.field("Volume", pa.int64()),
        ]
    )
    bad_table = pa.table(
        {
            "Date": pa.array([dt.date(2022, 1, 1)], type=pa.date32()),
            "Ticker": pa.array(["AAPL"], type=pa.string()),
            "Open": pa.array([1.0], type=pa.float64()),
            "High": pa.array([2.0], type=pa.float64()),
            "Low": pa.array([0.5], type=pa.float64()),
            "Close": pa.array([1.5], type=pa.float64()),
            "Volume": pa.array([100], type=pa.int64()),
        },
        schema=bad_schema,
    )
    with pytest.raises(flight.FlightServerError):
        server.do_put(None, None, _make_reader(bad_table), MagicMock())


def test_do_put_filters_excluded_tickers(server, db, caplog):
    """Rows for tickers in exclude_tickers are dropped with a warning."""
    table = _daily_close_table(
        [
            {
                "Date": dt.date(2022, 1, 1),
                "Ticker": "AAPL",
                "Open": 1.0,
                "High": 2.0,
                "Low": 0.5,
                "Close": 1.5,
                "Volume": 100.0,
            },
            {
                "Date": dt.date(2022, 1, 1),
                "Ticker": "AVB",
                "Open": 10.0,
                "High": 11.0,
                "Low": 9.0,
                "Close": 10.5,
                "Volume": 200.0,
            },
        ]
    )
    with caplog.at_level("WARNING"):
        server.do_put(None, None, _make_reader(table), MagicMock())

    result = db.execute_query("SELECT * FROM daily_close").to_pandas()
    assert len(result) == 1
    assert result.iloc[0]["Ticker"] == "AAPL"
    assert "do_put filtered 1 row(s)" in caplog.text
    assert "AVB" in caplog.text


def test_do_put_all_excluded_upserts_zero_rows(server, db):
    """If every row is excluded, upsert_dataframe receives an empty frame."""
    table = _daily_close_table(
        [
            {
                "Date": dt.date(2022, 1, 1),
                "Ticker": "AVB",
                "Open": 10.0,
                "High": 11.0,
                "Low": 9.0,
                "Close": 10.5,
                "Volume": 200.0,
            }
        ]
    )
    server.do_put(None, None, _make_reader(table), MagicMock())
    count = db.execute_query("SELECT COUNT(*) FROM daily_close").to_pandas().iloc[0, 0]
    assert count == 0


def test_do_put_empty_stream_upserts_zero_rows(server, db):
    """An empty Arrow table upserts zero rows without error."""
    empty = _daily_close_table([])
    server.do_put(None, None, _make_reader(empty), MagicMock())
    count = db.execute_query("SELECT COUNT(*) FROM daily_close").to_pandas().iloc[0, 0]
    assert count == 0
