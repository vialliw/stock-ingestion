"""INC-T6 regression tests: enforced (ticker, date) row exclusions.

Covers the INC-T6 acceptance criteria:
  a. config parse + validation of ``ingestion.exclude_rows``
     (good / malformed entries, YAML seed pairs);
  b. ingestion filter — downloaded rows matching (Ticker, Date) are
     dropped on the mocked-download path, all others kept, and a
     RUNTIME EVENT is logged with the dropped count;
  c. ``DatabaseManager.delete_daily_rows`` against a tmp_path DuckDB
     (parameterized delete, count, empty list no-op);
  d. ``do_action('delete_daily_rows')`` routing, payload validation and
     empty/malformed-payload behaviour (mocked engine/db per
     test_flight_server.py conventions).

Executor implementation only — INC-T6's output files are all in
``src/`` (no challenger twin for this task).  All yfinance calls are
mocked; no real API calls or network access.
"""

from __future__ import annotations

import importlib
import json
import logging
from datetime import date
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pyarrow.flight as flight
import pytest

from src.config import PipelineConfig
from src.db import DatabaseManager
from src.ingestion import IngestionEngine


# =====================================================================
# a. Config: ingestion.exclude_rows parsing + validation
# =====================================================================

class TestConfigExcludeRows:
    """``PipelineConfig.ingestion.exclude_rows`` normalization + rejection."""

    def test_default_is_empty_list(self):
        assert PipelineConfig().ingestion.exclude_rows == []

    def test_none_becomes_empty_list(self):
        cfg = PipelineConfig(ingestion={"exclude_rows": None})
        assert cfg.ingestion.exclude_rows == []

    def test_parses_and_normalizes_ticker(self):
        cfg = PipelineConfig(
            ingestion={
                "exclude_rows": [{"ticker": " qrvo ", "date": "2026-10-05"}]
            }
        )
        assert cfg.ingestion.exclude_rows == [
            {"ticker": "QRVO", "date": "2026-10-05"}
        ]

    def test_accepts_yaml_parsed_date_object(self):
        """An unquoted ``2026-10-05`` in YAML parses to datetime.date —
        it must normalize to the ISO string, not be rejected."""
        cfg = PipelineConfig(
            ingestion={
                "exclude_rows": [
                    {"ticker": "WBD", "date": date(2026, 10, 5)}
                ]
            }
        )
        assert cfg.ingestion.exclude_rows == [
            {"ticker": "WBD", "date": "2026-10-05"}
        ]

    def test_deduplicates_pairs_preserving_order(self):
        cfg = PipelineConfig(
            ingestion={
                "exclude_rows": [
                    {"ticker": "QRVO", "date": "2026-10-05"},
                    {"ticker": "WBD", "date": "2026-10-05"},
                    {"ticker": " qrvo ", "date": "2026-10-05"},
                ]
            }
        )
        assert cfg.ingestion.exclude_rows == [
            {"ticker": "QRVO", "date": "2026-10-05"},
            {"ticker": "WBD", "date": "2026-10-05"},
        ]

    @pytest.mark.parametrize(
        "bad",
        [
            "QRVO",                                   # scalar, not a list
            {"ticker": "QRVO", "date": "2026-10-05"},  # dict, not a list
            ["QRVO|2026-10-05"],                      # non-dict entry
            [42],                                     # non-dict entry
            [{"ticker": "QRVO"}],                     # missing date
            [{"date": "2026-10-05"}],                 # missing ticker
            [{"ticker": "", "date": "2026-10-05"}],   # empty ticker
            [{"ticker": "   ", "date": "2026-10-05"}],  # blank ticker
            [{"ticker": 5, "date": "2026-10-05"}],    # non-str ticker
            [{"ticker": "QRVO", "date": "10/05/2026"}],  # bad format
            [{"ticker": "QRVO", "date": "2026-13-01"}],  # impossible month
            [{"ticker": "QRVO", "date": "2026-02-30"}],  # impossible day
            [{"ticker": "QRVO", "date": 20261005}],   # non-str date
            # unknown key -> strict shape (typos must not silently no-op)
            [{"ticker": "QRVO", "date": "2026-10-05", "reason": "x"}],
        ],
    )
    def test_malformed_entries_rejected(self, bad):
        with pytest.raises(Exception):
            PipelineConfig(ingestion={"exclude_rows": bad})

    def test_seeded_config_yaml_pairs(self):
        """The real src/config.yaml seeds the two known vendor artifact
        rows (QRVO + WBD, both 2026-10-05)."""
        cfg = PipelineConfig.from_yaml("src/config.yaml")
        pairs = {
            (e["ticker"], e["date"]) for e in cfg.ingestion.exclude_rows
        }
        assert ("QRVO", "2026-10-05") in pairs
        assert ("WBD", "2026-10-05") in pairs


# =====================================================================
# b. Ingestion: exclude_rows filter on the mocked download path
# =====================================================================

def _multiindex_download_df(tickers, dates):
    """A mock ``yf.download`` frame with MultiIndex (ticker, field) cols."""
    cols = pd.MultiIndex.from_product(
        [tickers, ["Open", "High", "Low", "Close", "Volume"]]
    )
    idx = pd.to_datetime(dates)
    idx.name = "Date"
    data = np.random.uniform(90, 110, size=(len(idx), len(cols)))
    df = pd.DataFrame(data, index=idx, columns=cols)
    df.columns.name = "Ticker"
    return df


@pytest.fixture
def db(tmp_path):
    """tmp_path-isolated real DatabaseManager."""
    mgr = DatabaseManager(str(tmp_path / "inc_t6_ingestion.duckdb"))
    try:
        yield mgr
    finally:
        mgr.close()


@pytest.fixture
def cfg():
    c = PipelineConfig()
    c.ingestion.tickers = ["AAPL", "WBD"]
    return c


@pytest.fixture
def engine(db, cfg):
    return IngestionEngine(db, cfg)


class TestIngestionExcludeRows:
    """The (ticker, date) filter drops matching rows before upsert."""

    def test_matching_row_dropped_others_kept(self, engine, cfg, caplog):
        cfg.ingestion.exclude_rows = [
            {"ticker": "WBD", "date": "2026-10-05"}
        ]
        dates = ["2026-10-03", "2026-10-05", "2026-10-06"]
        mock_df = _multiindex_download_df(["AAPL", "WBD"], dates)
        mock_ticker = MagicMock()
        mock_ticker.splits = pd.Series([], dtype=float, name="splits")
        with patch("src.ingestion.yf.download", return_value=mock_df), \
             patch("src.ingestion.yf.Ticker", return_value=mock_ticker), \
             caplog.at_level(logging.WARNING, logger="src.ingestion"):
            df, succeeded, failed, errors = engine.fetch_and_clean(
                ["AAPL", "WBD"], "2026-10-03", "2026-10-06"
            )

        # 6 raw rows -> 5 kept (only WBD 2026-10-05 dropped).
        assert len(df) == 5
        kept = set(zip(df["Ticker"], df["Date"].astype(str)))
        assert ("WBD", "2026-10-05") not in kept
        assert ("WBD", "2026-10-03") in kept
        assert ("WBD", "2026-10-06") in kept
        assert ("AAPL", "2026-10-05") in kept
        # The ticker itself is still a success — only the row is filtered.
        assert "WBD" in succeeded
        assert failed == []
        # RUNTIME EVENT logged with the dropped count.
        assert any(
            "RUNTIME EVENT" in r.getMessage()
            and "dropped 1 row" in r.getMessage()
            for r in caplog.records
        )

    def test_no_exclusions_keeps_everything(self, engine, caplog):
        dates = ["2026-10-05", "2026-10-06"]
        mock_df = _multiindex_download_df(["AAPL", "WBD"], dates)
        mock_ticker = MagicMock()
        mock_ticker.splits = pd.Series([], dtype=float, name="splits")
        with patch("src.ingestion.yf.download", return_value=mock_df), \
             patch("src.ingestion.yf.Ticker", return_value=mock_ticker), \
             caplog.at_level(logging.WARNING, logger="src.ingestion"):
            df, succeeded, failed, errors = engine.fetch_and_clean(
                ["AAPL", "WBD"], "2026-10-05", "2026-10-06"
            )
        assert len(df) == 4
        assert not any(
            "exclude_rows dropped" in r.getMessage() for r in caplog.records
        )

    def test_fully_excluded_ticker_contributes_no_rows(
        self, engine, cfg
    ):
        """A ticker whose every downloaded row is excluded still
        succeeds — it just contributes zero rows to the upsert."""
        cfg.ingestion.exclude_rows = [
            {"ticker": "WBD", "date": "2026-10-05"},
            {"ticker": "WBD", "date": "2026-10-06"},
        ]
        dates = ["2026-10-05", "2026-10-06"]
        mock_df = _multiindex_download_df(["AAPL", "WBD"], dates)
        mock_ticker = MagicMock()
        mock_ticker.splits = pd.Series([], dtype=float, name="splits")
        with patch("src.ingestion.yf.download", return_value=mock_df), \
             patch("src.ingestion.yf.Ticker", return_value=mock_ticker):
            df, succeeded, failed, errors = engine.fetch_and_clean(
                ["AAPL", "WBD"], "2026-10-05", "2026-10-06"
            )
        assert set(df["Ticker"]) == {"AAPL"}
        assert len(df) == 2
        assert "WBD" in succeeded

    def test_apply_exclude_rows_tolerates_string_dates(self, engine, cfg):
        """The Date column may be a string instead of datetime.date —
        the match normalizes both sides to YYYY-MM-DD."""
        cfg.ingestion.exclude_rows = [
            {"ticker": "QRVO", "date": "2026-10-05"}
        ]
        df = pd.DataFrame(
            {
                "Date": ["2026-10-05", "2026-10-06"],
                "Ticker": ["QRVO", "QRVO"],
                "Open": [1.0, 1.0],
                "High": [1.0, 1.0],
                "Low": [1.0, 1.0],
                "Close": [1.0, 1.0],
                "Volume": [0.0, 1.0],
            }
        )
        out = engine._apply_exclude_rows(df)
        assert len(out) == 1
        assert out["Date"].iloc[0] == "2026-10-06"

    def test_apply_exclude_rows_noop_on_empty_and_missing_cfg(
        self, engine, cfg
    ):
        df = pd.DataFrame(
            {
                "Date": [date(2026, 10, 5)],
                "Ticker": ["QRVO"],
                "Open": [1.0],
                "High": [1.0],
                "Low": [1.0],
                "Close": [1.0],
                "Volume": [0.0],
            }
        )
        # Empty config list -> unchanged frame object.
        cfg.ingestion.exclude_rows = []
        assert engine._apply_exclude_rows(df) is df
        # A cfg without the attribute (MagicMock) -> unchanged.
        mock_engine = IngestionEngine(MagicMock(), MagicMock())
        out = mock_engine._apply_exclude_rows(df)
        assert len(out) == 1


# =====================================================================
# c. DatabaseManager.delete_daily_rows against a tmp_path DuckDB
# =====================================================================

def _seed_daily_close(db, rows):
    """rows: list of (date_str, ticker) — deterministic OHLCV."""
    df = pd.DataFrame(
        {
            "Date": pd.to_datetime([r[0] for r in rows]),
            "Ticker": [r[1] for r in rows],
            "Open": [100.0] * len(rows),
            "High": [101.0] * len(rows),
            "Low": [99.0] * len(rows),
            "Close": [100.5] * len(rows),
            "Volume": [1000.0] * len(rows),
        }
    )
    return db.upsert_dataframe(df)


class TestDeleteDailyRows:
    """Parameterized (Ticker, Date) deletes on a real DuckDB."""

    def test_deletes_only_matching_pairs(self, db):
        _seed_daily_close(
            db,
            [
                ("2026-10-05", "QRVO"),
                ("2026-10-05", "WBD"),
                ("2026-10-06", "WBD"),
                ("2026-10-05", "AAPL"),
            ],
        )
        deleted = db.delete_daily_rows(
            [("QRVO", "2026-10-05"), ("WBD", "2026-10-05")]
        )
        assert deleted == 2
        remaining = db.execute_query(
            "SELECT Ticker, CAST(Date AS VARCHAR) AS d "
            "FROM daily_close ORDER BY Ticker, d"
        ).to_pandas()
        kept = set(zip(remaining["Ticker"], remaining["d"]))
        assert kept == {("AAPL", "2026-10-05"), ("WBD", "2026-10-06")}

    def test_empty_pairs_is_noop(self, db):
        _seed_daily_close(db, [("2026-10-05", "QRVO")])
        assert db.delete_daily_rows([]) == 0
        n = db.execute_query(
            "SELECT COUNT(*) AS n FROM daily_close"
        ).to_pandas()["n"].iloc[0]
        assert n == 1

    def test_nonexistent_pairs_delete_zero(self, db):
        _seed_daily_close(db, [("2026-10-05", "QRVO")])
        assert db.delete_daily_rows([("NOSUCH", "2026-10-05")]) == 0
        assert db.delete_daily_rows([("QRVO", "2026-10-06")]) == 0

    def test_delete_is_idempotent(self, db):
        _seed_daily_close(db, [("2026-10-05", "QRVO")])
        assert db.delete_daily_rows([("QRVO", "2026-10-05")]) == 1
        assert db.delete_daily_rows([("QRVO", "2026-10-05")]) == 0


# =====================================================================
# d. do_action("delete_daily_rows") — routing + payload validation
# =====================================================================

def _make_server():
    """A ``MarketDataFlightServer`` with mocked deps (no gRPC bind).

    Same pattern as test_flight_server.py::make_server — subclass that
    skips the port-binding base __init__.
    """
    module = importlib.import_module("src.flight_server")
    db_mgr = MagicMock()
    ingest_engine = MagicMock()
    cfg = MagicMock()

    class _TestServer(module.MarketDataFlightServer):
        def __init__(self, db_mgr, ingest_engine, cfg):
            self.db = db_mgr
            self.engine = ingest_engine
            self.cfg = cfg

    return _TestServer(db_mgr, ingest_engine, cfg), db_mgr


def _make_action(action_type, payload=None):
    action = MagicMock()
    action.type = action_type
    if payload is not None:
        action.body = MagicMock()
        action.body.to_pybytes.return_value = json.dumps(payload).encode(
            "utf-8"
        )
    else:
        action.body = None
    return action


def _parse_result(result):
    return json.loads(result.body.to_pybytes().decode("utf-8"))


class TestDeleteDailyRowsAction:
    """do_action routing, validation and result shape."""

    def test_success_returns_deleted_and_requested(self):
        server, db_mgr = _make_server()
        db_mgr.delete_daily_rows.return_value = 2
        action = _make_action(
            "delete_daily_rows",
            {
                "rows": [
                    {"ticker": "QRVO", "date": "2026-10-05"},
                    {"ticker": "wbd", "date": "2026-10-05"},
                ]
            },
        )
        data = _parse_result(server.do_action(MagicMock(), action))
        assert data["status"] == "SUCCESS"
        assert data["deleted"] == 2
        assert data["requested"] == 2
        # Tickers are normalized to uppercase before the delete.
        db_mgr.delete_daily_rows.assert_called_once_with(
            [("QRVO", "2026-10-05"), ("WBD", "2026-10-05")]
        )

    def test_zero_deleted_is_still_success(self):
        server, db_mgr = _make_server()
        db_mgr.delete_daily_rows.return_value = 0
        action = _make_action(
            "delete_daily_rows",
            {"rows": [{"ticker": "QRVO", "date": "2026-10-05"}]},
        )
        data = _parse_result(server.do_action(MagicMock(), action))
        assert data["status"] == "SUCCESS"
        assert data["deleted"] == 0
        assert data["requested"] == 1

    @pytest.mark.parametrize(
        "payload",
        [
            [{"rows": []}],                       # non-dict payload (list)
            {},                                   # missing 'rows'
            {"rows": "QRVO"},                     # rows not a list
            {"rows": []},                         # empty rows
            {"rows": ["QRVO"]},                   # non-dict entry
            {"rows": [{"ticker": "QRVO"}]},       # missing date
            {"rows": [{"date": "2026-10-05"}]},   # missing ticker
            {"rows": [{"ticker": "", "date": "2026-10-05"}]},
            {"rows": [{"ticker": 5, "date": "2026-10-05"}]},
            {"rows": [{"ticker": "QRVO", "date": "10/05/2026"}]},
            {"rows": [{"ticker": "QRVO", "date": "2026-13-01"}]},
            {"rows": [{"ticker": "QRVO", "date": "2026-02-30"}]},
            {"rows": [{"ticker": "QRVO", "date": 20261005}]},
        ],
    )
    def test_malformed_payloads_raise_flight_error(self, payload):
        server, db_mgr = _make_server()
        action = _make_action("delete_daily_rows", payload)
        with pytest.raises(flight.FlightServerError):
            server.do_action(MagicMock(), action)
        db_mgr.delete_daily_rows.assert_not_called()

    def test_no_body_raises_flight_error(self):
        server, db_mgr = _make_server()
        action = _make_action("delete_daily_rows")  # body=None -> {}
        with pytest.raises(flight.FlightServerError):
            server.do_action(MagicMock(), action)
        db_mgr.delete_daily_rows.assert_not_called()

    def test_db_failure_returns_failed_json_not_transport_error(self):
        server, db_mgr = _make_server()
        db_mgr.delete_daily_rows.side_effect = RuntimeError("db locked")
        action = _make_action(
            "delete_daily_rows",
            {"rows": [{"ticker": "QRVO", "date": "2026-10-05"}]},
        )
        data = _parse_result(server.do_action(MagicMock(), action))
        assert data["status"] == "FAILED"
        assert "db locked" in data["error"]
        assert data["requested"] == 1
