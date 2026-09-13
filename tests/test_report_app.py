"""Tests for ``scripts/report_app.py`` — the Streamlit report app.

Only the non-UI surface is exercised: the SQL builders, the
Flight-first / DuckDB-fallback fetch logic, and the in-memory xlsx
serializer.  No Streamlit server is started; ``streamlit`` is never
imported by these tests (report_app imports it lazily inside the UI
functions only).
"""

from __future__ import annotations

import io
import json
import sys
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import duckdb
import openpyxl
import pandas as pd
import pyarrow as pa
import pyarrow.flight as flight
import pytest

# Add scripts dir to path so report_app can be imported (same pattern
# as tests/test_scheduler.py).
SCRIPTS_DIR = Path(__file__).parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

import report_app

from src.config import PipelineConfig


def _config(tmp_path) -> PipelineConfig:
    """Config whose DB lives under ``tmp_path`` (nothing touches the
    real C:\\Intel\\parquet)."""
    return PipelineConfig(
        database={
            "duckdb_dir": str(tmp_path),
            "duckdb_filename": "test.duckdb",
        },
        logging={"log_file": str(tmp_path / "logs" / "run.log")},
    )


def _seed_daily_close(db_path: str, rows: list) -> None:
    """Create a fresh DuckDB file with a ``daily_close`` table holding
    ``rows`` of (date_str, ticker, open, high, low, close, volume)."""
    conn = duckdb.connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE daily_close (
                Date DATE NOT NULL,
                Ticker VARCHAR NOT NULL,
                Open DOUBLE,
                High DOUBLE,
                Low DOUBLE,
                "Close" DOUBLE,
                Volume DOUBLE,
                PRIMARY KEY (Date, Ticker)
            )
            """
        )
        conn.executemany(
            "INSERT INTO daily_close VALUES (?, ?, ?, ?, ?, ?, ?)", rows
        )
    finally:
        conn.close()


def _mock_flight(table=None):
    """Stand-in for the ``pyarrow.flight`` module inside ``report_app``
    (same pattern as ``_mock_flight_for_report`` in test_scheduler)."""
    mock_flight = MagicMock()
    mock_flight.Ticket = flight.Ticket
    mock_flight.FlightCallOptions = flight.FlightCallOptions
    mock_flight.FlightError = flight.FlightError
    mock_flight.FlightServerError = flight.FlightServerError
    mock_flight.FlightTimedOutError = flight.FlightTimedOutError
    mock_flight.FlightUnavailableError = flight.FlightUnavailableError
    if table is not None:
        reader = MagicMock()
        reader.read_all.return_value = table
        mock_flight.FlightClient.return_value.do_get.return_value = reader
    return mock_flight


class TestBuildTop25Sql:
    """Tab A builder — top-N by latest dollar volume + edge metrics."""

    def test_defaults_injected(self):
        sql = report_app.build_top25_sql()
        assert "FROM daily_close" in sql
        assert "period_rank <= 30" in sql
        assert "LIMIT 25" in sql
        assert ">= 2.5 THEN 'Strong Acceleration'" in sql
        assert ">= 1.8 THEN 'Acceleration'" in sql
        assert "<= 0.6 THEN 'Strong Deceleration'" in sql
        assert "<= 0.8 THEN 'Deceleration'" in sql
        assert "ELSE 'Normal'" in sql
        assert "ORDER BY c.latest_trade_money DESC" in sql
        assert "latest_dollar_volume" in sql
        assert "pct_of_total_avg_dollar" in sql
        # No regime filter by default.
        assert "volume_regime IN" not in sql

    def test_params_injected(self):
        sql = report_app.build_top25_sql(
            lookback=20,
            limit=10,
            strong_accel=3.0,
            accel=2.0,
            decel=0.7,
            strong_decel=0.5,
        )
        assert "period_rank <= 20" in sql
        assert "LIMIT 10" in sql
        assert ">= 3.0 THEN 'Strong Acceleration'" in sql
        assert ">= 2.0 THEN 'Acceleration'" in sql
        assert "<= 0.5 THEN 'Strong Deceleration'" in sql
        assert "<= 0.7 THEN 'Deceleration'" in sql

    def test_regime_filter_injected(self):
        sql = report_app.build_top25_sql(
            regimes=["Acceleration", "Strong Acceleration"]
        )
        assert (
            "WHERE c.volume_regime IN ('Acceleration', 'Strong Acceleration')"
            in sql
        )

    def test_empty_regime_selection_means_no_filter(self):
        sql = report_app.build_top25_sql(regimes=[])
        assert "volume_regime IN" not in sql

    def test_unknown_regime_rejected(self):
        # Values come from a fixed whitelist — anything else (e.g. a
        # hand-crafted string trying to break out of the literal) must
        # raise before reaching the SQL text.
        with pytest.raises(ValueError):
            report_app.build_top25_sql(
                regimes=["Acceleration'); DROP TABLE daily_close; --"]
            )

    def test_non_numeric_params_rejected(self):
        with pytest.raises(TypeError):
            report_app.build_top25_sql(lookback="30")
        with pytest.raises(TypeError):
            report_app.build_top25_sql(limit=True)
        with pytest.raises(TypeError):
            report_app.build_top25_sql(accel="1.8")

    def test_generated_sql_executes_on_duckdb(self, tmp_path):
        """The built query is valid SQL: runs against a seeded
        daily_close table and returns the expected shape/ordering."""
        db_path = str(tmp_path / "test.duckdb")
        rows = []
        # 40 distinct periods for two tickers; T2 has higher latest
        # volume.
        for i in range(40):
            day = (date(2026, 7, 1) + timedelta(days=i)).isoformat()
            # justified: seeding distinct synthetic rows per period —
            # deterministic test fixture, not data transformation.
            for ticker, vol in (("T1", 100.0), ("T2", 200.0)):
                rows.append(
                    (day, ticker, 1.0, 1.0, 1.0, 1.0, vol + i)
                )
        _seed_daily_close(db_path, rows)

        conn = duckdb.connect(db_path, read_only=True)
        try:
            df = conn.execute(
                report_app.build_top25_sql(limit=5)
            ).df()
        finally:
            conn.close()

        assert len(df) == 2
        assert "latest_dollar_volume" in df.columns
        assert "rvol" in df.columns
        assert "volume_regime" in df.columns
        # Ranked by latest dollar volume: T2's last row is larger.
        assert list(df["Ticker"]) == ["T2", "T1"]


class TestBuildAccelSql:
    """Tab B builder — acceleration candidates within top-K liquid."""

    def test_defaults_injected(self):
        sql = report_app.build_accel_sql()
        assert "FROM daily_close" in sql
        assert "period_rank <= 30" in sql
        assert "dollar_rank <= 50" in sql
        assert ">= 2.5 THEN 'Strong Acceleration'" in sql
        assert ">= 1.8 THEN 'Acceleration'" in sql
        assert "ELSE 'Other'" in sql
        assert (
            "volume_regime IN ('Acceleration', 'Strong Acceleration')"
            in sql
        )
        assert "ORDER BY rvol DESC, latest_trade_money DESC" in sql
        assert "ranked_by_dollar" in sql

    def test_params_injected(self):
        sql = report_app.build_accel_sql(
            lookback=15, accel=2.0, strong_accel=3.0, top_k=10
        )
        assert "period_rank <= 15" in sql
        assert ">= 3.0 THEN 'Strong Acceleration'" in sql
        assert ">= 2.0 THEN 'Acceleration'" in sql
        assert "dollar_rank <= 10" in sql

    def test_non_numeric_params_rejected(self):
        with pytest.raises(TypeError):
            report_app.build_accel_sql(lookback="30")
        with pytest.raises(TypeError):
            report_app.build_accel_sql(top_k="50")
        with pytest.raises(TypeError):
            report_app.build_accel_sql(accel=None)

    def test_generated_sql_executes_on_duckdb(self, tmp_path):
        """Tab B SQL filters to accelerating names among the most
        liquid and orders by rvol descending."""
        db_path = str(tmp_path / "test.duckdb")
        rows = []
        # 35 distinct periods: a huge latest-volume spike for SPIKE;
        # QUIET stays flat (rvol ~1 -> 'Other', filtered out).
        for i in range(35):
            day = (date(2026, 8, 1) + timedelta(days=i)).isoformat()
            spike_vol = 100.0 if i < 34 else 1000.0
            # justified: deterministic synthetic fixture rows.
            for ticker, vol in (("SPIKE", spike_vol), ("QUIET", 500.0)):
                rows.append((day, ticker, 1.0, 1.0, 1.0, 1.0, vol))
        _seed_daily_close(db_path, rows)

        conn = duckdb.connect(db_path, read_only=True)
        try:
            df = conn.execute(
                report_app.build_accel_sql(lookback=30, top_k=50)
            ).df()
        finally:
            conn.close()

        assert list(df["Ticker"]) == ["SPIKE"]
        assert df["volume_regime"].iloc[0] in (
            "Acceleration",
            "Strong Acceleration",
        )
        assert df["rvol"].iloc[0] >= 1.8


class TestFetchDataframe:
    """Flight-first / direct-DuckDB fallback logic."""

    def test_flight_success_returns_dataframe(self, tmp_path):
        config = _config(tmp_path)
        table = pa.table(
            {"Ticker": ["MSFT"], "rvol": [2.1], "latest_dollar_volume": [1.0]}
        )
        mock_flight = _mock_flight(table)
        mock_client = mock_flight.FlightClient.return_value

        with patch.object(report_app, "flight", mock_flight):
            df = report_app.fetch_dataframe("SELECT 1", config)

        assert list(df["Ticker"]) == ["MSFT"]
        mock_client.wait_for_available.assert_called_once()
        mock_client.do_get.assert_called_once()
        ticket = mock_client.do_get.call_args.args[0]
        payload = json.loads(ticket.ticket.decode("utf-8"))
        assert payload["sql"] == "SELECT 1"
        # The call is bounded — FlightCallOptions(timeout=120).
        options = mock_client.do_get.call_args.kwargs["options"]
        assert isinstance(options, flight.FlightCallOptions)

    def test_do_get_carries_timeout_call_options(self, tmp_path):
        """A real FlightCallOptions built with ``timeout=120`` is
        passed so a wedged-but-connected server cannot hang the app."""
        config = _config(tmp_path)
        table = pa.table({"Ticker": ["MSFT"]})
        mock_flight = _mock_flight(table)
        # Wrap the real class so constructor kwargs are recorded while
        # a genuine FlightCallOptions is still produced (the pyarrow
        # object exposes no readable .timeout attribute).
        mock_flight.FlightCallOptions = MagicMock(
            wraps=flight.FlightCallOptions
        )
        mock_client = mock_flight.FlightClient.return_value

        with patch.object(report_app, "flight", mock_flight):
            df = report_app.fetch_dataframe("SELECT 1", config)

        assert df is not None
        mock_flight.FlightCallOptions.assert_called_once_with(timeout=120)
        options = mock_client.do_get.call_args.kwargs["options"]
        assert isinstance(options, flight.FlightCallOptions)

    def test_flight_down_falls_back_to_direct_readonly_db(self, tmp_path):
        config = _config(tmp_path)
        db_path = config.db_path
        _seed_daily_close(
            db_path, [("2026-09-01", "AAA", 1.0, 1.0, 1.0, 1.0, 10.0)]
        )

        mock_flight = _mock_flight()
        mock_flight.FlightClient.return_value.wait_for_available.side_effect = (
            flight.FlightUnavailableError("connection refused")
        )
        mock_duckdb = MagicMock()
        mock_duckdb.connect = MagicMock(wraps=duckdb.connect)

        with patch.object(
            report_app, "flight", mock_flight
        ), patch.object(report_app, "duckdb", mock_duckdb):
            df = report_app.fetch_dataframe(
                "SELECT Ticker, Volume FROM daily_close", config
            )

        # The fallback opened the DB read-only.
        mock_duckdb.connect.assert_called_once_with(
            db_path, read_only=True
        )
        assert list(df["Ticker"]) == ["AAA"]

    def test_both_paths_fail_returns_none(self, tmp_path):
        """Flight unreachable AND read-only connect fails (write lock
        held) -> None; the UI renders st.error."""
        config = _config(tmp_path)

        mock_flight = _mock_flight()
        mock_flight.FlightClient.return_value.wait_for_available.side_effect = (
            flight.FlightUnavailableError("connection refused")
        )
        mock_duckdb = MagicMock()
        mock_duckdb.connect.side_effect = duckdb.IOException(
            "Cannot open file: it is being used by another process"
        )

        with patch.object(
            report_app, "flight", mock_flight
        ), patch.object(report_app, "duckdb", mock_duckdb):
            df = report_app.fetch_dataframe("SELECT 1", config)

        assert df is None

    def test_flight_query_error_also_falls_back(self, tmp_path):
        """A mid-call Flight failure (not just connect) takes the
        fallback path too."""
        config = _config(tmp_path)
        db_path = config.db_path
        _seed_daily_close(
            db_path, [("2026-09-01", "AAA", 1.0, 1.0, 1.0, 1.0, 10.0)]
        )

        mock_flight = _mock_flight()
        mock_flight.FlightClient.return_value.do_get.side_effect = (
            flight.FlightServerError("query exploded")
        )

        with patch.object(report_app, "flight", mock_flight):
            df = report_app.fetch_dataframe(
                "SELECT Ticker FROM daily_close", config
            )

        assert list(df["Ticker"]) == ["AAA"]


class TestDataframeToXlsxBytes:
    """In-memory xlsx serializer used by st.download_button."""

    def test_produces_readable_workbook_with_widths(self):
        df = pd.DataFrame(
            {
                "Ticker": ["MSFT", "AAPL"],
                "rvol": [2.1, 1.9],
                "latest_dollar_volume": [1234.5, 678.9],
            }
        )
        blob = report_app.dataframe_to_xlsx_bytes(df, sheet_name="Report")
        assert isinstance(blob, bytes) and len(blob) > 0

        wb = openpyxl.load_workbook(io.BytesIO(blob))
        ws = wb["Report"]
        headers = [cell.value for cell in ws[1]]
        assert headers == ["Ticker", "rvol", "latest_dollar_volume"]
        data_rows = list(ws.iter_rows(min_row=2, values_only=True))
        assert data_rows[0][0] == "MSFT"
        assert data_rows[1][0] == "AAPL"
        # Auto-adjusted widths: each column dimension is set.
        for col_letter, header in zip(("A", "B", "C"), headers):
            width = ws.column_dimensions[col_letter].width
            assert width is not None
            assert width >= len(header)
        wb.close()

    def test_empty_dataframe_still_produces_workbook(self):
        df = pd.DataFrame({"Ticker": [], "rvol": []})
        blob = report_app.dataframe_to_xlsx_bytes(df, sheet_name="Empty")
        wb = openpyxl.load_workbook(io.BytesIO(blob))
        ws = wb["Empty"]
        assert [cell.value for cell in ws[1]] == ["Ticker", "rvol"]
        wb.close()
