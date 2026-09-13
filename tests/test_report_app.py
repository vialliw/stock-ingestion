"""Tests for ``scripts/report_app.py`` — the Streamlit report app.

Only the non-UI surface is exercised: the SQL builders, the
threshold-inversion helper, the Flight-first / DuckDB-fallback fetch
logic, the ``st.cache_data`` key derivation (via a fake ``st``), and
the in-memory xlsx serializer.  No Streamlit server is started;
``streamlit`` is never imported by these tests (report_app imports it
lazily inside the UI functions only).
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


class TestFindThresholdInversions:
    """Module-level helper behind the per-tab ``st.warning`` calls —
    pure function, exercised here without streamlit."""

    def test_accel_pair_inverted(self):
        """strong_accel < accel — the live-verified footgun: the CASE
        checks strong first so rvol ~1.0 names get 'Strong
        Acceleration' and tab B's filter silently becomes
        rvol >= strong_accel."""
        msgs = report_app.find_threshold_inversions(
            accel=2.0, strong_accel=1.0
        )
        assert len(msgs) == 1
        assert "Inverted" in msgs[0]
        assert "Strong Acceleration" in msgs[0]
        # Both bounds appear so the user sees the misordering.
        assert "rvol >= 1" in msgs[0]
        assert "rvol >= 2" in msgs[0]

    def test_decel_pair_inverted(self):
        """strong_decel > decel — the mirror-image misordering on the
        deceleration side (tab A)."""
        msgs = report_app.find_threshold_inversions(
            decel=0.5, strong_decel=0.9
        )
        assert len(msgs) == 1
        assert "Inverted" in msgs[0]
        assert "Strong Deceleration" in msgs[0]
        assert "rvol <= 0.9" in msgs[0]
        assert "rvol <= 0.5" in msgs[0]

    def test_both_pairs_inverted_reports_two(self):
        msgs = report_app.find_threshold_inversions(
            accel=2.0, strong_accel=1.0, decel=0.5, strong_decel=0.9
        )
        assert len(msgs) == 2
        joined = " ".join(msgs)
        assert "Strong Acceleration" in joined
        assert "Strong Deceleration" in joined

    def test_defaults_not_inverted(self):
        assert (
            report_app.find_threshold_inversions(
                accel=report_app.DEFAULT_ACCEL,
                strong_accel=report_app.DEFAULT_STRONG_ACCEL,
                decel=report_app.DEFAULT_DECEL,
                strong_decel=report_app.DEFAULT_STRONG_DECEL,
            )
            == []
        )

    def test_equal_bounds_not_inverted(self):
        """Equal thresholds are not *inverted* — the strong label
        legitimately absorbs the boundary."""
        assert (
            report_app.find_threshold_inversions(
                accel=1.8, strong_accel=1.8, decel=0.8, strong_decel=0.8
            )
            == []
        )

    def test_none_pairs_skipped(self):
        """Tab B only has the accel pair — a ``None`` decel pair must
        produce no warning."""
        assert (
            report_app.find_threshold_inversions(
                accel=1.8, strong_accel=2.5
            )
            == []
        )
        assert report_app.find_threshold_inversions() == []


class _FakeCacheData:
    """Minimal stand-in for the ``st`` object's ``cache_data``.

    Implements the kwargs-decorator form (``@st.cache_data(ttl=...)``)
    as an in-memory dict keyed on the wrapped function's positional
    args — enough to prove the cache key is SQL-dependent — and records
    the decorator kwargs so the TTL is assertable.  Exposes ``.clear()``
    on the wrapper like the real ``st.cache_data`` function.
    """

    def __init__(self):
        self.decorator_kwargs = []
        self.store = {}

    def cache_data(self, **kwargs):
        self.decorator_kwargs.append(kwargs)
        store = self.store

        def decorator(fn):
            def wrapper(*args):
                if args not in store:
                    store[args] = fn(*args)
                return store[args]

            def clear():
                store.clear()

            wrapper.clear = clear
            return wrapper

        return decorator


class TestCachedFetch:
    """The ``st.cache_data`` layer — built lazily via
    ``_build_cached_fetch(st)`` / ``_get_cached_fetch(st)`` so it is
    exercisable here with a fake ``st`` (no streamlit import)."""

    @pytest.fixture(autouse=True)
    def _reset_cached_fetch_singleton(self):
        saved = report_app._CACHED_FETCH
        report_app._CACHED_FETCH = None
        yield
        report_app._CACHED_FETCH = saved

    def test_cache_key_is_sql_dependent(self, tmp_path):
        """Same SQL -> one underlying fetch; changed SQL (a widget
        rerun that altered the query) -> re-fetch."""
        fake_st = _FakeCacheData()
        cached = report_app._build_cached_fetch(fake_st)
        df = pd.DataFrame({"x": [1]})
        with patch.object(
            report_app, "_fetch_dataframe_uncached", return_value=df
        ) as mock_fetch:
            first = cached("SELECT 1", "grpc://h:1", "db1")
            second = cached("SELECT 1", "grpc://h:1", "db1")
            third = cached("SELECT 2", "grpc://h:1", "db1")

        assert mock_fetch.call_count == 2
        # Cache hit returns the stored object for the repeated key.
        assert first is second
        assert third is df
        # The two underlying calls differ only in the SQL text.
        sqls = [call.args[0] for call in mock_fetch.call_args_list]
        assert sqls == ["SELECT 1", "SELECT 2"]

    def test_cache_key_includes_connection_targets(self):
        """Same SQL against a different Flight location or DB path is
        a different cache entry."""
        fake_st = _FakeCacheData()
        cached = report_app._build_cached_fetch(fake_st)
        with patch.object(
            report_app,
            "_fetch_dataframe_uncached",
            return_value=pd.DataFrame({"x": [1]}),
        ) as mock_fetch:
            cached("SELECT 1", "grpc://a:1", "db1")
            cached("SELECT 1", "grpc://b:1", "db1")
            cached("SELECT 1", "grpc://a:1", "db2")
            cached("SELECT 1", "grpc://a:1", "db1")

        assert mock_fetch.call_count == 3

    def test_ttl_is_configured(self):
        fake_st = _FakeCacheData()
        report_app._build_cached_fetch(fake_st)
        assert len(fake_st.decorator_kwargs) == 1
        assert (
            fake_st.decorator_kwargs[0]["ttl"]
            == report_app._CACHE_TTL_S
        )
        assert report_app._CACHE_TTL_S > 0

    def test_clear_forces_refetch(self):
        """The "Refresh data" buttons call ``.clear()`` — after it,
        the same SQL is fetched again."""
        fake_st = _FakeCacheData()
        cached = report_app._build_cached_fetch(fake_st)
        with patch.object(
            report_app,
            "_fetch_dataframe_uncached",
            return_value=pd.DataFrame({"x": [1]}),
        ) as mock_fetch:
            cached("SELECT 1", "grpc://h:1", "db1")
            cached.clear()
            cached("SELECT 1", "grpc://h:1", "db1")

        assert mock_fetch.call_count == 2

    def test_cached_fetch_dataframe_derives_key_from_config(
        self, tmp_path
    ):
        """``cached_fetch_dataframe`` maps config -> (location,
        db_path) so the cache key is (sql, grpc target, db file)."""
        config = _config(tmp_path)
        fake_st = _FakeCacheData()
        df = pd.DataFrame({"x": [1]})
        with patch.object(
            report_app, "_fetch_dataframe_uncached", return_value=df
        ) as mock_fetch:
            report_app.cached_fetch_dataframe(fake_st, "SELECT 1", config)
            report_app.cached_fetch_dataframe(fake_st, "SELECT 1", config)

        assert mock_fetch.call_count == 1
        args = mock_fetch.call_args.args
        assert args[0] == "SELECT 1"
        assert args[1] == (
            f"grpc://{config.server.flight_host}:"
            f"{config.server.flight_port}"
        )
        assert args[2] == config.db_path

    def test_get_cached_fetch_builds_singleton_once(self):
        """The st.cache_data wrapper is built once and reused — a
        fresh wrapper per rerun would never hit the cache."""
        fake_st = _FakeCacheData()
        first = report_app._get_cached_fetch(fake_st)
        second = report_app._get_cached_fetch(fake_st)
        assert first is second
        assert len(fake_st.decorator_kwargs) == 1


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
