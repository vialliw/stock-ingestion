"""Tests for ``scripts/breadth_dashboard.py`` — the market-breadth
dashboard port of the legacy ``breadth_builder02.py``.

Exercises the verbatim SQL builders, the legacy EMA seeding, the
indicator math (incl. both ``replace(0, 1)`` zero-division guards),
the post-query years filter, the 5-subplot figure, and the
``generate_breadth_dashboard`` orchestration with a monkeypatched
fetch — no Flight server or real database is touched.  ``plotly`` is
imported inside the test functions that need it (the module itself
lazy-imports it, so collection does not require it either).
"""

from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path

import duckdb
import pandas as pd
import pytest

# Add scripts dir to path so breadth_dashboard / report_app can be
# imported (same pattern as tests/test_report_app.py).
SCRIPTS_DIR = Path(__file__).parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

import breadth_dashboard  # noqa: E402
import report_app  # noqa: E402

from src.config import PipelineConfig  # noqa: E402


# ------------------------------------------------------------------
# Fixtures / helpers
# ------------------------------------------------------------------

_BREADTH_COLS = [
    "Date",
    "Total_Stocks",
    "Advances",
    "Declines",
    "Above_SMA_50",
    "Above_SMA_200",
    "Adv_Volume",
    "Dec_Volume",
    "New_Highs",
    "New_Lows",
]


def _breadth_df(dates) -> pd.DataFrame:
    """Synthetic breadth frame over ``dates`` (post-``breadth_agg``
    shape) with non-zero volume/high-low columns."""
    n = len(dates)
    return pd.DataFrame(
        {
            "Date": pd.to_datetime(pd.Series(dates)),
            "Total_Stocks": [100] * n,
            "Advances": [60] * n,
            "Declines": [30] * n,
            "Above_SMA_50": [55] * n,
            "Above_SMA_200": [45] * n,
            "Adv_Volume": [6000.0] * n,
            "Dec_Volume": [3000.0] * n,
            "New_Highs": [12] * n,
            "New_Lows": [4] * n,
        }
    )


def _spy_df(dates) -> pd.DataFrame:
    """Synthetic SPY OHLCV frame over ``dates``."""
    n = len(dates)
    return pd.DataFrame(
        {
            "Date": pd.to_datetime(pd.Series(dates)),
            "Open": [100.0 + i for i in range(n)],
            "High": [101.0 + i for i in range(n)],
            "Low": [99.0 + i for i in range(n)],
            "Close": [100.5 + i for i in range(n)],
            "Volume": [1_000_000.0] * n,
        }
    )


def _config(tmp_path, market_breadth_years=0) -> PipelineConfig:
    """Config rooted at ``tmp_path`` (nothing touches the real
    C:\\Intel\\parquet)."""
    return PipelineConfig(
        database={
            "duckdb_dir": str(tmp_path),
            "duckdb_filename": "test.duckdb",
        },
        logging={"log_file": str(tmp_path / "logs" / "run.log")},
        reports={
            "report_dir": str(tmp_path / "reports"),
            "market_breadth_years": market_breadth_years,
        },
    )


def _fake_fetch(breadth_df, spy_df):
    """Return a ``report_app.fetch_dataframe`` stand-in that serves
    the two canned frames by matching the SPY query."""
    def fetch(sql, config):
        if "Ticker = 'SPY'" in sql:
            return spy_df
        return breadth_df

    return fetch


# ------------------------------------------------------------------
# SQL builders — verbatim key clauses from the legacy file
# ------------------------------------------------------------------


class TestSqlBuilders:
    def test_breadth_sql_verbatim_clauses(self):
        sql = breadth_dashboard.build_breadth_sql()
        assert "WITH price_lag AS" in sql
        assert "LAG(Close) OVER (PARTITION BY Ticker ORDER BY Date) as Prev_Close" in sql
        assert "WHERE Ticker != 'SPY'" in sql
        assert "ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) as SMA_50" in sql
        assert "ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) as SMA_200" in sql
        # 252-day lookback excludes the current row (legacy FIX).
        assert "ROWS BETWEEN 252 PRECEDING AND 1 PRECEDING) as High_252d" in sql
        assert "ROWS BETWEEN 252 PRECEDING AND 1 PRECEDING) as Low_252d" in sql
        assert "WHERE m.Prev_Close IS NOT NULL" in sql
        assert "COALESCE(nh.New_Highs, 0) as New_Highs" in sql
        assert "SELECT * FROM breadth_agg" in sql

    def test_spy_sql_verbatim(self):
        sql = breadth_dashboard.build_spy_sql()
        assert "SELECT Date, Open, High, Low, Close, Volume" in sql
        assert "FROM daily_close" in sql
        assert "WHERE Ticker = 'SPY'" in sql
        assert "ORDER BY Date" in sql

    def test_generated_sql_executes_on_duckdb(self, tmp_path):
        """Both queries are valid DuckDB SQL against a seeded
        daily_close table (run on a read-only connection, like the
        production fallback path)."""
        db_path = str(tmp_path / "test.duckdb")
        rows = []
        base = date(2026, 1, 1)
        for i in range(60):
            day = (base + timedelta(days=i)).isoformat()
            # justified: seeding distinct synthetic rows per period —
            # deterministic test fixture, not data transformation.
            for ticker in ("T1", "T2", "SPY"):
                rows.append(
                    (day, ticker, 1.0, 2.0, 0.5, 1.0 + i, 100.0)
                )
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
                "INSERT INTO daily_close VALUES (?, ?, ?, ?, ?, ?, ?)",
                rows,
            )
        finally:
            conn.close()

        conn = duckdb.connect(db_path, read_only=True)
        try:
            breadth = conn.execute(
                breadth_dashboard.build_breadth_sql()
            ).df()
            spy = conn.execute(breadth_dashboard.build_spy_sql()).df()
        finally:
            conn.close()

        # First day per ticker has Prev_Close NULL -> dropped.
        assert len(breadth) == 59
        for col in _BREADTH_COLS:
            assert col in breadth.columns
        # SPY is excluded from the breadth universe.
        assert breadth["Total_Stocks"].max() == 2
        assert len(spy) == 60
        assert list(spy.columns) == [
            "Date", "Open", "High", "Low", "Close", "Volume",
        ]


# ------------------------------------------------------------------
# calculate_ema — legacy SMA-seed warmup (NOT pd.Series.ewm)
# ------------------------------------------------------------------


class TestCalculateEma:
    def test_sma_seed_and_recursive_tail(self):
        series = pd.Series(
            [10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0]
        )
        ema = breadth_dashboard.calculate_ema(series, 5)

        # First `period` values = SMA of them (12.0) — the legacy
        # seeding; ewm would produce something different here.
        assert list(ema.iloc[:5]) == pytest.approx([12.0] * 5)

        mult = 2 / (5 + 1)
        e5 = 15.0 * mult + 12.0 * (1 - mult)
        assert ema.iloc[5] == pytest.approx(e5)
        e6 = 16.0 * mult + e5 * (1 - mult)
        assert ema.iloc[6] == pytest.approx(e6)
        e7 = 17.0 * mult + e6 * (1 - mult)
        assert ema.iloc[7] == pytest.approx(e7)

    def test_shorter_than_period_all_sma(self):
        series = pd.Series([1.0, 2.0, 3.0])
        ema = breadth_dashboard.calculate_ema(series, 5)
        assert list(ema) == pytest.approx([2.0, 2.0, 2.0])


# ------------------------------------------------------------------
# compute_indicators — six derived columns incl. replace(0, 1) guards
# ------------------------------------------------------------------


class TestComputeIndicators:
    def test_formulas_on_synthetic_rows(self):
        df = pd.DataFrame(
            {
                "Date": pd.to_datetime(
                    ["2026-01-01", "2026-01-02", "2026-01-03"]
                ),
                "Total_Stocks": [100, 200, 50],
                "Advances": [60, 40, 10],
                "Declines": [30, 50, 10],
                "Above_SMA_50": [50, 100, 25],
                "Above_SMA_200": [40, 80, 20],
                "Adv_Volume": [6000.0, 4000.0, 1000.0],
                "Dec_Volume": [3000.0, 0.0, 500.0],   # row 2: zero guard
                "New_Highs": [12, 8, 5],
                "New_Lows": [4, 0, 5],                # row 2: zero guard
            }
        )
        out = breadth_dashboard.compute_indicators(df)

        # ADL = cumsum(Advances - Declines): 30, 30-10=20, 20+0=20.
        assert list(out["ADL"]) == [30, 20, 20]
        # Pct_Above_*: Above / Total * 100.
        assert list(out["Pct_Above_SMA50"]) == [50.0, 50.0, 50.0]
        assert list(out["Pct_Above_SMA200"]) == [40.0, 40.0, 40.0]
        # Volume_Ratio: Dec_Volume 0 -> replaced by 1 (no div-by-zero).
        assert list(out["Volume_Ratio"]) == [2.0, 4000.0, 2.0]
        # Net_New_Highs = New_Highs - New_Lows.
        assert list(out["Net_New_Highs"]) == [8, 8, 0]
        # NH_NL_Ratio: New_Lows 0 -> replaced by 1.
        assert list(out["NH_NL_Ratio"]) == [3.0, 8.0, 1.0]

    def test_input_frame_not_mutated(self):
        df = _breadth_df(pd.to_datetime(["2026-01-01"]))
        breadth_dashboard.compute_indicators(df)
        assert "ADL" not in df.columns


# ------------------------------------------------------------------
# filter_years — post-query pandas cutoff; 0/None passthrough
# ------------------------------------------------------------------


class TestFilterYears:
    def test_cutoff_applied_to_both_frames(self):
        dates = pd.date_range("2020-01-01", periods=8, freq="YS")
        breadth = _breadth_df(dates)
        spy = _spy_df(dates)

        out_b, out_s = breadth_dashboard.filter_years(breadth, spy, 2)
        # The cutoff is anchored at the latest date across BOTH frames
        # (legacy semantics) — here the two frames share the range.
        max_date = max(breadth["Date"].max(), spy["Date"].max())
        cutoff = max_date - pd.DateOffset(years=2)
        assert out_b["Date"].min() >= cutoff
        assert out_s["Date"].min() >= cutoff
        assert len(out_b) < len(breadth)
        assert len(out_s) < len(spy)

    def test_zero_and_none_pass_through(self):
        dates = pd.date_range("2020-01-01", periods=8, freq="YS")
        breadth = _breadth_df(dates)
        spy = _spy_df(dates)

        for years in (0, None):
            out_b, out_s = breadth_dashboard.filter_years(
                breadth, spy, years
            )
            assert len(out_b) == len(breadth)
            assert len(out_s) == len(spy)

    def test_string_dates_converted_to_datetime(self):
        breadth = _breadth_df(pd.to_datetime(["2026-01-05"]))
        spy = _spy_df(pd.to_datetime(["2026-01-05"]))
        breadth["Date"] = breadth["Date"].astype(str)
        spy["Date"] = spy["Date"].astype(str)
        out_b, out_s = breadth_dashboard.filter_years(breadth, spy, 0)
        assert pd.api.types.is_datetime64_any_dtype(out_b["Date"])
        assert pd.api.types.is_datetime64_any_dtype(out_s["Date"])


# ------------------------------------------------------------------
# build_breadth_figure — 5-row figure, expected traces
# ------------------------------------------------------------------


class TestBuildBreadthFigure:
    def _frames(self):
        dates = pd.date_range("2025-01-01", periods=80, freq="B")
        breadth = breadth_dashboard.compute_indicators(
            _breadth_df(dates)
        )
        spy = _spy_df(dates)
        spy["EMA_50"] = breadth_dashboard.calculate_ema(
            spy["Close"], 50
        )
        return breadth, spy

    def test_returns_go_figure_with_five_rows(self):
        import plotly.graph_objects as go

        breadth, spy = self._frames()
        fig = breadth_dashboard.build_breadth_figure(breadth, spy)
        assert isinstance(fig, go.Figure)
        # Five stacked rows -> yaxis..yaxis5 (plus the two overlaying
        # secondary axes).
        for axis in ("yaxis", "yaxis2", "yaxis3", "yaxis4", "yaxis5"):
            assert axis in fig.layout
        assert fig.layout.height == 1300
        assert fig.layout.width == 1280

    def test_expected_traces(self):
        breadth, spy = self._frames()
        fig = breadth_dashboard.build_breadth_figure(breadth, spy)
        names = [t.name for t in fig.data]
        assert names == [
            "SPY",
            "50-Day EMA",
            "% > SMA50",
            "% > SMA200",
            "Net Advances",
            "Vol Ratio (Adv/Dec)",
            "Net New Highs",
            "NH/NL Ratio",
        ]
        # Row-5 bar visibility fixes kept verbatim.
        nhnl = fig.data[6]
        assert nhnl.opacity == 1.0
        assert nhnl.marker.line.width == 0
        # Vol Ratio rides the overlaying y4 axis; NH/NL on y5.
        assert fig.data[5].yaxis == "y4"
        assert fig.data[7].yaxis == "y5"

    def test_nhnl_ratio_trace_skipped_when_all_nan(self):
        breadth, spy = self._frames()
        breadth["NH_NL_Ratio"] = float("nan")
        fig = breadth_dashboard.build_breadth_figure(breadth, spy)
        assert len(fig.data) == 7  # no 'NH/NL Ratio' trace


# ------------------------------------------------------------------
# generate_breadth_dashboard — monkeypatched fetch, file output
# ------------------------------------------------------------------


class TestGenerateBreadthDashboard:
    def test_writes_fixed_name_html_with_guide(self, tmp_path, monkeypatch):
        dates = pd.date_range("2025-01-01", periods=80, freq="B")
        monkeypatch.setattr(
            report_app,
            "fetch_dataframe",
            _fake_fetch(_breadth_df(dates), _spy_df(dates)),
        )
        config = _config(tmp_path, market_breadth_years=0)
        out = breadth_dashboard.generate_breadth_dashboard(config)
        path = Path(out)
        assert path.name == "Stock_Ingestion_Market_Breadth.html"
        assert path.parent == Path(config.reports.report_dir)
        html = path.read_text(encoding="utf-8")
        assert "plotly" in html.lower()          # embedded plotly div
        assert "Complete Interpretation Guide" in html  # guide marker

    def test_output_dir_override(self, tmp_path, monkeypatch):
        dates = pd.date_range("2025-01-01", periods=80, freq="B")
        monkeypatch.setattr(
            report_app,
            "fetch_dataframe",
            _fake_fetch(_breadth_df(dates), _spy_df(dates)),
        )
        config = _config(tmp_path, market_breadth_years=0)
        target = tmp_path / "elsewhere"
        out = breadth_dashboard.generate_breadth_dashboard(
            config, output_dir=str(target)
        )
        assert Path(out).parent == target
        assert (target / "Stock_Ingestion_Market_Breadth.html").exists()

    def test_empty_breadth_raises_value_error(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            report_app,
            "fetch_dataframe",
            _fake_fetch(_breadth_df([]), _spy_df(pd.to_datetime(["2025-01-01"]))),
        )
        config = _config(tmp_path)
        with pytest.raises(ValueError, match="No breadth data"):
            breadth_dashboard.generate_breadth_dashboard(config)

    def test_empty_spy_raises_value_error(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            report_app,
            "fetch_dataframe",
            _fake_fetch(
                _breadth_df(pd.to_datetime(["2025-01-01"])),
                _spy_df([]),
            ),
        )
        config = _config(tmp_path)
        with pytest.raises(ValueError, match="No SPY data"):
            breadth_dashboard.generate_breadth_dashboard(config)

    def test_both_fetch_paths_fail_raises_runtime_error(
        self, tmp_path, monkeypatch
    ):
        monkeypatch.setattr(
            report_app, "fetch_dataframe", lambda sql, config: None
        )
        config = _config(tmp_path)
        with pytest.raises(RuntimeError):
            breadth_dashboard.generate_breadth_dashboard(config)

    def test_years_none_uses_config_default(self, tmp_path, monkeypatch):
        """With ``years=None`` the configured
        ``reports.market_breadth_years`` applies — here 1, so data
        older than a year is cut from the rendered HTML."""
        dates = pd.date_range("2020-01-01", periods=300, freq="B")
        monkeypatch.setattr(
            report_app,
            "fetch_dataframe",
            _fake_fetch(_breadth_df(dates), _spy_df(dates)),
        )
        config = _config(tmp_path, market_breadth_years=1)
        out = breadth_dashboard.generate_breadth_dashboard(
            config, years=None, output_dir=str(tmp_path)
        )
        html = Path(out).read_text(encoding="utf-8")
        # Earliest synthetic date is 2020-01-01 — must be cut.
        assert "2020-01-01" not in html
        # The most recent dates survive.
        assert str(dates[-1].date()) in html

    def test_years_zero_includes_all_data(self, tmp_path, monkeypatch):
        dates = pd.date_range("2020-01-01", periods=300, freq="B")
        monkeypatch.setattr(
            report_app,
            "fetch_dataframe",
            _fake_fetch(_breadth_df(dates), _spy_df(dates)),
        )
        config = _config(tmp_path, market_breadth_years=1)
        out = breadth_dashboard.generate_breadth_dashboard(
            config, years=0, output_dir=str(tmp_path)
        )
        html = Path(out).read_text(encoding="utf-8")
        # Explicit 0 overrides the config default -> all history kept.
        assert "2020-01-01" in html

    def test_main_positional_years_override(self, tmp_path, monkeypatch):
        """``main([\"5\"])`` routes a trailing-years override; also
        proves the entrypoint returns 0 on success."""
        dates = pd.date_range("2025-01-01", periods=80, freq="B")
        monkeypatch.setattr(
            report_app,
            "fetch_dataframe",
            _fake_fetch(_breadth_df(dates), _spy_df(dates)),
        )
        config = _config(tmp_path, market_breadth_years=0)
        monkeypatch.setattr(
            breadth_dashboard.PipelineConfig,
            "from_yaml",
            classmethod(lambda cls, path=None: config),
        )
        assert breadth_dashboard.main(["5"]) == 0


# ------------------------------------------------------------------
# Guide HTML — verbatim marker sanity
# ------------------------------------------------------------------


def test_guide_html_contains_legacy_markers():
    guide = breadth_dashboard.BREADTH_GUIDE_HTML
    assert "Complete Interpretation Guide" in guide
    assert "display:grid" in guide
    assert "Combined Signal Framework" in guide
    assert "New Highs vs. New Lows (252-Day Lookback)" in guide
