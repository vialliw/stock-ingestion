"""Tests for ``scripts/correlation_dashboard.py`` — the SQL / pandas /
figure module behind the report_app "Correlation" tab (F22-T1).

Exercised without a Streamlit runtime and (except where importorskip'd)
without plotly: the SQL builder, ``normalize_dates`` (the F13
object-dtype ``datetime.date`` regression), ``to_wide`` pivot/labels/
dropna (incl. the CL=F misaligned-calendar case), ``returns`` kinds,
``filter_window`` presets, normalized base-100 math, rolling corr vs a
pandas reference, structural assertions on the five figures, and the
``CORR_GUIDE`` per-chart markdown dict.
"""

from __future__ import annotations

import ast
import sys
from datetime import date
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import pytest

# Add scripts dir to path so correlation_dashboard can be imported
# (same pattern as tests/test_fear_greed_dashboard.py).
SCRIPTS_DIR = Path(__file__).parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

import correlation_dashboard as cd


def _long_df(n: int = 80, end: str = "2026-09-18") -> pd.DataFrame:
    """Synthetic long (Date, Ticker, Close) frame covering all five
    CORR_TICKERS — ``Date`` left as object-dtype ``datetime.date``
    like Flight delivers (the F13 lesson)."""
    dates = pd.date_range(end=end, periods=n, freq="B")
    recs = []
    for k, ticker in enumerate(cd.CORR_TICKERS):
        for i, d in enumerate(dates):
            recs.append(
                {
                    "Date": d.date(),
                    "Ticker": ticker,
                    "Close": float(40.0 * (k + 1) + i * (0.3 + 0.1 * k)),
                }
            )
    df = pd.DataFrame.from_records(recs)
    df["Date"] = df["Date"].astype(object)
    return df


def _wide(n: int = 80) -> pd.DataFrame:
    return cd.to_wide(_long_df(n))


class TestModuleImportableWithoutPlotly:
    """The module must import cleanly on a host without plotly —
    figure builders lazy-import it."""

    def test_no_top_level_plotly_import(self):
        tree = ast.parse(Path(cd.__file__).read_text())
        for node in tree.body:
            if isinstance(node, ast.Import):
                for alias in node.names:
                    assert not alias.name.startswith("plotly"), node.lineno
            elif isinstance(node, ast.ImportFrom):
                assert node.module is None or not node.module.startswith(
                    "plotly"
                ), node.lineno


class TestConstants:
    def test_corr_tickers_mapping_and_order(self):
        assert cd.CORR_TICKERS == {
            "^TNX": "TNX_10Y",
            "CL=F": "WTI",
            "^IRX": "IRX_3M",
            "SPY": "SPY",
            "SMH": "SMH",
        }

    def test_window_presets_and_default(self):
        assert cd.WINDOW_PRESETS == (
            "1M",
            "3M",
            "6M",
            "YTD",
            "1Y",
            "2Y",
            "Max",
        )
        assert cd.DEFAULT_WINDOW == "Max"
        assert cd.DEFAULT_WINDOW in cd.WINDOW_PRESETS

    def test_default_pairs_are_known_labels(self):
        assert len(cd.DEFAULT_ROLLING_PAIRS) == 6
        labels = set(cd.CORR_TICKERS.values())
        for a, b in cd.DEFAULT_ROLLING_PAIRS:
            assert a in labels and b in labels and a != b

    def test_rolling_window_bounds(self):
        assert cd.ROLLING_WINDOW_MIN == 10
        assert cd.ROLLING_WINDOW_MAX == 252
        assert cd.DEFAULT_ROLLING_WINDOW == 60

    def test_pair_label_roundtrip(self):
        for pair in cd.DEFAULT_ROLLING_PAIRS:
            assert cd.parse_pair_label(cd.pair_label(pair)) == pair


class TestBuildCorrSql:
    def test_sql_shape(self):
        sql = cd.build_corr_sql()
        assert sql == (
            "SELECT Date, Ticker, Close FROM daily_close "
            "WHERE Ticker IN ('^TNX', 'CL=F', '^IRX', 'SPY', 'SMH') "
            "ORDER BY Date"
        )

    def test_generated_sql_executes_on_duckdb(self, tmp_path):
        """The emitted query matches the real daily_close DDL and the
        Ticker predicate holds."""
        db_path = str(tmp_path / "corr.duckdb")
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
                [
                    ("2026-09-16", "SPY", 1, 1, 1, 640.0, 1),
                    ("2026-09-16", "AAPL", 1, 1, 1, 250.0, 1),
                    ("2026-09-16", "^TNX", 1, 1, 1, 4.2, 1),
                    ("2026-09-17", "SPY", 1, 1, 1, 645.0, 1),
                ],
            )
            df = conn.execute(cd.build_corr_sql()).df()
        finally:
            conn.close()

        assert list(df.columns) == ["Date", "Ticker", "Close"]
        # AAPL is not in the five-ticker whitelist.
        assert set(df["Ticker"]) == {"SPY", "^TNX"}
        assert df["Date"].iloc[0] < df["Date"].iloc[-1]


class TestNormalizeDates:
    def test_object_dtype_datetime_date_converted(self):
        """F13 regression: Flight delivers DATE as object-dtype
        ``datetime.date`` — must become datetime64."""
        df = pd.DataFrame(
            {
                "Date": pd.Series(
                    [date(2026, 9, 17), date(2026, 9, 18)], dtype=object
                ),
                "Ticker": ["SPY", "SPY"],
                "Close": [640.0, 645.0],
            }
        )
        assert df["Date"].dtype == object
        out = cd.normalize_dates(df)
        assert pd.api.types.is_datetime64_any_dtype(out["Date"])
        # Input frame is not mutated.
        assert df["Date"].dtype == object


class TestToWide:
    def test_pivot_labels_and_index(self):
        wide = _wide(60)
        assert list(wide.columns) == [
            "TNX_10Y",
            "WTI",
            "IRX_3M",
            "SPY",
            "SMH",
        ]
        assert isinstance(wide.index, pd.DatetimeIndex)
        assert wide.index.name == "Date"
        assert wide.index.is_monotonic_increasing
        assert len(wide) == 60

    def test_misaligned_calendar_inner_aligned(self):
        """CL=F trades on equity-market holidays: a WTI-only date must
        drop out of the wide frame (dropna), not carry NaN closes."""
        df = _long_df(3)  # 2026-09-16/17/18 business days
        extra = pd.DataFrame(
            {
                "Date": pd.Series([date(2026, 9, 19)], dtype=object),
                "Ticker": ["CL=F"],
                "Close": [77.7],
            }
        )
        df = pd.concat([df, extra], ignore_index=True)
        wide = cd.to_wide(df)
        # The WTI-only Saturday dropped: 3 aligned rows remain.
        assert len(wide) == 3
        assert pd.Timestamp("2026-09-19") not in wide.index
        assert wide.notna().all().all()

    def test_missing_ticker_column_drops_out(self):
        """A configured ticker with no rows simply produces no column
        — never a crash, never an all-NaN column."""
        df = _long_df(10)
        df = df[df["Ticker"] != "^TNX"]
        wide = cd.to_wide(df)
        assert "TNX_10Y" not in wide.columns
        assert list(wide.columns) == ["WTI", "IRX_3M", "SPY", "SMH"]


class TestReturns:
    def test_daily_pct_change_matches_pandas(self):
        wide = _wide(50)
        out = cd.returns(wide, "daily")
        expected = wide.pct_change().dropna(how="all")
        pd.testing.assert_frame_equal(out, expected)
        # First all-NaN row dropped.
        assert len(out) == len(wide) - 1
        assert out.notna().all().all()

    def test_log_returns_match_pandas(self):
        wide = _wide(50)
        out = cd.returns(wide, "log")
        expected = np.log(wide).diff().dropna(how="all")
        pd.testing.assert_frame_equal(out, expected)

    def test_levels_is_passthrough(self):
        wide = _wide(20)
        out = cd.returns(wide, "levels")
        pd.testing.assert_frame_equal(out, wide)
        assert out is not wide  # a copy, not a view

    def test_unknown_kind_rejected(self):
        with pytest.raises(ValueError):
            cd.returns(_wide(10), "weekly")


class TestFilterWindow:
    def test_max_is_passthrough(self):
        wide = _wide(100)
        out = cd.filter_window(wide, "Max")
        assert len(out) == len(wide)
        assert out.index.min() == wide.index.min()

    def test_month_offset_and_ytd(self):
        wide = _wide(600)
        max_date = wide.index.max()
        out = cd.filter_window(wide, "1M")
        assert out.index.min() >= max_date - pd.DateOffset(months=1)
        assert out.index.max() == max_date
        ytd = cd.filter_window(wide, "YTD")
        assert ytd.index.min() >= pd.Timestamp(
            year=max_date.year, month=1, day=1
        )
        assert (ytd.index.year == max_date.year).all()

    def test_unknown_preset_rejected(self):
        with pytest.raises(ValueError):
            cd.filter_window(_wide(10), "5Y")

    def test_empty_frame_passthrough(self):
        wide = _wide(10).iloc[0:0]
        for preset in cd.WINDOW_PRESETS:
            assert cd.filter_window(wide, preset).empty


class TestBuildHeatmapFigures:
    def test_two_figures_scale_and_annotations(self):
        pytest.importorskip("plotly")
        wide = _wide(80)
        levels_fig, returns_fig = cd.build_heatmap_figures(
            wide, "daily"
        )
        for fig in (levels_fig, returns_fig):
            assert len(fig.data) == 1
            hm = fig.data[0]
            assert hm.zmin == -1 and hm.zmax == 1
            # 5x5 matrix over the display labels.
            assert np.asarray(hm.z).shape == (5, 5)
            assert list(hm.x) == list(cd.CORR_TICKERS.values())
            assert list(hm.y) == list(cd.CORR_TICKERS.values())
            # Annotated cells via text + texttemplate.
            assert hm.text is not None
            assert "%{text" in hm.texttemplate
            # Diagonal is exactly 1.
            assert np.allclose(np.diag(np.asarray(hm.z, dtype=float)), 1.0)

    def test_titles_reflect_kinds(self):
        pytest.importorskip("plotly")
        wide = _wide(40)
        levels_fig, daily_fig = cd.build_heatmap_figures(wide, "daily")
        assert "Levels" in levels_fig.layout.title.text
        assert "Daily Returns" in daily_fig.layout.title.text
        _, log_fig = cd.build_heatmap_figures(wide, "log")
        assert "Log Returns" in log_fig.layout.title.text

    def test_heatmap_values_match_pandas_corr(self):
        pytest.importorskip("plotly")
        wide = _wide(60)
        levels_fig, returns_fig = cd.build_heatmap_figures(
            wide, "daily"
        )
        assert np.allclose(
            np.asarray(levels_fig.data[0].z, dtype=float),
            wide.corr().values,
        )
        assert np.allclose(
            np.asarray(returns_fig.data[0].z, dtype=float),
            wide.pct_change().dropna(how="all").corr().values,
        )


class TestBuildNormalizedFigure:
    def test_base_100_and_trace_count(self):
        pytest.importorskip("plotly")
        wide = _wide(60)
        fig = cd.build_normalized_figure(wide)
        assert len(fig.data) == 5
        assert [t.name for t in fig.data] == list(
            cd.CORR_TICKERS.values()
        )
        # Every series starts at exactly 100.
        for t in fig.data:
            assert t.y[0] == pytest.approx(100.0)
        # Category x axis of YYYY-MM-DD strings.
        assert all(isinstance(v, str) for v in fig.data[0].x)
        assert fig.layout.xaxis.type == "category"

    def test_normalized_math_correct(self):
        """Hand-checked: a column doubling over the window ends at
        200."""
        pytest.importorskip("plotly")
        wide = pd.DataFrame(
            {
                "SPY": [100.0, 150.0, 200.0],
                "SMH": [50.0, 50.0, 100.0],
            },
            index=pd.to_datetime(
                ["2026-09-16", "2026-09-17", "2026-09-18"]
            ),
        )
        fig = cd.build_normalized_figure(wide)
        spy = fig.data[0]
        smh = fig.data[1]
        assert list(spy.y) == [100.0, 150.0, 200.0]
        assert list(smh.y) == [100.0, 100.0, 200.0]


class TestBuildMultipanelFigure:
    def test_three_rows_and_secondary_axes(self):
        pytest.importorskip("plotly")
        wide = _wide(60)
        fig = cd.build_multipanel_figure(wide)
        # 7 traces: WTI + TNX (row1), SPY+SMH+TNX (row2), IRX+TNX (row3).
        assert len(fig.data) == 7
        names = [t.name for t in fig.data]
        assert names == [
            "WTI",
            "TNX_10Y",
            "SPY",
            "SMH",
            "TNX_10Y",
            "IRX_3M",
            "TNX_10Y",
        ]
        layout = fig.to_dict()["layout"]
        # Row 1 secondary axis: TNX on y2 overlaying y.
        y2 = layout["yaxis2"]
        assert y2["overlaying"] == "y"
        assert y2["side"] == "right"
        # Row 2 secondary axis exists too; row 3 has a single axis.
        assert "yaxis4" in layout  # row2 secondary (y4 overlays y3)
        assert layout["yaxis4"]["overlaying"] == "y3"
        # Axis titles carry units.
        assert "%" in layout["yaxis2"]["title"]["text"]
        assert "$" in layout["yaxis"]["title"]["text"]
        # Traces reference the right axes: row1 WTI on y, TNX on y2.
        assert fig.data[0].yaxis == "y"
        assert fig.data[1].yaxis == "y2"
        # Row 3 traces share one axis (y5), not secondary.
        assert fig.data[5].yaxis == "y5"
        assert fig.data[6].yaxis == "y5"

    def test_missing_series_skipped_per_panel(self):
        pytest.importorskip("plotly")
        wide = _wide(30).drop(columns=["WTI"])
        fig = cd.build_multipanel_figure(wide)
        names = [t.name for t in fig.data]
        assert "WTI" not in names
        assert "TNX_10Y" in names and "SPY" in names


class TestBuildRollingFigure:
    def test_rolling_corr_matches_pandas_reference(self):
        pytest.importorskip("plotly")
        # Synthetic data with an embedded correlation structure.
        rng = np.random.default_rng(7)
        n = 200
        base = rng.normal(size=n)
        idx = pd.date_range("2025-01-02", periods=n, freq="B")
        wide = pd.DataFrame(
            {
                "SPY": 100 * np.exp(np.cumsum(base) * 0.01),
                "SMH": 100
                * np.exp(
                    np.cumsum(0.8 * base + 0.6 * rng.normal(size=n))
                    * 0.01
                ),
                "WTI": 60 * np.exp(np.cumsum(rng.normal(size=n)) * 0.01),
            },
            index=idx,
        )
        pairs = [("SMH", "SPY"), ("WTI", "SPY")]
        fig = cd.build_rolling_figure(wide, 20, pairs)
        assert len(fig.data) == 2
        rets = wide.pct_change().dropna(how="all")
        expected = rets["SMH"].rolling(20).corr(rets["SPY"])
        got = pd.Series(
            np.asarray(fig.data[0].y, dtype=float), index=rets.index
        )
        pd.testing.assert_series_equal(
            got, expected, check_names=False
        )
        # Dashed y=0 hline.
        shapes = list(fig.layout.shapes)
        assert len(shapes) == 1
        assert shapes[0].y0 == 0 and shapes[0].y1 == 0
        assert shapes[0].line.dash == "dash"

    def test_pair_count_and_unknown_pair_skipped(self):
        pytest.importorskip("plotly")
        wide = _wide(80)
        fig = cd.build_rolling_figure(
            wide, 30, list(cd.DEFAULT_ROLLING_PAIRS)
        )
        assert len(fig.data) == 6
        fig2 = cd.build_rolling_figure(
            wide, 30, [("SPY", "SMH"), ("NOPE", "SPY")]
        )
        assert len(fig2.data) == 1
        assert fig2.data[0].name == "SPY vs SMH"
        # Category x axis.
        assert fig.layout.xaxis.type == "category"


class TestBuildScatterFigure:
    def test_markers_trendline_and_corr_title(self):
        pytest.importorskip("plotly")
        wide = _wide(60)
        fig = cd.build_scatter_figure(wide, "WTI", "TNX_10Y")
        assert len(fig.data) == 2
        markers, trend = fig.data
        assert markers.mode == "markers"
        assert trend.mode == "lines"
        assert trend.line.dash == "dash"
        # OLS trendline = np.polyfit on the raw levels.
        slope, intercept = np.polyfit(
            wide["WTI"].to_numpy(), wide["TNX_10Y"].to_numpy(), 1
        )
        xs = np.asarray(trend.x, dtype=float)
        ys = np.asarray(trend.y, dtype=float)
        assert np.allclose(ys, slope * xs + intercept)
        # Pearson corr in the title.
        corr = float(wide["WTI"].corr(wide["TNX_10Y"]))
        assert f"{corr:.2f}" in fig.layout.title.text
        assert "WTI" in fig.layout.title.text
        assert "TNX_10Y" in fig.layout.title.text
        # Axis titles are the selected labels.
        assert fig.layout.xaxis.title.text == "WTI"
        assert fig.layout.yaxis.title.text == "TNX_10Y"


class TestCorrGuide:
    """``CORR_GUIDE`` — one markdown entry per chart key, rendered in
    the tab's per-chart "How to read this chart" expanders."""

    def test_keys_cover_all_five_charts(self):
        assert set(cd.CORR_GUIDE) == {
            "heatmap",
            "normalized",
            "multipanel",
            "rolling",
            "scatter",
        }

    def test_entries_non_empty_markdown(self):
        for key, text in cd.CORR_GUIDE.items():
            assert isinstance(text, str) and text.strip(), key

    def test_no_screenshot_references(self):
        """The app shows live data — every screenshot reference from
        the source doc was genericized away."""
        for key, text in cd.CORR_GUIDE.items():
            assert "screenshot" not in text.lower(), key

    def test_required_caveats_present(self):
        guide = "\n".join(cd.CORR_GUIDE.values())
        # levels-vs-returns caveat
        assert "levels" in guide.lower() and "returns" in guide.lower()
        # rolling regime-shift reading incl. oil-10Y spikes
        assert "regime" in guide.lower()
        # SMH duration note
        assert "SMH" in guide and "duration" in guide.lower()
        # ^IRX is a proxy caveat
        assert "proxy" in guide.lower() and "IRX" in guide
        # general caveats
        assert "causation" in guide.lower()
        assert "window" in guide.lower()
