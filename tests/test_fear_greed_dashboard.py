"""Tests for ``scripts/fear_greed_dashboard.py`` — the SQL / pandas /
figure module behind the report_app "Fear & Greed" tab (F18-T1).

Exercised without a Streamlit runtime and (except where importorskip'd)
without plotly: the SQL builder, ``normalize_dates`` (the F13
object-dtype ``datetime.date`` regression), ``filter_window`` presets,
the rating->ordinal mapping, and structural assertions on the three
figures (gauge steps/delta, history band shapes + MA toggle, heatmap
ordinal matrix + discrete colorscale + hover customdata).
"""

from __future__ import annotations

import ast
import sys
from datetime import date
from pathlib import Path

import duckdb
import pandas as pd
import pytest

# Add scripts dir to path so fear_greed_dashboard can be imported (same
# pattern as tests/test_report_app.py).
SCRIPTS_DIR = Path(__file__).parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

import fear_greed_dashboard as fgd


def _fg_df(n: int = 60, end: str = "2026-09-18") -> pd.DataFrame:
    """Synthetic normalized ``fear_greed_history`` frame — all 21
    display columns, business-day dates ending at ``end``."""
    dates = pd.date_range(end=end, periods=n, freq="B")
    data = {
        "date": dates,
        "fg_score": [30.0 + i * 0.25 for i in range(n)],
        "fg_rating": ["fear"] * n,
    }
    for key, _label in fgd.INDICATORS:
        data[key] = [float(i % 10) for i in range(n)]
        data[f"{key}_rating"] = ["neutral"] * n
    return pd.DataFrame(data)


class TestModuleImportableWithoutPlotly:
    """The module must import cleanly on a host without plotly —
    figure builders lazy-import it (AC1)."""

    def test_no_top_level_plotly_import(self):
        tree = ast.parse(Path(fgd.__file__).read_text())
        for node in tree.body:
            if isinstance(node, ast.Import):
                for alias in node.names:
                    assert not alias.name.startswith("plotly"), node.lineno
            elif isinstance(node, ast.ImportFrom):
                assert node.module is None or not node.module.startswith(
                    "plotly"
                ), node.lineno


class TestConstants:
    def test_rating_order_vocabulary(self):
        assert fgd.RATING_ORDER == {
            "extreme fear": 0,
            "fear": 1,
            "neutral": 2,
            "greed": 3,
            "extreme greed": 4,
        }

    def test_palette_and_bounds(self):
        assert fgd.RATING_COLORS == [
            "#d62728",
            "#ff7f0e",
            "#7f7f7f",
            "#9ed36a",
            "#2ca02c",
        ]
        assert fgd.BAND_BOUNDS == [0, 25, 45, 55, 75, 100]
        # One color per band.
        assert len(fgd.RATING_COLORS) == len(fgd.BAND_BOUNDS) - 1

    def test_window_presets_and_default(self):
        assert fgd.WINDOW_PRESETS == (
            "1M",
            "3M",
            "6M",
            "YTD",
            "1Y",
            "2Y",
            "Max",
        )
        assert fgd.DEFAULT_WINDOW == "1Y"
        assert fgd.DEFAULT_WINDOW in fgd.WINDOW_PRESETS

    def test_indicator_labels_cover_all_nine(self):
        assert len(fgd.INDICATORS) == 9
        keys = [k for k, _ in fgd.INDICATORS]
        assert keys == [
            "stock_price_strength",
            "stock_price_breadth",
            "market_momentum_sp500",
            "market_momentum_sp125",
            "market_volatility_vix",
            "market_volatility_vix_50",
            "put_call_options",
            "junk_bond_demand",
            "safe_haven_demand",
        ]
        labels = [label for _, label in fgd.INDICATORS]
        assert labels == [
            "Stock Price Strength",
            "Stock Price Breadth",
            "Momentum (SPX)",
            "Momentum (125d MA)",
            "Volatility (VIX)",
            "VIX 50d MA",
            "Put/Call",
            "Junk Bond Demand",
            "Safe Haven Demand",
        ]


class TestBuildFearGreedSql:
    def test_selects_all_display_columns(self):
        sql = fgd.build_fear_greed_sql()
        for col in ("date", "fg_score", "fg_rating"):
            assert col in sql
        for key, _label in fgd.INDICATORS:
            assert key in sql
            assert f"{key}_rating" in sql
        assert "FROM fear_greed_history" in sql
        assert "ORDER BY date" in sql
        # The audit column is deliberately not selected.
        assert "ingested_at" not in sql

    def test_generated_sql_executes_on_duckdb(self, tmp_path):
        """The emitted column list matches the real table DDL."""
        db_path = str(tmp_path / "fg.duckdb")
        conn = duckdb.connect(db_path)
        try:
            conn.execute(
                """
                CREATE TABLE fear_greed_history (
                    date DATE PRIMARY KEY,
                    fg_score DOUBLE,
                    fg_rating VARCHAR,
                    stock_price_strength DOUBLE,
                    stock_price_strength_rating VARCHAR,
                    stock_price_breadth DOUBLE,
                    stock_price_breadth_rating VARCHAR,
                    market_momentum_sp500 DOUBLE,
                    market_momentum_sp500_rating VARCHAR,
                    market_momentum_sp125 DOUBLE,
                    market_momentum_sp125_rating VARCHAR,
                    market_volatility_vix DOUBLE,
                    market_volatility_vix_rating VARCHAR,
                    market_volatility_vix_50 DOUBLE,
                    market_volatility_vix_50_rating VARCHAR,
                    put_call_options DOUBLE,
                    put_call_options_rating VARCHAR,
                    junk_bond_demand DOUBLE,
                    junk_bond_demand_rating VARCHAR,
                    safe_haven_demand DOUBLE,
                    safe_haven_demand_rating VARCHAR,
                    ingested_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            conn.execute(
                "INSERT INTO fear_greed_history (date, fg_score, "
                "fg_rating) VALUES ('2026-09-17', 40.0, 'fear'), "
                "('2026-09-18', 42.5, 'fear')"
            )
            df = conn.execute(fgd.build_fear_greed_sql()).df()
        finally:
            conn.close()

        assert len(df) == 2
        assert list(df["fg_score"]) == [40.0, 42.5]
        # 3 composite + 9 value + 9 rating = 21 columns, date ascending.
        assert df.shape[1] == 21
        assert df["date"].iloc[0] < df["date"].iloc[1]


class TestNormalizeDates:
    def test_object_dtype_datetime_date_converted(self):
        """F13 regression: Flight delivers DATE as object-dtype
        ``datetime.date`` — must become datetime64 before Timestamp
        ops work."""
        df = pd.DataFrame(
            {
                "date": pd.Series(
                    [date(2026, 9, 17), date(2026, 9, 18)], dtype=object
                ),
                "fg_score": [40.0, 42.5],
            }
        )
        assert df["date"].dtype == object
        out = fgd.normalize_dates(df)
        assert pd.api.types.is_datetime64_any_dtype(out["date"])
        # .dt accessors and DateOffset math work post-normalize.
        assert out["date"].dt.strftime("%Y-%m-%d").iloc[0] == "2026-09-17"
        # Input frame is not mutated.
        assert df["date"].dtype == object

    def test_already_datetime_is_idempotent(self):
        df = _fg_df(5)
        out = fgd.normalize_dates(df)
        assert pd.api.types.is_datetime64_any_dtype(out["date"])
        assert len(out) == 5


class TestFilterWindow:
    def test_max_is_passthrough(self):
        df = _fg_df(100)
        out = fgd.filter_window(df, "Max")
        assert len(out) == len(df)
        assert out["date"].min() == df["date"].min()

    def test_month_and_year_offsets(self):
        df = _fg_df(600)
        max_date = df["date"].max()
        expected = {
            "1M": max_date - pd.DateOffset(months=1),
            "3M": max_date - pd.DateOffset(months=3),
            "6M": max_date - pd.DateOffset(months=6),
            "1Y": max_date - pd.DateOffset(years=1),
            "2Y": max_date - pd.DateOffset(years=2),
        }
        for preset, cutoff in expected.items():
            out = fgd.filter_window(df, preset)
            assert out["date"].min() >= cutoff, preset
            assert out["date"].max() == max_date, preset
            # The row just before the cutoff is excluded.
            assert (df["date"] < cutoff).any() or len(out) == len(df)

    def test_windows_are_ordered_by_size(self):
        df = _fg_df(600)
        sizes = {
            p: len(fgd.filter_window(df, p))
            for p in ("1M", "3M", "6M", "1Y", "2Y", "Max")
        }
        assert (
            sizes["1M"]
            <= sizes["3M"]
            <= sizes["6M"]
            <= sizes["1Y"]
            <= sizes["2Y"]
            <= sizes["Max"]
        )
        assert sizes["1M"] < sizes["Max"]

    def test_ytd_anchors_to_latest_year_in_data(self):
        """YTD = Jan 1 of the latest year present IN THE DATA — not
        the calendar year (a stale table must not go empty)."""
        df = _fg_df(300, end="2026-09-18")
        assert df["date"].max().year == 2026
        out = fgd.filter_window(df, "YTD")
        assert out["date"].min() >= pd.Timestamp("2026-01-01")
        assert out["date"].max() == df["date"].max()
        # Rows from 2025 are excluded.
        assert (out["date"].dt.year == 2026).all()
        assert (df["date"].dt.year < 2026).any()

    def test_ytd_boundary_row_included(self):
        """A row exactly on Jan 1 of the latest data year survives."""
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(
                    ["2025-12-31", "2026-01-01", "2026-01-02"]
                ),
                "fg_score": [10.0, 20.0, 30.0],
            }
        )
        out = fgd.filter_window(df, "YTD")
        assert list(out["fg_score"]) == [20.0, 30.0]

    def test_unknown_preset_rejected(self):
        with pytest.raises(ValueError):
            fgd.filter_window(_fg_df(10), "5Y")

    def test_empty_frame_passthrough(self):
        df = _fg_df(10).iloc[0:0]
        for preset in fgd.WINDOW_PRESETS:
            out = fgd.filter_window(df, preset)
            assert out.empty


class TestRatingOrdinal:
    def test_all_five_ratings(self):
        assert fgd.rating_ordinal("extreme fear") == 0
        assert fgd.rating_ordinal("fear") == 1
        assert fgd.rating_ordinal("neutral") == 2
        assert fgd.rating_ordinal("greed") == 3
        assert fgd.rating_ordinal("extreme greed") == 4

    def test_unknown_and_null_map_to_gap(self):
        assert fgd.rating_ordinal(None) is None
        assert fgd.rating_ordinal(float("nan")) is None
        assert fgd.rating_ordinal(pd.NA) is None
        assert fgd.rating_ordinal("extreme bullish") is None
        assert fgd.rating_ordinal("") is None


class TestBuildGaugeFigure:
    def test_gauge_structure(self):
        pytest.importorskip("plotly")
        fig = fgd.build_gauge_figure(42.5, 40.0, "fear")
        assert len(fig.data) == 1
        ind = fig.data[0]
        assert "gauge" in ind.mode
        assert "number" in ind.mode
        assert "delta" in ind.mode
        assert ind.value == 42.5
        # Delta vs the previous trading day.
        assert ind.delta.reference == 40.0
        # Axis fixed to the 0-100 domain.
        assert list(ind.gauge.axis.range) == [0, 100]
        # Five band steps in palette order at the band bounds.
        assert len(ind.gauge.steps) == 5
        for i, step in enumerate(ind.gauge.steps):
            assert list(step.range) == [
                fgd.BAND_BOUNDS[i],
                fgd.BAND_BOUNDS[i + 1],
            ]
            assert step.color == fgd.RATING_COLORS[i]
        # Rating label surfaces in the title.
        assert "Fear" in ind.title.text

    def test_gauge_handles_missing_rating(self):
        pytest.importorskip("plotly")
        fig = fgd.build_gauge_figure(50.0, 50.0, None)
        assert fig.data[0].value == 50.0
        assert "Fear & Greed" in fig.data[0].title.text


class TestBuildHistoryFigure:
    def test_bands_line_and_axes(self):
        pytest.importorskip("plotly")
        df = _fg_df(120)
        fig = fgd.build_history_figure(df, show_ma=False)

        # One score trace (no MA traces by default).
        assert len(fig.data) == 1
        line = fig.data[0]
        assert line.fill == "tozeroy"
        # Category x axis of YYYY-MM-DD strings — trading days only.
        assert all(isinstance(v, str) for v in line.x)
        assert line.x[0] == df["date"].iloc[0].strftime("%Y-%m-%d")
        assert fig.layout.xaxis.type == "category"
        assert list(fig.layout.yaxis.range) == [0, 100]
        # Hover carries date | score | rating (rating via customdata).
        assert "%{customdata}" in line.hovertemplate
        assert "%{x}" in line.hovertemplate
        assert "%{y" in line.hovertemplate

        # Five semi-transparent hrect band shapes, palette-ordered.
        shapes = list(fig.layout.shapes)
        assert len(shapes) == 5
        for i, shape in enumerate(shapes):
            assert shape.y0 == fgd.BAND_BOUNDS[i]
            assert shape.y1 == fgd.BAND_BOUNDS[i + 1]
            assert shape.fillcolor == fgd.RATING_COLORS[i]
            assert shape.opacity == pytest.approx(0.12)

    def test_ma_toggle_adds_two_traces(self):
        pytest.importorskip("plotly")
        df = _fg_df(120)
        fig_off = fgd.build_history_figure(df, show_ma=False)
        fig_on = fgd.build_history_figure(df, show_ma=True)
        assert len(fig_off.data) == 1
        assert len(fig_on.data) == 3
        names = [t.name for t in fig_on.data]
        assert "MA20" in names and "MA50" in names
        # MA traces share the same category-date x axis.
        assert list(fig_on.data[1].x) == list(fig_on.data[0].x)


class TestBuildHeatmapFigure:
    def test_ordinal_matrix_labels_and_axis(self):
        pytest.importorskip("plotly")
        df = _fg_df(40)
        # Known ratings: strength=extreme fear(0), breadth=greed(3).
        df["stock_price_strength_rating"] = "extreme fear"
        df["stock_price_breadth_rating"] = "greed"
        # Gaps: NULL rating + out-of-vocabulary rating.
        df.loc[df.index[0], "put_call_options_rating"] = None
        df.loc[df.index[1], "put_call_options_rating"] = "bogus"

        fig = fgd.build_heatmap_figure(df)
        assert len(fig.data) == 1
        hm = fig.data[0]
        # z = 9 indicator rows x 40 trading-day columns.
        assert len(hm.z) == 9
        assert len(hm.z[0]) == 40
        assert list(hm.y) == [label for _, label in fgd.INDICATORS]
        # Ordinals: every strength cell 0, every breadth cell 3, all
        # neutral cells 2.
        assert all(v == 0 for v in hm.z[0])
        assert all(v == 3 for v in hm.z[1])
        assert all(v == 2 for v in hm.z[2])
        # NULL/unknown -> gap (plotly may surface the passed None as
        # nan once any other cell forces a float ndarray — isna
        # covers both representations).
        assert pd.isna(hm.z[6][0])
        assert pd.isna(hm.z[6][1])
        assert hm.z[6][2] == 2
        # Discrete colorscale anchored to z 0-4 with the palette.
        assert hm.zmin == 0 and hm.zmax == 4
        cs_colors = [entry[1] for entry in hm.colorscale]
        assert len(hm.colorscale) == 10
        for color in fgd.RATING_COLORS:
            assert cs_colors.count(color) == 2
        # Category x axis of date strings — no weekend gaps.
        assert all(isinstance(v, str) for v in hm.x)
        assert fig.layout.xaxis.type == "category"

    def test_hover_customdata_carries_value_and_rating(self):
        pytest.importorskip("plotly")
        df = _fg_df(10)
        df["junk_bond_demand"] = [1.5] * 10
        df["junk_bond_demand_rating"] = ["greed"] * 10

        fig = fgd.build_heatmap_figure(df)
        hm = fig.data[0]
        # customdata[0] = raw metric value, customdata[1] = rating.
        row_idx = [k for k, _ in fgd.INDICATORS].index("junk_bond_demand")
        cell = hm.customdata[row_idx][0]
        assert cell[0] == pytest.approx(1.5)
        assert cell[1] == "greed"
        assert "%{customdata[0]}" in hm.hovertemplate
        assert "%{customdata[1]}" in hm.hovertemplate

    def test_missing_columns_render_as_gaps(self):
        """A frame lacking an indicator column pair must not crash —
        its row renders as all-gap."""
        pytest.importorskip("plotly")
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2026-09-17", "2026-09-18"]),
                "fg_score": [40.0, 42.5],
                "fg_rating": ["fear", "fear"],
            }
        )
        fig = fgd.build_heatmap_figure(df)
        hm = fig.data[0]
        assert len(hm.z) == 9
        assert all(pd.isna(v) for row in hm.z for v in row)
