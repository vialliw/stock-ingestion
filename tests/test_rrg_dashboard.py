"""Tests for ``scripts/rrg_dashboard.py`` — the Relative Rotation
Graph (RRG) dashboard port of the legacy ``plot_rrg_chart_gemini10b.py``.

Exercises the verbatim presets, the SQL builder (safe quoted join,
ticker validation, lookback window), the pivot+ffill, the RRG math
(rs / rs_ratio / rs_momentum / valid_mask / dynamic limit), the
<3-point spline passthrough, the 4-row animated figure (trace/frame
counts, quadrant shapes, slider/play-pause), the ``gen_ai_prompt``
JSON payload, and the ``generate_rrg_dashboard`` / ``main()``
orchestration with a monkeypatched fetch — no Flight server or real
database is touched.  ``plotly``/``scipy`` are imported inside the
test functions that need them (the module lazy-imports them, so
collection does not require them either).
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import pytest

# Add scripts dir to path so rrg_dashboard / report_app can be
# imported (same pattern as tests/test_breadth_dashboard.py).
SCRIPTS_DIR = Path(__file__).parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

import rrg_dashboard  # noqa: E402
import report_app  # noqa: E402

from src.config import PipelineConfig  # noqa: E402


# ------------------------------------------------------------------
# Fixtures / helpers
# ------------------------------------------------------------------


def _config(tmp_path) -> PipelineConfig:
    """Config rooted at ``tmp_path`` (nothing touches the real
    C:\\Intel\\parquet)."""
    return PipelineConfig(
        database={
            "duckdb_dir": str(tmp_path),
            "duckdb_filename": "test.duckdb",
        },
        logging={"log_file": str(tmp_path / "logs" / "run.log")},
        reports={"report_dir": str(tmp_path / "reports")},
    )


def _weekly_long_df(n: int = 60) -> pd.DataFrame:
    """Long-format (Date, Ticker, Close) frame over ``n`` weekly
    (Friday) dates ending today — the shape ``build_rrg_sql``
    returns.  Tickers cover the ``sector3`` preset partially: XLK and
    DIA are sector3 members (the other five are absent -> the
    missing-ticker drop path), SPY is its default benchmark, and AAA
    is a spare usable as a benchmark override."""
    dates = pd.date_range(
        end=pd.Timestamp.today().normalize(), periods=n, freq="W-FRI"
    )
    i = np.arange(n)
    closes = {
        "XLK": 100.0 * (1.03 ** i),
        "DIA": 100.0 * np.exp(0.01 * i + 0.001 * i * i),
        "AAA": 100.0 * (1.02 ** i),
        "SPY": np.full(n, 100.0),
    }
    recs = [
        {"Date": d, "Ticker": t, "Close": c}
        for t, series in closes.items()
        for d, c in zip(dates, series)
    ]
    return pd.DataFrame.from_records(recs)


def _pivot(n: int = 30) -> pd.DataFrame:
    """Wide close matrix: AAA = 2x benchmark (constant RS), BBB rising
    geometrically, CCC accelerating, SPY flat."""
    dates = pd.date_range("2026-01-02", periods=n, freq="W-FRI")
    i = np.arange(n)
    return pd.DataFrame(
        {
            "SPY": np.full(n, 100.0),
            "AAA": np.full(n, 200.0),
            "BBB": 100.0 * (1.03 ** i),
            "CCC": 100.0 * np.exp(0.01 * i + 0.001 * i * i),
        },
        index=dates,
    )


def _seed_weekly_close(db_path: str, rows: list) -> None:
    """Create a fresh DuckDB file with a ``weekly_close`` table holding
    ``rows`` of (date, ticker, open, high, low, close, volume)."""
    conn = duckdb.connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE weekly_close (
                Date DATE,
                Ticker VARCHAR,
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
            "INSERT INTO weekly_close VALUES (?, ?, ?, ?, ?, ?, ?)", rows
        )
    finally:
        conn.close()


# ------------------------------------------------------------------
# RRG_PRESETS — the six legacy __main__ configs verbatim
# ------------------------------------------------------------------


class TestPresets:
    def test_six_presets_verbatim(self):
        presets = rrg_dashboard.RRG_PRESETS
        assert list(presets) == [
            "ETFs",
            "sector1",
            "sector2",
            "sector3",
            "Semi",
            "AI",
        ]
        # (benchmark, lookback_years, prefix, ticker count) — the
        # counts are the true sizes of the verbatim legacy ticker
        # strings.
        expected = {
            "ETFs": ("SPY", 1, "Relative_Rotation_Dashboard_ETFs", 85),
            "sector1": (
                "SPY",
                3,
                "Relative_Rotation_Dashboard_sector1",
                17,
            ),
            "sector2": (
                "SGOV",
                3,
                "Relative_Rotation_Dashboard_sector2",
                17,
            ),
            "sector3": (
                "SPY",
                3,
                "Relative_Rotation_Dashboard_sector3",
                7,
            ),
            "Semi": ("SMH", 3, "Relative_Rotation_Dashboard_Semi", 35),
            "AI": ("XLK", 3, "Relative_Rotation_Dashboard_AI", 41),
        }
        for name, (bench, years, prefix, count) in expected.items():
            p = presets[name]
            assert p["benchmark"] == bench
            assert p["lookback_years"] == years
            assert p["period"] == 14
            assert p["trail"] == 3
            assert p["prefix"] == prefix
            assert len(p["tickers"].split(",")) == count

    def test_preset_ticker_strings_verbatim(self):
        presets = rrg_dashboard.RRG_PRESETS
        assert (
            presets["sector3"]["tickers"] == "XLK,IWM,DIA,TLT,GLD,SLV,SGOV"
        )
        assert presets["sector1"]["tickers"].endswith(",CIBR")
        assert presets["sector2"]["tickers"].endswith(",SPY")
        assert presets["Semi"]["tickers"].startswith("ADI,ALAB,")
        assert presets["Semi"]["tickers"].endswith(",TSM,TXN")
        assert presets["AI"]["tickers"].startswith("XLK,NVDA,TSM,")
        assert presets["AI"]["tickers"].endswith(",HUT,CLSK")
        assert presets["ETFs"]["tickers"].startswith("AMLP,ARKK,AWAY,")
        assert presets["ETFs"]["tickers"].endswith(",XSW,XTN")


# ------------------------------------------------------------------
# build_rrg_sql — legacy query semantics, safe quoted IN list
# ------------------------------------------------------------------


class TestBuildRrgSql:
    def test_query_shape_and_quoted_join(self):
        sql = rrg_dashboard.build_rrg_sql(["AAA", "BBB"], "SPY", 3)
        assert 'SELECT Date, Ticker, "Close" FROM weekly_close' in sql
        # Quoted join — NOT the legacy tuple repr ('AAA', 'BBB').
        assert "Ticker IN ('AAA','BBB','SPY')" in sql
        assert "'AAA', 'BBB'" not in sql
        assert "ORDER BY Date ASC" in sql
        expected = (datetime.today() - timedelta(days=3 * 365)).strftime(
            "%Y-%m-%d"
        )
        assert f"Date >= '{expected}'" in sql

    def test_single_ticker_not_tuple_repr(self):
        """The legacy ``tuple(all_t)`` produced the fragile
        ``('X',)`` singleton — the join never does."""
        sql = rrg_dashboard.build_rrg_sql(["AAA"], "SPY", 1)
        assert "IN ('AAA','SPY')" in sql
        assert "('AAA',)" not in sql

    def test_benchmark_appended_even_when_already_listed(self):
        """Legacy ``all_t = tickers + [benchmark]`` keeps a duplicate
        (harmless inside IN)."""
        sql = rrg_dashboard.build_rrg_sql(["XLK", "NVDA"], "XLK", 3)
        assert "IN ('XLK','NVDA','XLK')" in sql

    def test_invalid_tickers_rejected(self):
        # Anything outside ^[A-Z0-9.\-]+$ — including quote-breakout
        # attempts and lowercase — is rejected before reaching SQL.
        for bad in ("AA'AA", "AAA;DROP", "A A", "aapl", "", "AA,BB"):
            with pytest.raises(ValueError):
                rrg_dashboard.build_rrg_sql([bad], "SPY", 1)
        # The benchmark is validated too (it joins the IN list).
        with pytest.raises(ValueError):
            rrg_dashboard.build_rrg_sql(["AAA"], "SP Y", 1)

    def test_lookback_years_validated(self):
        with pytest.raises((TypeError, ValueError)):
            rrg_dashboard.build_rrg_sql(["AAA"], "SPY", 0)
        with pytest.raises((TypeError, ValueError)):
            rrg_dashboard.build_rrg_sql(["AAA"], "SPY", True)
        with pytest.raises((TypeError, ValueError)):
            rrg_dashboard.build_rrg_sql(["AAA"], "SPY", "3")

    def test_generated_sql_executes_on_duckdb(self, tmp_path):
        """The built query is valid DuckDB SQL against a seeded
        weekly_close table (run on a read-only connection, like the
        production fallback path)."""
        db_path = str(tmp_path / "test.duckdb")
        dates = pd.date_range(
            end=pd.Timestamp.today().normalize(), periods=30, freq="W-FRI"
        )
        rows = []
        # justified: deterministic synthetic fixture rows.
        for i, d in enumerate(dates):
            for ticker, close in (
                ("T1", 100.0 + i),
                ("T2", 200.0 + 2 * i),
                ("SPY", 50.0),
            ):
                rows.append((d.date(), ticker, 1.0, 2.0, 0.5, close, 10.0))
        _seed_weekly_close(db_path, rows)

        conn = duckdb.connect(db_path, read_only=True)
        try:
            df = conn.execute(
                rrg_dashboard.build_rrg_sql(["T1", "T2"], "SPY", 1)
            ).df()
        finally:
            conn.close()

        assert list(df.columns) == ["Date", "Ticker", "Close"]
        assert set(df["Ticker"]) == {"T1", "T2", "SPY"}
        assert len(df) == 30 * 3
        # ORDER BY Date ASC
        assert list(df["Date"]) == sorted(df["Date"])


# ------------------------------------------------------------------
# pivot_closes — pivot + ffill verbatim
# ------------------------------------------------------------------


class TestPivotCloses:
    def test_pivot_and_ffill(self):
        d0, d1, d2 = pd.to_datetime(
            ["2026-01-02", "2026-01-09", "2026-01-16"]
        )
        df = pd.DataFrame(
            [
                (d0, "AAA", 10.0),
                (d1, "AAA", 11.0),
                (d2, "AAA", 12.0),
                (d0, "SPY", 100.0),
                # SPY missing d1 -> NaN -> ffill carries 100.0.
                (d2, "SPY", 102.0),
            ],
            columns=["Date", "Ticker", "Close"],
        )
        p = rrg_dashboard.pivot_closes(df)
        assert list(p.columns) == ["AAA", "SPY"]
        assert list(p.index) == [d0, d1, d2]
        assert p.loc[d1, "SPY"] == 100.0  # ffilled
        assert p.loc[d2, "SPY"] == 102.0


# ------------------------------------------------------------------
# compute_rrg — verbatim RRG math
# ------------------------------------------------------------------


class TestComputeRrg:
    def test_constant_rs_gives_100(self):
        res = rrg_dashboard.compute_rrg(_pivot(), ["AAA"], "SPY", 2)
        assert res["clean_ratio"]["AAA"].tolist() == pytest.approx(
            [100.0] * len(res["clean_ratio"])
        )
        assert res["clean_momo"]["AAA"].tolist() == pytest.approx(
            [100.0] * len(res["clean_momo"])
        )

    def test_rising_rs_gives_above_100(self):
        res = rrg_dashboard.compute_rrg(_pivot(), ["BBB"], "SPY", 2)
        assert (res["clean_ratio"]["BBB"] > 100).all()

    def test_accelerating_rs_gives_momentum_above_100(self):
        res = rrg_dashboard.compute_rrg(_pivot(), ["CCC"], "SPY", 2)
        assert (res["clean_ratio"]["CCC"] > 100).all()
        assert (res["clean_momo"]["CCC"] > 100).all()

    def test_valid_mask_drops_warmup_rows(self):
        """With ``period`` the earliest rows are NaN in
        rs_ratio/rs_momentum — the valid_mask drops exactly the
        ``2*period + 4`` warmup rows (pct_change(period) + rolling(3)
        applied twice)."""
        n = 30
        res = rrg_dashboard.compute_rrg(_pivot(n), ["AAA"], "SPY", 2)
        assert len(res["clean_ratio"]) == n - (2 * 2 + 4)
        assert not res["clean_ratio"].isna().any().any()
        assert not res["clean_momo"].isna().any().any()
        # The mask itself marks the first 8 rows invalid.
        assert res["valid_mask"].tolist()[:8] == [False] * 8
        assert res["valid_mask"].tolist()[8:] == [True] * (n - 8)
        assert len(res["dates"]) == n - 8

    def test_missing_ticker_dropped_with_warning(self, capsys):
        res = rrg_dashboard.compute_rrg(
            _pivot(), ["AAA", "GHOST"], "SPY", 2
        )
        assert res["tickers"] == ["AAA"]
        assert list(res["clean_ratio"].columns) == ["AAA"]
        assert "GHOST" in capsys.readouterr().out

    def test_missing_benchmark_raises_value_error(self):
        with pytest.raises(ValueError, match="Benchmark"):
            rrg_dashboard.compute_rrg(
                _pivot().drop(columns=["SPY"]), ["AAA"], "SPY", 2
            )

    def test_all_tickers_missing_raises_value_error(self):
        with pytest.raises(ValueError, match="None of the requested"):
            rrg_dashboard.compute_rrg(
                _pivot(), ["GHOST1", "GHOST2"], "SPY", 2
            )

    def test_limit_symmetric_around_100(self):
        res = rrg_dashboard.compute_rrg(_pivot(), ["BBB"], "SPY", 2)
        lo, hi = res["limit"]
        assert lo < 100 < hi
        assert (100 - lo) == pytest.approx(hi - 100)


# ------------------------------------------------------------------
# get_spline — <3-point passthrough, 25-point resample
# ------------------------------------------------------------------


class TestGetSpline:
    def test_under_three_points_pass_through(self):
        x = np.array([1.0, 2.0])
        y = np.array([3.0, 4.0])
        xs, ys = rrg_dashboard.get_spline(x, y)
        assert list(xs) == [1.0, 2.0]
        assert list(ys) == [3.0, 4.0]

    def test_nan_points_filtered_then_passthrough(self):
        x = np.array([1.0, np.nan, 2.0])
        y = np.array([3.0, 4.0, np.nan])
        xs, ys = rrg_dashboard.get_spline(x, y)
        # Only (1.0, 3.0) is finite on both axes -> <3 -> passthrough.
        assert list(xs) == [1.0]
        assert list(ys) == [3.0]

    def test_three_or_more_resamples_to_25(self):
        pytest.importorskip("scipy")
        x = np.array([1.0, 2.0, 3.0, 4.0])
        y = np.array([1.0, 4.0, 2.0, 5.0])
        xs, ys = rrg_dashboard.get_spline(x, y)
        assert len(xs) == 25
        assert len(ys) == 25
        # Quadratic spline stays inside the input hull.
        assert xs.min() >= x.min() - 1
        assert xs.max() <= x.max() + 1


# ------------------------------------------------------------------
# build_rrg_figure — 4-row animated figure
# ------------------------------------------------------------------


class TestBuildRrgFigure:
    def _figure(self, n_tickers=2, n=30, trail=3):
        p = _pivot(n)
        tickers = ["AAA", "BBB"][:n_tickers]
        res = rrg_dashboard.compute_rrg(p, tickers, "SPY", 2)
        fig = rrg_dashboard.build_rrg_figure(
            res["clean_ratio"],
            res["clean_momo"],
            p,
            res["tickers"],
            "SPY",
            trail,
        )
        return fig, res, trail

    def test_trace_count_two_n_plus_four(self):
        """n line trails + n diamond heads + 2 benchmark bars + stats
        table + guide table = 2n + 4 base traces."""
        fig, res, _ = self._figure(n_tickers=2)
        assert len(fig.data) == 2 * 2 + 4
        names = [t.name for t in fig.data]
        assert names[:2] == ["AAA", "BBB"]  # legend on line traces

    def test_frame_count_is_dates_minus_trail(self):
        fig, res, trail = self._figure(n_tickers=2, n=30, trail=3)
        assert len(fig.frames) == len(res["dates"]) - trail
        # Each frame carries one entry per base trace.
        assert all(
            len(f.data) == len(fig.data) for f in fig.frames
        )

    def test_quadrant_shapes_and_annotations(self):
        fig, _, _ = self._figure()
        rects = [s for s in fig.layout.shapes if s.type == "rect"]
        assert len(rects) == 4
        texts = {a.text for a in fig.layout.annotations}
        assert texts == {"LEADING", "WEAKENING", "LAGGING", "IMPROVING"}

    def test_layout_slider_and_buttons(self):
        fig, res, trail = self._figure()
        assert fig.layout.width == 1500
        assert fig.layout.height == 1500
        # Slider steps match frames; last frame active.
        slider = fig.layout.sliders[0]
        assert len(slider.steps) == len(res["dates"]) - trail
        assert slider.active == len(fig.frames) - 1
        # Play / pause updatemenu.
        labels = [b.label for b in fig.layout.updatemenus[0].buttons]
        assert labels == ["▶ Play", "⏸ Pause"]


# ------------------------------------------------------------------
# gen_ai_prompt — verbatim JSON payload
# ------------------------------------------------------------------


class TestGenAiPrompt:
    def test_json_parses_with_expected_structure(self):
        p = _pivot()
        res = rrg_dashboard.compute_rrg(p, ["AAA", "BBB"], "SPY", 2)
        payload = json.loads(
            rrg_dashboard.gen_ai_prompt(
                res["clean_ratio"], res["clean_momo"], "SPY"
            )
        )
        assert set(payload) == {
            "system_prompt",
            "analysis_query",
            "instructions",
            "market_snapshot",
        }
        snap = payload["market_snapshot"]
        assert snap["benchmark"] == "SPY"
        assert snap["lookback_weeks"] == 12
        assert set(snap["ticker_data"]) == {"AAA", "BBB"}
        for entry in snap["ticker_data"].values():
            assert set(entry) == {
                "current_position",
                "trajectory",
                "persistence",
            }
            assert entry["current_position"]["quadrant"] in {
                "LEADING",
                "WEAKENING",
                "LAGGING",
                "IMPROVING",
            }
        # AAA (constant RS -> 100/100) counts as LEADING (>= 100).
        assert (
            snap["ticker_data"]["AAA"]["current_position"]["quadrant"]
            == "LEADING"
        )

    def test_all_four_quadrant_labels(self):
        """Hand-built clean frames landing one ticker in each
        quadrant verify the >=/else boundaries."""
        idx = pd.date_range("2026-01-02", periods=15, freq="W-FRI")
        clean_ratio = pd.DataFrame(
            {
                "LEAD": [101.0] * 15,
                "WEAK": [101.0] * 15,
                "LAG": [99.0] * 15,
                "IMP": [99.0] * 15,
            },
            index=idx,
        )
        clean_momo = pd.DataFrame(
            {
                "LEAD": [101.0] * 15,
                "WEAK": [99.0] * 15,
                "LAG": [99.0] * 15,
                "IMP": [101.0] * 15,
            },
            index=idx,
        )
        payload = json.loads(
            rrg_dashboard.gen_ai_prompt(clean_ratio, clean_momo, "BENCH")
        )
        quads = {
            t: e["current_position"]["quadrant"]
            for t, e in payload["market_snapshot"]["ticker_data"].items()
        }
        assert quads == {
            "LEAD": "LEADING",
            "WEAK": "WEAKENING",
            "LAG": "LAGGING",
            "IMP": "IMPROVING",
        }
        assert payload["market_snapshot"]["benchmark"] == "BENCH"


# ------------------------------------------------------------------
# generate_rrg_dashboard — monkeypatched fetch, file output
# ------------------------------------------------------------------


class TestGenerateRrgDashboard:
    def test_writes_html_and_json_artifacts(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            report_app,
            "fetch_dataframe",
            lambda sql, config: _weekly_long_df(),
        )
        config = _config(tmp_path)
        out = rrg_dashboard.generate_rrg_dashboard(config, "sector3")
        html_path = Path(out["html"])
        json_path = Path(out["json"])
        assert (
            html_path.name == "Relative_Rotation_Dashboard_sector3.html"
        )
        assert (
            json_path.name
            == "Relative_Rotation_Dashboard_sector3_analysis_prompt.json"
        )
        assert html_path.parent == Path(config.reports.report_dir)
        assert json_path.parent == Path(config.reports.report_dir)
        html = html_path.read_text(encoding="utf-8")
        assert "Relative Rotation" in html
        assert "cdn.plot.ly" in html or "plotly" in html.lower()
        payload = json.loads(json_path.read_text(encoding="utf-8"))
        assert payload["market_snapshot"]["benchmark"] == "SPY"
        # Only the two sector3 members present in the fixture survive
        # the missing-ticker drop.
        assert set(payload["market_snapshot"]["ticker_data"]) == {
            "XLK",
            "DIA",
        }

    def test_output_dir_override(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            report_app,
            "fetch_dataframe",
            lambda sql, config: _weekly_long_df(),
        )
        config = _config(tmp_path)
        target = tmp_path / "elsewhere"
        out = rrg_dashboard.generate_rrg_dashboard(
            config, "sector3", output_dir=str(target)
        )
        assert Path(out["html"]).parent == target
        assert (
            target / "Relative_Rotation_Dashboard_sector3.html"
        ).exists()
        assert (
            target
            / "Relative_Rotation_Dashboard_sector3_analysis_prompt.json"
        ).exists()

    def test_preset_defaults_and_overrides(self, tmp_path, monkeypatch):
        """Overrides flow into the SQL (benchmark + lookback window)
        and the figure (trail)."""
        captured = {}

        def fake_fetch(sql, config):
            captured["sql"] = sql
            return _weekly_long_df()

        monkeypatch.setattr(
            report_app, "fetch_dataframe", fake_fetch
        )
        config = _config(tmp_path)
        rrg_dashboard.generate_rrg_dashboard(
            config,
            "sector3",
            lookback_years=2,
            benchmark="AAA",
            output_dir=str(tmp_path / "out"),
        )
        sql = captured["sql"]
        # Benchmark override is appended to the IN list.
        assert sql.endswith("ORDER BY Date ASC")
        expected = (datetime.today() - timedelta(days=2 * 365)).strftime(
            "%Y-%m-%d"
        )
        assert f"Date >= '{expected}'" in sql
        assert "'AAA'" in sql  # override benchmark joins the IN list

    def test_fetch_none_raises_runtime_error(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            report_app, "fetch_dataframe", lambda sql, config: None
        )
        config = _config(tmp_path)
        with pytest.raises(RuntimeError, match="weekly_close"):
            rrg_dashboard.generate_rrg_dashboard(config, "sector3")

    def test_empty_df_raises_value_error(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            report_app,
            "fetch_dataframe",
            lambda sql, config: pd.DataFrame(
                columns=["Date", "Ticker", "Close"]
            ),
        )
        config = _config(tmp_path)
        with pytest.raises(ValueError, match="No weekly_close data"):
            rrg_dashboard.generate_rrg_dashboard(config, "sector3")

    def test_unknown_preset_raises_value_error(self, tmp_path):
        config = _config(tmp_path)
        with pytest.raises(ValueError, match="Unknown RRG preset"):
            rrg_dashboard.generate_rrg_dashboard(config, "nope")

    def test_missing_benchmark_raises_value_error(
        self, tmp_path, monkeypatch
    ):
        """A benchmark absent from the fetched data -> ValueError
        (compute_rrg guard), not a KeyError."""
        df = _weekly_long_df()
        df = df[df["Ticker"] != "SPY"]  # drop the benchmark rows
        monkeypatch.setattr(
            report_app, "fetch_dataframe", lambda sql, config: df
        )
        config = _config(tmp_path)
        with pytest.raises(ValueError, match="Benchmark"):
            rrg_dashboard.generate_rrg_dashboard(config, "sector3")


# ------------------------------------------------------------------
# main() — preset batch, per-preset failure isolation, exit codes
# ------------------------------------------------------------------


class TestMain:
    @pytest.fixture
    def _patched(self, tmp_path, monkeypatch):
        """Config load + generate stubbed; returns the call log."""
        config = _config(tmp_path)
        monkeypatch.setattr(
            rrg_dashboard.PipelineConfig,
            "from_yaml",
            classmethod(lambda cls, path=None: config),
        )
        calls = []

        def fake_generate(cfg, name, **overrides):
            calls.append((name, overrides))
            return {"html": f"{name}.html", "json": f"{name}.json"}

        monkeypatch.setattr(
            rrg_dashboard, "generate_rrg_dashboard", fake_generate
        )
        return calls

    def test_no_dashboard_runs_all_six(self, _patched):
        calls = _patched
        assert rrg_dashboard.main([]) == 0
        assert [c[0] for c in calls] == list(rrg_dashboard.RRG_PRESETS)

    def test_single_dashboard(self, _patched):
        calls = _patched
        assert rrg_dashboard.main(["--dashboard", "sector3"]) == 0
        assert [c[0] for c in calls] == ["sector3"]

    def test_overrides_forwarded(self, _patched):
        calls = _patched
        assert (
            rrg_dashboard.main(
                [
                    "--dashboard",
                    "AI",
                    "--period",
                    "10",
                    "--trail",
                    "5",
                    "--years",
                    "2",
                    "--benchmark",
                    "MSFT",
                ]
            )
            == 0
        )
        assert calls[0][1] == {
            "period": 10,
            "trail": 5,
            "lookback_years": 2,
            "benchmark": "MSFT",
        }

    def test_per_preset_failure_continues(self, _patched, monkeypatch):
        calls = _patched

        def flaky(cfg, name, **overrides):
            calls.append(name)
            if name == "sector1":
                raise RuntimeError("boom")
            return {"html": "", "json": ""}

        monkeypatch.setattr(
            rrg_dashboard, "generate_rrg_dashboard", flaky
        )
        assert rrg_dashboard.main([]) == 0
        # All six attempted despite the sector1 failure.
        assert calls == list(rrg_dashboard.RRG_PRESETS)

    def test_all_fail_returns_1(self, _patched, monkeypatch):
        def always_fail(cfg, name, **overrides):
            raise RuntimeError("boom")

        monkeypatch.setattr(
            rrg_dashboard, "generate_rrg_dashboard", always_fail
        )
        assert rrg_dashboard.main([]) == 1
        assert rrg_dashboard.main(["--dashboard", "sector3"]) == 1

    def test_unknown_dashboard_returns_1(self, _patched):
        calls = _patched
        assert rrg_dashboard.main(["--dashboard", "nope"]) == 1
        assert calls == []

    def test_config_load_failure_returns_1(
        self, tmp_path, monkeypatch
    ):
        def _boom(cls, path=None):
            raise RuntimeError("bad yaml")

        monkeypatch.setattr(
            rrg_dashboard.PipelineConfig,
            "from_yaml",
            classmethod(_boom),
        )
        assert rrg_dashboard.main([]) == 1


# ------------------------------------------------------------------
# export_rotation_summary — ported but deliberately never invoked
# ------------------------------------------------------------------


def test_export_rotation_summary_not_called_by_generate(
    tmp_path, monkeypatch
):
    """Parity with the legacy file: the export call is commented out
    upstream, so generate_rrg_dashboard must not invoke it (no .txt
    artifact is produced)."""
    monkeypatch.setattr(
        report_app,
        "fetch_dataframe",
        lambda sql, config: _weekly_long_df(),
    )
    called = []
    monkeypatch.setattr(
        rrg_dashboard,
        "export_rotation_summary",
        lambda *a, **k: called.append(1),
    )
    out = rrg_dashboard.generate_rrg_dashboard(
        _config(tmp_path), "sector3", output_dir=str(tmp_path)
    )
    assert called == []
    assert not list(Path(out["html"]).parent.glob("*.txt"))
