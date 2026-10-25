"""Tests for the ``scripts/rrg_dashboard.py`` backdate support.

Covers ``parse_as_of`` (None/str/date/datetime-reject/future-reject/
malformed), the ``build_rrg_sql`` inclusive ``Date <=`` cutoff bound
with the lookback measured backwards from ``as_of`` (and the unchanged
default query), the defensive in-memory window bound in
``generate_rrg_dashboard`` (a fetcher ignoring the SQL predicates
cannot leak post-cutoff rows), the <12-valid-week guard (which fires
only when a cutoff is set), the ``{prefix}_{YYYY-MM-DD}`` date-stamped
artifact names, ``--json-only`` (no HTML written, ``html: None``), the
``requested_as_of_date`` / ``historical_reconstruction`` payload
fields, and ``main()``'s ``--as-of``/``--date``/``--json-only``
passthrough plus bad-date ``parser.error``.

Adopted from
``docs/enhancements/rrg_dashboard_backdate_bundle/tests/test_rrg_dashboard_backdate.py``
and adapted to the repo conventions in ``tests/test_rrg_dashboard.py``
— real ``rrg_dashboard``/``report_app`` imports (no ``sys.modules``
stubbing) and the ``PipelineConfig`` ``_config(tmp_path)`` fixture.
The fetch layer is monkeypatched; no Flight server or real database is
touched.  ``plotly`` is required by the two real-render tests.
"""

from __future__ import annotations

import json
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

# Add scripts dir to path so rrg_dashboard / report_app can be
# imported (same pattern as tests/test_rrg_dashboard.py).
SCRIPTS_DIR = Path(__file__).parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

import rrg_dashboard  # noqa: E402
import report_app  # noqa: E402

from src.config import PipelineConfig  # noqa: E402


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


@pytest.fixture
def synthetic_bars():
    """Synthetic Fridays with later prices deliberately very different."""
    idx = pd.date_range("2022-12-30", periods=145, freq="W-FRI")
    ticker_names = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA"]
    records = []
    for n, d in enumerate(idx):
        baseline = 100 + n * 0.5
        records.append((d, "SPY", baseline))
        for j, t in enumerate(ticker_names):
            ratio = 1.05 + j * 0.07 + 0.1 * np.sin(n / (5 + j))
            # The large post-cutoff jump makes a leakage bug easy to
            # detect.
            if d > pd.Timestamp("2024-06-19"):
                ratio *= 2.0
            records.append((d, t, float(baseline * ratio)))
    return pd.DataFrame(records, columns=["Date", "Ticker", "Close"])


def fake_fetch_ignoring_upper_limit(monkeypatch, data, observed_queries):
    """Patch ``report_app.fetch_dataframe`` to return ``data``
    verbatim — simulating a fetch layer that ignores BOTH SQL date
    predicates.  ``generate_rrg_dashboard`` must apply its own
    matching historical window defensively."""

    def fetch(sql, cfg):
        observed_queries.append(sql)
        return data.copy()

    monkeypatch.setattr(report_app, "fetch_dataframe", fetch)


# ------------------------------------------------------------------
# parse_as_of — accepted/rejected input matrix
# ------------------------------------------------------------------


class TestParseAsOf:
    def test_none_and_valid_inputs(self):
        assert rrg_dashboard.parse_as_of(None) is None
        assert rrg_dashboard.parse_as_of("2024-06-19") == date(2024, 6, 19)
        assert rrg_dashboard.parse_as_of(date(2024, 6, 19)) == date(
            2024, 6, 19
        )

    @pytest.mark.parametrize(
        "invalid",
        [
            "2024-02-30",
            "2024-6-19",
            "19/06/2024",
            "not-a-date",
            "2999-01-01",
            True,
        ],
    )
    def test_invalid_dates_fail(self, invalid):
        with pytest.raises(ValueError):
            rrg_dashboard.parse_as_of(invalid)

    def test_datetime_rejected(self):
        """A ``datetime`` is a ``date`` subclass — the strict
        calendar-date contract must still reject it."""
        with pytest.raises(ValueError):
            rrg_dashboard.parse_as_of(datetime(2024, 6, 19, 12, 0))

    def test_future_rejected(self):
        with pytest.raises(ValueError, match="future"):
            rrg_dashboard.parse_as_of(date.today() + timedelta(days=30))


# ------------------------------------------------------------------
# build_rrg_sql — cutoff bound + lookback anchored at as_of
# ------------------------------------------------------------------


class TestBuildRrgSqlCutoff:
    def test_builder_cutoff_and_legacy_default(self):
        query = rrg_dashboard.build_rrg_sql(["XLK"], "SPY", 1, "2024-06-19")
        # Lookback measured backwards from the cutoff: 2024-06-19 is
        # past the 2024 leap day, so 365 days back lands on 2023-06-20.
        assert "Date >= '2023-06-20'" in query
        assert "Date <= '2024-06-19'" in query
        assert "'XLK','SPY'" in query
        # Default latest-data query is unchanged — no upper bound.
        assert "Date <=" not in rrg_dashboard.build_rrg_sql(
            ["XLK"], "SPY", 1
        )

    def test_cutoff_accepts_date_object(self):
        query = rrg_dashboard.build_rrg_sql(
            ["XLK"], "SPY", 1, date(2024, 6, 19)
        )
        assert "Date <= '2024-06-19'" in query

    def test_cutoff_validated_like_tickers(self):
        with pytest.raises(ValueError):
            rrg_dashboard.build_rrg_sql(["XLK"], "SPY", 1, "not-a-date")


# ------------------------------------------------------------------
# generate_rrg_dashboard — filenames, payload fields, guards
# ------------------------------------------------------------------


class TestHistoricalGenerate:
    def test_historical_json_cutoff_and_filename(
        self, monkeypatch, tmp_path, synthetic_bars
    ):
        queries = []
        fake_fetch_ignoring_upper_limit(monkeypatch, synthetic_bars, queries)
        # HTML must not be constructed in JSON-only mode.
        monkeypatch.setattr(
            rrg_dashboard,
            "build_rrg_figure",
            lambda *a, **k: pytest.fail("figure called"),
        )
        cfg = _config(tmp_path)
        result = rrg_dashboard.generate_rrg_dashboard(
            cfg, "Mag7", lookback_years=1, as_of="2024-06-19", json_only=True
        )
        assert result["html"] is None
        assert result["json"].endswith("Mag7_2024-06-19_analysis_prompt.json")
        assert "Date <= '2024-06-19'" in queries[0]
        report_dir = Path(cfg.reports.report_dir)
        assert not list(report_dir.glob("*.html"))

        payload = json.loads(Path(result["json"]).read_text(encoding="utf-8"))
        snap = payload["market_snapshot"]
        assert snap["requested_as_of_date"] == "2024-06-19"
        assert snap["analysis_date"] == "2024-06-14"  # Wednesday -> prior Friday
        assert snap["historical_reconstruction"] is True

        start = date(2024, 6, 19) - timedelta(days=365)
        filtered = synthetic_bars.loc[
            (synthetic_bars["Date"] >= pd.Timestamp(start))
            & (synthetic_bars["Date"] <= pd.Timestamp("2024-06-19"))
        ]
        expected_rrg = rrg_dashboard.compute_rrg(
            rrg_dashboard.pivot_closes(filtered),
            rrg_dashboard.RRG_PRESETS["Mag7"]["tickers"].split(","),
            "SPY",
            14,
        )
        expected = json.loads(
            rrg_dashboard.gen_ai_prompt(
                expected_rrg["clean_ratio"], expected_rrg["clean_momo"], "SPY"
            )
        )
        assert snap["ticker_data"] == expected["market_snapshot"]["ticker_data"]

    def test_later_changes_do_not_change_historical_json(
        self, monkeypatch, tmp_path, synthetic_bars
    ):
        """Post-cutoff rows mutated by 500x cannot reach the output —
        the defensive in-memory bound holds even when the fetcher
        ignores the SQL predicates."""
        queries = []
        fake_fetch_ignoring_upper_limit(monkeypatch, synthetic_bars, queries)
        cfg = _config(tmp_path)
        kwargs = dict(lookback_years=1, as_of="2024-06-19", json_only=True)
        before = rrg_dashboard.generate_rrg_dashboard(cfg, "Mag7", **kwargs)
        bytes_before = Path(before["json"]).read_bytes()
        changed = synthetic_bars.copy()
        changed.loc[
            changed["Date"] > pd.Timestamp("2024-06-19"), "Close"
        ] *= 500
        fake_fetch_ignoring_upper_limit(monkeypatch, changed, queries)
        after = rrg_dashboard.generate_rrg_dashboard(cfg, "Mag7", **kwargs)
        assert bytes_before == Path(after["json"]).read_bytes()

    def test_week_before_warmup_fails_without_creating_output(
        self, monkeypatch, tmp_path, synthetic_bars
    ):
        queries = []
        fake_fetch_ignoring_upper_limit(monkeypatch, synthetic_bars, queries)
        cfg = _config(tmp_path)
        with pytest.raises(ValueError, match="Not enough complete RRG weeks"):
            rrg_dashboard.generate_rrg_dashboard(
                cfg, "Mag7", as_of="2023-03-01", json_only=True
            )
        report_dir = Path(cfg.reports.report_dir)
        assert not report_dir.exists() or not list(report_dir.iterdir())

    def test_less_than_12_valid_weeks_is_rejected(
        self, monkeypatch, tmp_path, synthetic_bars
    ):
        queries = []
        fake_fetch_ignoring_upper_limit(monkeypatch, synthetic_bars, queries)
        cfg = _config(tmp_path)
        with pytest.raises(ValueError, match="12 are needed"):
            rrg_dashboard.generate_rrg_dashboard(
                cfg, "Mag7", as_of="2023-09-22", json_only=True
            )
        report_dir = Path(cfg.reports.report_dir)
        assert not report_dir.exists() or not list(report_dir.iterdir())

    def test_under_12_weeks_without_cutoff_is_allowed(
        self, monkeypatch, tmp_path
    ):
        """The 12-valid-week guard fires ONLY when a cutoff is set —
        the latest-data path keeps the legacy behaviour (only the
        3-week compute_rrg minimum applies)."""
        idx = pd.date_range("2026-06-05", periods=40, freq="W-FRI")
        records = []
        for n, d in enumerate(idx):
            records.append((d, "SPY", 100.0))
            for j, t in enumerate(("XLK", "DIA")):
                records.append((d, t, float(100.0 * (1.03 + 0.02 * j) ** n)))
        df = pd.DataFrame(records, columns=["Date", "Ticker", "Close"])
        monkeypatch.setattr(
            report_app, "fetch_dataframe", lambda sql, cfg: df
        )
        cfg = _config(tmp_path)
        # 40 - (2*14+4) = 8 valid weeks < 12 — succeeds without cutoff.
        result = rrg_dashboard.generate_rrg_dashboard(
            cfg, "sector3", json_only=True
        )
        assert Path(result["json"]).exists()
        assert result["html"] is None

    def test_current_default_output_keeps_original_filename_and_shape(
        self, monkeypatch, tmp_path, synthetic_bars
    ):
        # Deliberately bypass SQL in this mock, as no up-to-date DB
        # is provided.
        monkeypatch.setattr(
            report_app,
            "fetch_dataframe",
            lambda sql, cfg: synthetic_bars,
        )
        cfg = _config(tmp_path)
        result = rrg_dashboard.generate_rrg_dashboard(
            cfg, "Mag7", json_only=True
        )
        assert (
            Path(result["json"]).name
            == "Relative_Rotation_Dashboard_Mag7_analysis_prompt.json"
        )
        snapshot = json.loads(Path(result["json"]).read_text())[
            "market_snapshot"
        ]
        assert "requested_as_of_date" not in snapshot
        assert "historical_reconstruction" not in snapshot

    def test_historical_html_and_json_have_same_date_suffix(
        self, monkeypatch, tmp_path, synthetic_bars
    ):
        queries = []
        fake_fetch_ignoring_upper_limit(monkeypatch, synthetic_bars, queries)
        fig = SimpleNamespace(
            write_html=lambda path, **kwargs: Path(path).write_text("HTML")
        )
        monkeypatch.setattr(
            rrg_dashboard, "build_rrg_figure", lambda *args: fig
        )
        cfg = _config(tmp_path)
        result = rrg_dashboard.generate_rrg_dashboard(
            cfg, "Mag7", lookback_years=1, as_of="2024-06-19"
        )
        assert Path(result["html"]).name.endswith("Mag7_2024-06-19.html")
        assert Path(result["html"]).exists()
        assert Path(result["json"]).exists()

    def test_real_historical_html_render(
        self, monkeypatch, tmp_path, synthetic_bars
    ):
        # Exercises Plotly's actual figure builder, not just the
        # orchestration.
        pytest.importorskip("plotly")
        pytest.importorskip("scipy")
        queries = []
        fake_fetch_ignoring_upper_limit(monkeypatch, synthetic_bars, queries)
        cfg = _config(tmp_path)
        result = rrg_dashboard.generate_rrg_dashboard(
            cfg, "Mag7", lookback_years=1, as_of="2024-06-19"
        )
        html = Path(result["html"])
        assert html.exists() and html.stat().st_size > 10_000
        assert Path(result["json"]).exists()


# ------------------------------------------------------------------
# main() — --as-of/--date/--json-only passthrough, bad-date error
# ------------------------------------------------------------------


class TestMainBackdate:
    def test_cli_date_alias_and_json_only_forwarded(
        self, tmp_path, monkeypatch
    ):
        captured = []
        monkeypatch.setattr(
            rrg_dashboard.PipelineConfig,
            "from_yaml",
            classmethod(lambda cls, path=None: _config(tmp_path)),
        )

        def stub_generate(config, preset_name, **kwargs):
            captured.append((preset_name, kwargs))
            return {"html": None, "json": "/tmp/test.json"}

        monkeypatch.setattr(
            rrg_dashboard, "generate_rrg_dashboard", stub_generate
        )
        assert (
            rrg_dashboard.main(
                ["--dashboard", "Mag7", "--date", "2024-06-19", "--json-only"]
            )
            == 0
        )
        assert captured[0][0] == "Mag7"
        assert captured[0][1]["as_of"] == date(2024, 6, 19)
        assert captured[0][1]["json_only"] is True

    def test_cli_as_of_long_flag_forwarded(self, tmp_path, monkeypatch):
        captured = []
        monkeypatch.setattr(
            rrg_dashboard.PipelineConfig,
            "from_yaml",
            classmethod(lambda cls, path=None: _config(tmp_path)),
        )
        monkeypatch.setattr(
            rrg_dashboard,
            "generate_rrg_dashboard",
            lambda config, preset_name, **kwargs: captured.append(kwargs)
            or {"html": "h", "json": "j"},
        )
        assert (
            rrg_dashboard.main(
                ["--dashboard", "sector3", "--as-of", "2024-06-19"]
            )
            == 0
        )
        assert captured[0]["as_of"] == date(2024, 6, 19)
        assert captured[0]["json_only"] is False

    def test_cli_bad_date_exits_2(self, monkeypatch):
        """``parser.error`` on an unparseable --as-of exits 2 before
        any config load or generation."""
        monkeypatch.setattr(
            rrg_dashboard.PipelineConfig,
            "from_yaml",
            classmethod(
                lambda cls, path=None: pytest.fail("config load reached")
            ),
        )
        with pytest.raises(SystemExit) as excinfo:
            rrg_dashboard.main(["--as-of", "not-a-date"])
        assert excinfo.value.code == 2
