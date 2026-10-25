"""Self-contained tests for the rrg_dashboard backdate patch.

Run: python -m pytest -q test_rrg_dashboard_backdate.py
The production repo's report_app/src.config imports are stubbed here because the
user-provided file was uploaded without the rest of the stock-ingestion repo.
"""

import importlib.util
import json
import re
import sys
import types
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest


@pytest.fixture
def rrg(monkeypatch):
    fake_report_app = types.ModuleType("report_app")
    fake_report_app.fetch_dataframe = lambda sql, config: None
    fake_src = types.ModuleType("src")
    fake_src.__path__ = []
    fake_config = types.ModuleType("src.config")
    fake_config.PipelineConfig = type(
        "PipelineConfig", (), {"from_yaml": staticmethod(lambda path: None)}
    )
    monkeypatch.setitem(sys.modules, "report_app", fake_report_app)
    monkeypatch.setitem(sys.modules, "src", fake_src)
    monkeypatch.setitem(sys.modules, "src.config", fake_config)

    module_path = Path(__file__).with_name("rrg_dashboard.py")
    if not module_path.exists():
        module_path = Path(__file__).parent.parent / "scripts" / "rrg_dashboard.py"
    spec = importlib.util.spec_from_file_location("rrg_dashboard_under_test", module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


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
            ratio = 1.05 + j * .07 + 0.1 * np.sin(n / (5 + j))
            # The large post-cutoff jump makes a leakage bug easy to detect.
            if d > pd.Timestamp("2024-06-19"):
                ratio *= 2.0
            records.append((d, t, float(baseline * ratio)))
    return pd.DataFrame(records, columns=["Date", "Ticker", "Close"])


def fake_fetch_ignoring_upper_limit(rrg, monkeypatch, data, observed_queries):
    def fetch(sql, cfg):
        observed_queries.append(sql)
        # Simulate a fetch layer ignoring BOTH SQL date predicates.
        # The module must apply its own matching historical window.
        return data.copy()

    monkeypatch.setattr(rrg.report_app, "fetch_dataframe", fetch)


def test_builder_cutoff_and_legacy_default(rrg):
    query = rrg.build_rrg_sql(["XLK"], "SPY", 1, "2024-06-19")
    assert "Date >= '2023-06-20'" in query  # legacy 365-day convention
    assert "Date <= '2024-06-19'" in query
    assert "'XLK','SPY'" in query
    assert "Date <=" not in rrg.build_rrg_sql(["XLK"], "SPY", 1)


@pytest.mark.parametrize("invalid", ["2024-02-30", "2024-6-19", "19/06/2024", "not-a-date", "2999-01-01", True])
def test_invalid_dates_fail(rrg, invalid):
    with pytest.raises(ValueError):
        rrg.parse_as_of(invalid)


def test_as_of_accepts_date_object(rrg):
    assert rrg.parse_as_of(date(2024, 6, 19)) == date(2024, 6, 19)


def test_historical_json_cutoff_and_filename(rrg, monkeypatch, tmp_path, synthetic_bars):
    queries = []
    fake_fetch_ignoring_upper_limit(rrg, monkeypatch, synthetic_bars, queries)
    # HTML must not be constructed in JSON-only mode.
    monkeypatch.setattr(rrg, "build_rrg_figure", lambda *a, **k: pytest.fail("figure called"))
    cfg = SimpleNamespace(reports=SimpleNamespace(report_dir=tmp_path))
    result = rrg.generate_rrg_dashboard(cfg, "Mag7", lookback_years=1,
                                        as_of="2024-06-19", json_only=True)
    assert result["html"] is None
    assert result["json"].endswith("Mag7_2024-06-19_analysis_prompt.json")
    assert "Date <= '2024-06-19'" in queries[0]
    assert not list(tmp_path.glob("*.html"))

    payload = json.loads(Path(result["json"]).read_text(encoding="utf-8"))
    snap = payload["market_snapshot"]
    assert snap["requested_as_of_date"] == "2024-06-19"
    assert snap["analysis_date"] == "2024-06-14"  # Wednesday -> prior Friday
    assert snap["historical_reconstruction"] is True

    start = date(2024, 6, 19) - timedelta(days=365)
    filtered = synthetic_bars.loc[
        (synthetic_bars["Date"] >= pd.Timestamp(start)) &
        (synthetic_bars["Date"] <= pd.Timestamp("2024-06-19"))
    ]
    expected_rrg = rrg.compute_rrg(rrg.pivot_closes(filtered),
                                   rrg.RRG_PRESETS["Mag7"]["tickers"].split(","),
                                   "SPY", 14)
    expected = json.loads(rrg.gen_ai_prompt(
        expected_rrg["clean_ratio"], expected_rrg["clean_momo"], "SPY"))
    assert snap["ticker_data"] == expected["market_snapshot"]["ticker_data"]


def test_later_changes_do_not_change_historical_json(rrg, monkeypatch, tmp_path, synthetic_bars):
    queries = []
    fake_fetch_ignoring_upper_limit(rrg, monkeypatch, synthetic_bars, queries)
    cfg = SimpleNamespace(reports=SimpleNamespace(report_dir=tmp_path))
    kwargs = dict(lookback_years=1, as_of="2024-06-19", json_only=True)
    before = rrg.generate_rrg_dashboard(cfg, "Mag7", **kwargs)
    bytes_before = Path(before["json"]).read_bytes()
    changed = synthetic_bars.copy()
    changed.loc[changed["Date"] > pd.Timestamp("2024-06-19"), "Close"] *= 500
    fake_fetch_ignoring_upper_limit(rrg, monkeypatch, changed, queries)
    after = rrg.generate_rrg_dashboard(cfg, "Mag7", **kwargs)
    assert bytes_before == Path(after["json"]).read_bytes()


def test_week_before_warmup_fails_without_creating_output(rrg, monkeypatch, tmp_path, synthetic_bars):
    queries = []
    fake_fetch_ignoring_upper_limit(rrg, monkeypatch, synthetic_bars, queries)
    cfg = SimpleNamespace(reports=SimpleNamespace(report_dir=tmp_path))
    with pytest.raises(ValueError, match="Not enough complete RRG weeks"):
        rrg.generate_rrg_dashboard(cfg, "Mag7", as_of="2023-03-01", json_only=True)
    assert not list(tmp_path.iterdir())


def test_less_than_12_valid_weeks_is_rejected(rrg, monkeypatch, tmp_path, synthetic_bars):
    queries = []
    fake_fetch_ignoring_upper_limit(rrg, monkeypatch, synthetic_bars, queries)
    cfg = SimpleNamespace(reports=SimpleNamespace(report_dir=tmp_path))
    with pytest.raises(ValueError, match="12 are needed"):
        rrg.generate_rrg_dashboard(cfg, "Mag7", as_of="2023-09-22", json_only=True)
    assert not list(tmp_path.iterdir())


def test_current_default_output_keeps_original_filename_and_shape(rrg, monkeypatch, tmp_path, synthetic_bars):
    # Deliberately bypass SQL in this mock, as no up-to-date DB is provided.
    monkeypatch.setattr(rrg.report_app, "fetch_dataframe", lambda sql, cfg: synthetic_bars)
    cfg = SimpleNamespace(reports=SimpleNamespace(report_dir=tmp_path))
    result = rrg.generate_rrg_dashboard(cfg, "Mag7", json_only=True)
    assert Path(result["json"]).name == "Relative_Rotation_Dashboard_Mag7_analysis_prompt.json"
    snapshot = json.loads(Path(result["json"]).read_text())["market_snapshot"]
    assert "requested_as_of_date" not in snapshot
    assert "historical_reconstruction" not in snapshot


def test_historical_html_and_json_have_same_date_suffix(rrg, monkeypatch, tmp_path, synthetic_bars):
    queries = []
    fake_fetch_ignoring_upper_limit(rrg, monkeypatch, synthetic_bars, queries)
    fig = SimpleNamespace(write_html=lambda path, **kwargs: Path(path).write_text("HTML"))
    monkeypatch.setattr(rrg, "build_rrg_figure", lambda *args: fig)
    cfg = SimpleNamespace(reports=SimpleNamespace(report_dir=tmp_path))
    result = rrg.generate_rrg_dashboard(cfg, "Mag7", lookback_years=1,
                                        as_of="2024-06-19")
    assert Path(result["html"]).name.endswith("Mag7_2024-06-19.html")
    assert Path(result["html"]).exists()
    assert Path(result["json"]).exists()


def test_cli_date_alias_and_json_only_forwarded(rrg, monkeypatch):
    captured = []
    monkeypatch.setattr(rrg.PipelineConfig, "from_yaml", lambda path: object())
    def stub_generate(config, preset_name, **kwargs):
        captured.append((preset_name, kwargs))
        return {"html": None, "json": "/tmp/test.json"}
    monkeypatch.setattr(rrg, "generate_rrg_dashboard", stub_generate)
    assert rrg.main(["--dashboard", "Mag7", "--date", "2024-06-19", "--json-only"]) == 0
    assert captured[0][0] == "Mag7"
    assert captured[0][1]["as_of"] == date(2024, 6, 19)
    assert captured[0][1]["json_only"] is True


def test_real_historical_html_render(rrg, monkeypatch, tmp_path, synthetic_bars):
    # Exercises Plotly's actual figure builder, not just the orchestration.
    queries = []
    fake_fetch_ignoring_upper_limit(rrg, monkeypatch, synthetic_bars, queries)
    cfg = SimpleNamespace(reports=SimpleNamespace(report_dir=tmp_path))
    result = rrg.generate_rrg_dashboard(
        cfg, "Mag7", lookback_years=1, as_of="2024-06-19",
    )
    html = Path(result["html"])
    assert html.exists() and html.stat().st_size > 10_000
    assert Path(result["json"]).exists()
