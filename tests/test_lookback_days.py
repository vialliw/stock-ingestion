"""Critic tests for the ``daily_lookback_days`` parameter (F7-T8c).

Covers the executor production modules only:
  - src.config.IngestionConfig.daily_lookback_days (default 4, ge=1)
  - src.ingestion.IngestionEngine.run_daily_load(lookback_days)
    validation + window width passed to fetch_and_clean
  - src/scheduler.py and scripts/ci_daily_load.py hard-code lookback_days=1
  - src.flight_server do_action(daily_load) forwards cfg value
"""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from pydantic import ValidationError

from src.config import IngestionConfig, PipelineConfig
from src.db import DatabaseManager
from src.ingestion import IngestionEngine


@pytest.fixture
def engine(tmp_path):
    cfg = PipelineConfig()
    cfg.ingestion.tickers = ["AAPL"]
    db = DatabaseManager(str(tmp_path / "lb.duckdb"))
    try:
        yield IngestionEngine(db, cfg)
    finally:
        db.close()


def test_config_default_is_4():
    assert IngestionConfig().daily_lookback_days == 4
    assert PipelineConfig.from_yaml("src/config.yaml").ingestion.daily_lookback_days == 4


@pytest.mark.parametrize("bad", [0, -1, "abc", 2.5])
def test_config_rejects_invalid_values(bad):
    with pytest.raises(ValidationError):
        IngestionConfig(daily_lookback_days=bad)


@pytest.mark.parametrize("bad", [0, -3, 1.5, "2", None, True])
def test_run_daily_load_rejects_invalid_lookback(engine, bad):
    with pytest.raises(ValueError, match="lookback_days"):
        engine.run_daily_load(lookback_days=bad)


@pytest.mark.parametrize("days", [1, 4, 10])
def test_run_daily_load_window_width(engine, days):
    mock_cal = MagicMock()
    mock_cal.schedule.return_value = pd.DataFrame({"x": [1]})
    engine.db.detect_new_tickers = MagicMock(return_value=[])
    engine.fetch_and_clean = MagicMock(return_value=(pd.DataFrame(), ["AAPL"], [], {}))
    engine.db.upsert_dataframe = MagicMock(return_value=0)
    engine.db.record_ingestion_run = MagicMock()
    with patch("src.ingestion.mcal.get_calendar", return_value=mock_cal):
        engine.run_daily_load(lookback_days=days)
    _, start, end = engine.fetch_and_clean.call_args[0]
    delta = datetime.strptime(end, "%Y-%m-%d") - datetime.strptime(start, "%Y-%m-%d")
    assert delta.days == days


def test_scheduler_and_ci_use_lookback_1():
    for path in ("src/scheduler.py", "scripts/ci_daily_load.py"):
        assert "run_daily_load(lookback_days=1)" in open(path).read(), path


def test_flight_server_forwards_cfg_lookback():
    src = open("src/flight_server.py").read()
    assert "lookback_days=self.cfg.ingestion.daily_lookback_days" in src
