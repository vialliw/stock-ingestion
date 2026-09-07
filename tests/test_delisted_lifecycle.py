"""Verify the delisted-ticker lifecycle scenario:
  Day 1-20:  XYZ downloads fine, data stored
  Day 21-25: XYZ delisted (empty from yfinance), NOT in exclude -> old data intact
  Day 26:    User adds XYZ to exclude_tickers -> program purges old records
"""
import logging
from unittest.mock import patch, MagicMock

import numpy as np
import pandas as pd
import pytest

from src.config import PipelineConfig
from src.db import DatabaseManager
from src.ingestion import IngestionEngine

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")


def make_download_df(tickers, dates):
    """Build a multi-ticker yf.download-style DataFrame."""
    cols = pd.MultiIndex.from_product([tickers, ["Open", "High", "Low", "Close", "Volume"]])
    idx = pd.to_datetime(dates)
    idx.name = "Date"
    data = np.random.uniform(90, 110, size=(len(idx), len(cols)))
    return pd.DataFrame(data, index=idx, columns=cols)


def make_empty_download_df(tickers, dates):
    """Build an all-NaN yf.download-style DataFrame (simulates delisted)."""
    cols = pd.MultiIndex.from_product([tickers, ["Open", "High", "Low", "Close", "Volume"]])
    idx = pd.to_datetime(dates)
    idx.name = "Date"
    data = np.full((len(idx), len(cols)), np.nan)
    return pd.DataFrame(data, index=idx, columns=cols)


def make_mock_ticker_no_splits():
    mock = MagicMock()
    mock.splits = pd.Series([], dtype=float, name="splits")
    return mock


def test_delisted_ticker_lifecycle(tmp_path):
    """Phase 1: download OK. Phase 2: delisted, not excluded -> data intact.
    Phase 3: added to exclude_tickers -> data purged."""

    db_path = str(tmp_path / "lifecycle.duckdb")
    db = DatabaseManager(db_path)

    # --- Phase 1: Days 1-20 — XYZ downloads successfully ---
    cfg = PipelineConfig()
    cfg.ingestion.tickers = ["XYZ"]
    cfg.ingestion.exclude_tickers = []
    engine = IngestionEngine(db, cfg)

    dates_d1_20 = pd.bdate_range("2024-01-01", "2024-01-26")  # 20 trading days
    good_df = make_download_df(["XYZ"], list(dates_d1_20.strftime("%Y-%m-%d")))

    mock_ticker = make_mock_ticker_no_splits()
    with patch("src.ingestion.yf.download", return_value=good_df), \
         patch("src.ingestion.yf.Ticker", return_value=mock_ticker):
        df, succeeded, failed, errors = engine.fetch_and_clean(
            ["XYZ"], "2024-01-01", "2024-01-26"
        )
        rows = db.upsert_dataframe(df)

    print(f"\n[Phase 1] rows_upserted={rows}, succeeded={succeeded}, failed={failed}")
    assert rows == 20
    assert "XYZ" in succeeded

    # Verify XYZ data is in DB
    table = db.execute_query("SELECT COUNT(*) as c FROM daily_quotes WHERE Ticker='XYZ'")
    count = table.to_pandas().iloc[0]["c"]
    print(f"[Phase 1] XYZ rows in DB: {count}")
    assert count == 20

    # --- Phase 2: Days 21-25 — XYZ delisted, NOT in exclude_tickers ---
    empty_df = make_empty_download_df(["XYZ"], ["2024-01-29", "2024-01-30", "2024-01-31", "2024-02-01", "2024-02-02"])

    with patch("src.ingestion.yf.download", return_value=empty_df), \
         patch("src.ingestion.yf.Ticker", return_value=mock_ticker):
        df2, succeeded2, failed2, errors2 = engine.fetch_and_clean(
            ["XYZ"], "2024-01-29", "2024-02-02"
        )
        rows2 = db.upsert_dataframe(df2)

    print(f"\n[Phase 2] rows_upserted={rows2}, succeeded={succeeded2}, failed={failed2}, errors={errors2}")
    assert "XYZ" in failed2
    assert errors2.get("XYZ") == "Empty dataset"

    # Verify XYZ data is STILL in DB (not purged — not in exclude_tickers)
    table2 = db.execute_query("SELECT COUNT(*) as c FROM daily_quotes WHERE Ticker='XYZ'")
    count2 = table2.to_pandas().iloc[0]["c"]
    print(f"[Phase 2] XYZ rows in DB after delisted download: {count2}")
    assert count2 == 20, f"Expected 20 rows intact, got {count2}"

    # --- Phase 3: Day 26 — User adds XYZ to exclude_tickers ---
    cfg.ingestion.exclude_tickers = ["XYZ"]
    # Recreate engine with updated config
    engine = IngestionEngine(db, cfg)

    with patch("src.ingestion.yf.download", return_value=good_df) as mock_dl, \
         patch("src.ingestion.yf.Ticker", return_value=mock_ticker):
        df3, succeeded3, failed3, errors3 = engine.fetch_and_clean(
            ["XYZ"], "2024-02-05", "2024-02-09"
        )

    # Verify XYZ was NOT downloaded (excluded at earliest stage)
    print(f"\n[Phase 3] succeeded={succeeded3}, failed={failed3}")
    assert "XYZ" not in succeeded3
    assert "XYZ" not in failed3  # excluded tickers are filtered, not failed

    # KEY CHECK: Was XYZ purged from the DB?
    table3 = db.execute_query("SELECT COUNT(*) as c FROM daily_quotes WHERE Ticker='XYZ'")
    count3 = table3.to_pandas().iloc[0]["c"]
    print(f"[Phase 3] XYZ rows in DB after exclude: {count3}")

    if count3 == 0:
        print("\n[RESULT] PASS — XYZ was purged after being added to exclude_tickers")
    else:
        print(f"\n[RESULT] FAIL — XYZ still has {count3} rows in DB after being added to exclude_tickers")
        print("           The current code filters excluded tickers at the earliest stage")
        print("           but never purges existing data for newly-excluded tickers.")

    db.close()
    # This assert will fail with current code — that's the point
    assert count3 == 0, f"Expected XYZ purged (0 rows), got {count3} — exclude_tickers should purge existing data"
