import os
import json
import pytest
from src.fetcher import load_config, fetch_stock_data

def test_load_config(tmp_path):
    config_data = {"start_year": 2020, "tickers": ["AAPL", "MSFT"]}
    config_file = tmp_path / "config.json"
    config_file.write_text(json.dumps(config_data))

    loaded = load_config(str(config_file))
    assert loaded["start_year"] == 2020
    assert "AAPL" in loaded["tickers"]

def test_fetch_stock_data_columns():
    # Fetch a small date slice to verify standardized output
    df = fetch_stock_data(["AAPL"], start_date="2024-01-02", end_date="2024-01-05")
    assert not df.empty
    expected_cols = ["date", "ticker", "open", "high", "low", "close", "volume"]
    assert list(df.columns) == expected_cols
