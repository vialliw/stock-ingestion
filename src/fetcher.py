from datetime import datetime
import json
import os
import pandas as pd
import yfinance as yf


def load_config(config_path: str = "config.json") -> dict:
  """Load configuration settings from config.json."""
  if not os.path.exists(config_path):
    raise FileNotFoundError(f"Configuration file {config_path} not found.")

  with open(config_path, "r") as f:
    return json.load(f)


def fetch_stock_data(
    tickers: list[str], start_date: str, end_date: str = None
) -> pd.DataFrame:
  """Fetch daily price data for tickers from start_date up to end_date."""
  if not end_date:
    end_date = datetime.now().strftime("%Y-%m-%d")

  df = yf.download(
      tickers,
      start=start_date,
      end=end_date,
      group_by="ticker",
      progress=False,
  )

  if df.empty:
    return pd.DataFrame()

  records = []
  if len(tickers) == 1:
    ticker = tickers[0]
    data = df.copy().reset_index()
    data["ticker"] = ticker
    records.append(data)
  else:
    for ticker in tickers:
      if ticker in df.columns.levels[0]:
        data = df[ticker].copy().reset_index()
        data["ticker"] = ticker
        records.append(data)

  if not records:
    return pd.DataFrame()

  result = pd.concat(records, ignore_index=True)
  result.columns = [c.lower().replace(" ", "_") for c in result.columns]

  required = ["date", "ticker", "open", "high", "low", "close", "volume"]
  result = result.dropna(subset=["close"])
  result["volume"] = result["volume"].fillna(0).astype("int64")
  return result[required]
