import os
import boto3
import duckdb
import pandas as pd
import yfinance as yf
from src.database import StockDatabase

R2_ENDPOINT = os.getenv("R2_ENDPOINT_URL")
R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
BUCKET_NAME = "market-data-bucket"
DB_FILE = "db/market_data.duckdb"


def get_s3_client():
  return boto3.client(
      "s3",
      endpoint_url=R2_ENDPOINT,
      aws_access_key_id=R2_ACCESS_KEY,
      aws_secret_access_key=R2_SECRET_KEY,
  )


def download_existing_db():
  os.makedirs("db", exist_ok=True)
  s3 = get_s3_client()
  try:
    s3.download_file(BUCKET_NAME, "market_data.duckdb", DB_FILE)
    print("Downloaded existing database from Cloudflare R2.")
  except Exception as e:
    print(f"No existing DB found on R2 (or initial run): {e}")


def upload_updated_db():
  print("Uploading updated DuckDB file to Cloudflare R2...")
  s3 = get_s3_client()
  s3.upload_file(DB_FILE, BUCKET_NAME, "market_data.duckdb")
  print("Upload complete!")


def fetch_stock_data(tickers: list[str]) -> pd.DataFrame:
  """Fetch daily price data for a list of tickers via yfinance."""
  df = yf.download(tickers, period="5d", group_by="ticker", progress=False)
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


if __name__ == "__main__":
  download_existing_db()

  tickers = ["AAPL", "MSFT", "NVDA"]
  print(f"Fetching data for {tickers}...")
  prices_df = fetch_stock_data(tickers)

  db = StockDatabase(DB_FILE)
  rows_added = db.upsert_prices(prices_df)
  print(f"Successfully processed {rows_added} rows into DuckDB.")
  db.close()

  upload_updated_db()
