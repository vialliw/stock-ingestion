import os
import boto3
import pandas as pd
from src.database import StockDatabase
from src.fetcher import fetch_stock_data, load_config

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
    print(f"No existing DB found on R2: {e}")


def upload_updated_db():
  print("Uploading updated DuckDB file to Cloudflare R2...")
  s3 = get_s3_client()
  s3.upload_file(DB_FILE, BUCKET_NAME, "market_data.duckdb")
  print("Upload complete!")


if __name__ == "__main__":
  # Load repository config
  config = load_config("config.json")
  tickers = config.get("tickers", [])
  start_year = config.get("start_year", 2020)
  start_date = f"{start_year}-01-01"

  print(
      f"Configured tickers: {tickers} | Ingestion start date:"
      f" {start_date}"
  )

  download_existing_db()

  # Fetch historical dataset
  prices_df = fetch_stock_data(tickers, start_date=start_date)

  if not prices_df.empty:
    db = StockDatabase(DB_FILE)
    rows_added = db.upsert_prices(prices_df)
    print(f"Processed {rows_added} rows into DuckDB.")
    db.close()
    upload_updated_db()
  else:
    print("No new price data retrieved.")
