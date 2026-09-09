"""CI daily load script for GitHub Actions.

Downloads the existing DuckDB file from R2 (if it exists), runs the daily
load (which upserts new data and auto-syncs the updated file back to R2),
and prints a summary.

This script is designed to run in a GitHub Actions runner. It:
1. Overrides config paths for the Linux runner environment.
2. Enables R2 sync and sets the bucket name from env vars.
3. Downloads the existing DuckDB from R2 (so the DB persists across runs).
4. Runs IngestionEngine.run_daily_load() which upserts new data and
   auto-syncs the updated DuckDB back to R2.

Required environment variables:
    R2_ACCESS_KEY_ID     — R2 access key (from GitHub repo secret)
    R2_SECRET_ACCESS_KEY — R2 secret key (from GitHub repo secret)
    R2_ENDPOINT_URL      — R2 S3-compatible endpoint (from GitHub repo secret)
    R2_BUCKET_NAME       — R2 bucket name (from GitHub repo secret)

Optional environment variables:
    DB_DIR               — DuckDB directory (default: /tmp/duckdb)
    DB_FILENAME          — DuckDB filename (default: stock_ingestion.duckdb)
    LOG_FILE             — Log file path (default: /tmp/stock_ingestion.log)
    LOG_FORMAT           — Log format: text or json (default: text)
"""

from __future__ import annotations

import logging
import os
import sys

import boto3
from botocore.exceptions import ClientError

# Ensure src/ is importable when run as a script.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config import PipelineConfig
from src.db import DatabaseManager
from src.ingestion import IngestionEngine

logger = logging.getLogger("ci_daily_load")

_TEXT_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"


def _configure_logging(log_file: str, log_format: str) -> None:
    """Configure root logging: console + file."""
    log_dir = os.path.dirname(log_file)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)

    formatter = logging.Formatter(_TEXT_FORMAT)
    root = logging.getLogger()
    root.setLevel(logging.INFO)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    root.addHandler(stream_handler)

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)


def _download_duckdb_from_r2(
    db_path: str,
    bucket: str,
    key: str,
    access_key: str,
    secret_key: str,
    endpoint_url: str,
) -> bool:
    """Download the DuckDB file from R2 if it exists.

    Returns True if the file was downloaded, False if it doesn't exist
    (first run or fresh start).
    """
    client = boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        endpoint_url=endpoint_url,
        region_name="auto",
    )

    try:
        client.head_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "404":
            logger.info(
                "No existing DuckDB file in R2 at %s/%s — starting fresh.",
                bucket,
                key,
            )
            return False
        raise

    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    client.download_file(bucket, key, db_path)
    logger.info("Downloaded DuckDB from R2: %s/%s -> %s", bucket, key, db_path)
    return True


def _upload_duckdb_to_r2(
    db_path: str,
    bucket: str,
    key: str,
    access_key: str,
    secret_key: str,
    endpoint_url: str,
) -> None:
    """Upload the DuckDB file to R2."""
    client = boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        endpoint_url=endpoint_url,
        region_name="auto",
    )
    client.upload_file(db_path, bucket, key)
    logger.info("Uploaded DuckDB to R2: %s/%s", bucket, key)


def main() -> None:
    # --- Validate required environment variables ---
    required_env = [
        "R2_ACCESS_KEY_ID",
        "R2_SECRET_ACCESS_KEY",
        "R2_ENDPOINT_URL",
        "R2_BUCKET_NAME",
    ]
    missing = [name for name in required_env if not os.environ.get(name)]
    if missing:
        print(f"ERROR: Missing required environment variables: {', '.join(missing)}")
        sys.exit(1)

    r2_access_key = os.environ["R2_ACCESS_KEY_ID"]
    r2_secret_key = os.environ["R2_SECRET_ACCESS_KEY"]
    r2_endpoint = os.environ["R2_ENDPOINT_URL"]
    r2_bucket = os.environ["R2_BUCKET_NAME"]

    # --- Override config for CI environment ---
    db_dir = os.environ.get("DB_DIR", "/tmp/duckdb")
    db_filename = os.environ.get("DB_FILENAME", "stock_ingestion.duckdb")
    log_file = os.environ.get("LOG_FILE", "/tmp/stock_ingestion.log")
    log_format = os.environ.get("LOG_FORMAT", "text")
    r2_prefix = os.environ.get("R2_PREFIX", "stock-ingestion/")

    _configure_logging(log_file, log_format)

    # --- Load config and override for CI ---
    config = PipelineConfig.from_yaml("src/config.yaml")
    config.database.duckdb_dir = db_dir
    config.database.duckdb_filename = db_filename
    config.logging.log_file = log_file
    config.logging.log_format = log_format
    config.r2.enabled = True
    config.r2.bucket_name = r2_bucket
    config.r2.prefix = r2_prefix

    db_path = config.db_path
    r2_key = f"{r2_prefix}{db_filename}"
    if not r2_key.startswith(r2_prefix):
        r2_key = r2_prefix + db_filename

    logger.info("CI daily load starting.")
    logger.info("DB path: %s", db_path)
    logger.info("R2 bucket: %s, key: %s", r2_bucket, r2_key)

    # --- Download existing DuckDB from R2 ---
    downloaded = _download_duckdb_from_r2(
        db_path,
        r2_bucket,
        r2_key,
        r2_access_key,
        r2_secret_key,
        r2_endpoint,
    )

    if downloaded:
        logger.info("Existing DuckDB downloaded — will upsert new data.")
    else:
        logger.info("No existing DuckDB — first run or fresh start.")

    # --- Run daily load ---
    db_manager = DatabaseManager(db_path)
    ingest_engine = IngestionEngine(db_manager, config)

    try:
        result = ingest_engine.run_daily_load()
        logger.info("Daily load result: %s", result)

        status = result.get("status", "UNKNOWN")
        if status == "SKIPPED":
            logger.info("Market closed — daily load skipped. No R2 upload needed.")
        elif status in ("SUCCESS", "PARTIAL"):
            # run_daily_load already auto-syncs to R2 when r2.enabled is True,
            # but we do a final explicit upload to be sure.
            _upload_duckdb_to_r2(
                db_path,
                r2_bucket,
                r2_key,
                r2_access_key,
                r2_secret_key,
                r2_endpoint,
            )
            logger.info("DuckDB uploaded to R2 after daily load.")
        else:
            logger.warning("Daily load returned unexpected status: %s", status)

        # Print summary for GitHub Actions log
        print("\n=== Daily Load Summary ===")
        print(f"Status: {status}")
        print(f"Rows upserted: {result.get('rows_upserted', 0)}")
        print(f"Tickers succeeded: {len(result.get('tickers_succeeded', []))}")
        print(f"Tickers failed: {len(result.get('tickers_failed', []))}")
        if result.get("errors"):
            print(f"Errors: {result['errors']}")
        r2_sync = result.get("r2_sync", {})
        if r2_sync:
            print(f"R2 sync: {r2_sync}")
        print("===========================\n")

    finally:
        db_manager.close()
        logger.info("CI daily load complete.")


if __name__ == "__main__":
    main()
