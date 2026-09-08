"""R2 (S3-compatible) sync for the stock-ingestion pipeline.

Uploads the canonical DuckDB file directly to a Cloudflare R2 bucket
using a boto3 S3 client.  Credentials are read from environment
variables only (``R2_ACCESS_KEY_ID``, ``R2_SECRET_ACCESS_KEY``,
``R2_ENDPOINT_URL``); no credentials are stored in code or config.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, Optional

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class R2SyncManager:
    """Syncs the DuckDB database file to an R2 bucket.

    The destination is configured in ``cfg.r2``:
      - ``enabled`` (bool): whether sync is active (default False)
      - ``bucket_name`` (str): target R2 bucket
      - ``prefix`` (str): object key prefix, e.g. ``"stock-ingestion/"``

    The actual upload is delegated to ``boto3.client("s3").upload_file``
    with the configured R2 endpoint and credentials.
    """

    def __init__(self, cfg: Any):
        self.cfg = cfg
        self._client: Optional[Any] = None

    def _r2_client(self) -> Any:
        """Build and cache a boto3 S3 client from environment variables."""
        if self._client is None:
            access_key = os.environ.get("R2_ACCESS_KEY_ID")
            secret_key = os.environ.get("R2_SECRET_ACCESS_KEY")
            endpoint_url = os.environ.get("R2_ENDPOINT_URL")

            missing = [
                name
                for name, value in [
                    ("R2_ACCESS_KEY_ID", access_key),
                    ("R2_SECRET_ACCESS_KEY", secret_key),
                    ("R2_ENDPOINT_URL", endpoint_url),
                ]
                if not value
            ]
            if missing:
                raise RuntimeError(
                    f"Missing R2 credentials: {', '.join(missing)}"
                )

            self._client = boto3.client(
                "s3",
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                endpoint_url=endpoint_url,
                region_name="auto",
            )
        return self._client

    def _build_key(self, db_path: str) -> str:
        """Combine the configured prefix with the DB filename."""
        prefix = self.cfg.r2.prefix
        if prefix and not prefix.endswith("/"):
            prefix += "/"
        return f"{prefix}{os.path.basename(db_path)}"

    def sync(self, db_path: Optional[str] = None) -> Dict[str, Any]:
        """Upload the DuckDB file to R2 if ``cfg.r2.enabled`` is True.

        Returns a dict with ``status`` (SKIPPED/SUCCESS/FAILED) and, on
        success, the destination ``bucket`` and ``key``.  Errors are
        logged and re-raised so callers can decide how to surface them.
        """
        if not self.cfg.r2.enabled:
            logger.info("R2 sync is disabled; skipping upload.")
            return {"status": "SKIPPED", "reason": "R2 sync disabled"}

        if not self.cfg.r2.bucket_name:
            raise ValueError("r2.bucket_name is not configured")

        db_path = db_path or self.cfg.db_path
        if not os.path.exists(db_path):
            raise FileNotFoundError(f"DuckDB file not found: {db_path}")

        key = self._build_key(db_path)
        bucket = self.cfg.r2.bucket_name

        try:
            self._r2_client().upload_file(db_path, bucket, key)
        except ClientError as exc:
            logger.error("Failed to upload DuckDB to R2: %s", exc)
            raise

        logger.info("Synced DuckDB to R2: %s/%s", bucket, key)
        return {"status": "SUCCESS", "bucket": bucket, "key": key}


__all__ = ["R2SyncManager"]
