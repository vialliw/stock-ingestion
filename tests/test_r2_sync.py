"""Tests for the R2 (S3-compatible) DuckDB sync manager.

All R2/boto3 interactions are mocked — no real credentials or network
requests are used.
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from src.config import PipelineConfig
from src.r2_sync import R2SyncManager


@pytest.fixture
def cfg_enabled():
    """A PipelineConfig with R2 sync enabled and a test bucket."""
    cfg = PipelineConfig()
    cfg.r2.enabled = True
    cfg.r2.bucket_name = "test-bucket"
    cfg.r2.prefix = "stock-ingestion/"
    return cfg


@pytest.fixture
def cfg_disabled():
    """A PipelineConfig with R2 sync disabled."""
    cfg = PipelineConfig()
    cfg.r2.enabled = False
    return cfg


@pytest.fixture
def db_file(tmp_path):
    """A small DuckDB-like file to upload."""
    path = tmp_path / "market_data.duckdb"
    path.write_bytes(b"not a real duckdb")
    return str(path)


@pytest.fixture
def mock_boto3_client():
    """Patch boto3.client and yield the mocked client factory."""
    with patch("src.r2_sync.boto3.client") as mock_client_factory:
        mock_client = MagicMock()
        mock_client_factory.return_value = mock_client
        yield mock_client_factory, mock_client


@pytest.fixture
def env_vars():
    """Set the required R2 environment variables for a test."""
    env = {
        "R2_ACCESS_KEY_ID": "test-access-key",
        "R2_SECRET_ACCESS_KEY": "test-secret-key",
        "R2_ENDPOINT_URL": "https://test.endpoint.example",
    }
    with patch.dict(os.environ, env, clear=False):
        yield


class TestR2SyncManager:
    """Unit tests for R2SyncManager.sync."""

    def test_sync_skipped_when_disabled(
        self, cfg_disabled, db_file, mock_boto3_client
    ):
        result = R2SyncManager(cfg_disabled).sync(db_file)

        assert result["status"] == "SKIPPED"
        mock_boto3_client[1].upload_file.assert_not_called()

    def test_sync_uploads_file_when_enabled(
        self, cfg_enabled, db_file, mock_boto3_client, env_vars
    ):
        result = R2SyncManager(cfg_enabled).sync(db_file)

        assert result["status"] == "SUCCESS"
        assert result["bucket"] == "test-bucket"
        assert result["key"] == "stock-ingestion/market_data.duckdb"

        mock_client_factory, mock_client = mock_boto3_client
        mock_client_factory.assert_called_once()
        args, kwargs = mock_client_factory.call_args
        assert args == ("s3",)
        assert kwargs["endpoint_url"] == "https://test.endpoint.example"
        assert kwargs["aws_access_key_id"] == "test-access-key"
        assert kwargs["aws_secret_access_key"] == "test-secret-key"

        mock_client.upload_file.assert_called_once_with(
            db_file, "test-bucket", "stock-ingestion/market_data.duckdb"
        )

    def test_sync_raises_when_credentials_missing(
        self, cfg_enabled, db_file, mock_boto3_client
    ):
        with patch.dict(
            os.environ,
            {
                "R2_ACCESS_KEY_ID": "",
                "R2_SECRET_ACCESS_KEY": "",
                "R2_ENDPOINT_URL": "",
            },
            clear=False,
        ):
            with pytest.raises(RuntimeError, match="Missing R2 credentials"):
                R2SyncManager(cfg_enabled).sync(db_file)

    def test_sync_uses_prefix_without_trailing_slash(
        self, cfg_enabled, db_file, mock_boto3_client, env_vars
    ):
        cfg_enabled.r2.prefix = "backups"

        result = R2SyncManager(cfg_enabled).sync(db_file)

        assert result["key"] == "backups/market_data.duckdb"
        _, mock_client = mock_boto3_client
        mock_client.upload_file.assert_called_once_with(
            db_file, "test-bucket", "backups/market_data.duckdb"
        )

    def test_sync_raises_when_bucket_missing(
        self, cfg_enabled, db_file, mock_boto3_client, env_vars
    ):
        cfg_enabled.r2.bucket_name = ""

        with pytest.raises(ValueError, match="bucket_name"):
            R2SyncManager(cfg_enabled).sync(db_file)

    def test_sync_propagates_client_error(
        self, cfg_enabled, db_file, mock_boto3_client, env_vars
    ):
        _, mock_client = mock_boto3_client
        mock_client.upload_file.side_effect = ClientError(
            {"Error": {"Code": "NoSuchBucket", "Message": "missing"}},
            "UploadFile",
        )

        with pytest.raises(ClientError):
            R2SyncManager(cfg_enabled).sync(db_file)

    def test_sync_uses_cfg_db_path_when_not_provided(
        self, cfg_enabled, mock_boto3_client, env_vars, tmp_path
    ):
        cfg_enabled.database.duckdb_dir = str(tmp_path)
        cfg_enabled.database.duckdb_filename = "canonical.duckdb"
        db_path = cfg_enabled.db_path
        # Write the canonical path so the file exists.
        open(db_path, "wb").write(b"data")

        result = R2SyncManager(cfg_enabled).sync()

        assert result["key"] == "stock-ingestion/canonical.duckdb"
        _, mock_client = mock_boto3_client
        mock_client.upload_file.assert_called_once_with(
            db_path, "test-bucket", "stock-ingestion/canonical.duckdb"
        )
