"""Tests for the R2 (S3-compatible) DuckDB sync manager.

All R2/boto3 interactions are mocked — no real credentials or network
requests are used.
"""

from __future__ import annotations

import json
import os
from unittest.mock import MagicMock, patch

import pandas as pd
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


class TestR2SyncEdgeCases:
    """Edge-case coverage for key building and path handling."""

    def test_build_key_uses_posix_basename(self, cfg_enabled):
        assert (
            R2SyncManager(cfg_enabled)._build_key("/data/market_data.duckdb")
            == "stock-ingestion/market_data.duckdb"
        )

    def test_build_key_splits_windows_paths_with_ntpath(
        self, cfg_enabled, monkeypatch
    ):
        import ntpath

        monkeypatch.setattr("src.r2_sync.os.path.basename", ntpath.basename)
        assert (
            R2SyncManager(cfg_enabled)._build_key("C:\\data\\market_data.duckdb")
            == "stock-ingestion/market_data.duckdb"
        )


def _make_flight_server(module, cfg):
    """Create a Flight server that doesn't bind a real gRPC port."""
    db_mgr = MagicMock()
    ingest_engine = MagicMock()

    class _TestServer(module.MarketDataFlightServer):
        def __init__(self, db_mgr, ingest_engine, cfg):
            self.db = db_mgr
            self.engine = ingest_engine
            self.cfg = cfg

    return _TestServer(db_mgr, ingest_engine, cfg)


def _make_flight_action(action_type):
    action = MagicMock()
    action.type = action_type
    action.body = None
    return action


class TestFlightServerR2Action:
    """Coverage for the manual sync_to_r2 Flight action."""

    def test_sync_to_r2_action_returns_success(
        self, cfg_enabled, mock_boto3_client, env_vars, tmp_path
    ):
        import src.flight_server as module

        cfg = cfg_enabled
        cfg.database.duckdb_dir = str(tmp_path)
        cfg.database.duckdb_filename = "market_data.duckdb"
        open(cfg.db_path, "wb").write(b"data")

        server = _make_flight_server(module, cfg)
        action = _make_flight_action("sync_to_r2")
        result = server.do_action(MagicMock(), action)
        data = json.loads(result.body.to_pybytes().decode("utf-8"))

        assert data["status"] == "SUCCESS"
        assert data["bucket"] == "test-bucket"
        assert data["key"] == "stock-ingestion/market_data.duckdb"

    def test_sync_to_r2_action_propagates_raw_errors(
        self, cfg_enabled, tmp_path, monkeypatch
    ):
        import src.flight_server as module

        cfg = cfg_enabled
        cfg.database.duckdb_dir = str(tmp_path)
        cfg.database.duckdb_filename = "market_data.duckdb"

        server = _make_flight_server(module, cfg)
        action = _make_flight_action("sync_to_r2")

        fake_manager = MagicMock()
        fake_manager.sync.side_effect = RuntimeError("missing credentials")
        monkeypatch.setattr(
            "src.flight_server.R2SyncManager", lambda cfg: fake_manager
        )

        with pytest.raises(RuntimeError, match="missing credentials"):
            server.do_action(MagicMock(), action)


class TestIngestionAutoSync:
    """Coverage for the auto-sync trigger after daily_load."""

    @pytest.fixture(autouse=True)
    def _patch_market_and_download(self, monkeypatch):
        import src.ingestion as ing_module

        fake_sched = MagicMock()
        fake_sched.empty = False
        fake_cal = MagicMock()
        fake_cal.schedule.return_value = fake_sched
        monkeypatch.setattr(ing_module.mcal, "get_calendar", lambda name: fake_cal)
        monkeypatch.setattr(
            ing_module.yf, "download", lambda **kwargs: pd.DataFrame()
        )
        monkeypatch.setattr(
            ing_module.yf,
            "Ticker",
            lambda t: MagicMock(splits=pd.Series(dtype=float)),
        )

    def _make_engine(self, cfg):
        from src.ingestion import IngestionEngine

        db_mgr = MagicMock()
        db_mgr.detect_new_tickers.return_value = []
        db_mgr.upsert_dataframe.return_value = 0
        return IngestionEngine(db_mgr, cfg), db_mgr

    def test_daily_load_triggers_r2_sync_when_enabled(
        self, cfg_enabled, mock_boto3_client, env_vars, tmp_path
    ):
        cfg = cfg_enabled
        cfg.database.duckdb_dir = str(tmp_path)
        cfg.database.duckdb_filename = "market_data.duckdb"
        open(cfg.db_path, "wb").write(b"data")

        engine, db_mgr = self._make_engine(cfg)
        result = engine.run_daily_load()
        assert result["status"] == "SUCCESS"
        assert result["r2_sync"]["status"] == "SUCCESS"

    def test_daily_load_skips_r2_sync_when_disabled(
        self, cfg_disabled, tmp_path
    ):
        cfg = cfg_disabled
        cfg.database.duckdb_dir = str(tmp_path)
        cfg.database.duckdb_filename = "market_data.duckdb"

        engine, db_mgr = self._make_engine(cfg)
        result = engine.run_daily_load()
        assert result["status"] == "SUCCESS"
        assert "r2_sync" not in result

    def test_daily_load_skips_r2_sync_on_market_closed(
        self, cfg_enabled, tmp_path, monkeypatch
    ):
        cfg = cfg_enabled
        cfg.database.duckdb_dir = str(tmp_path)
        cfg.database.duckdb_filename = "market_data.duckdb"
        open(cfg.db_path, "wb").write(b"data")

        fake_sched = MagicMock()
        fake_sched.empty = True
        fake_cal = MagicMock()
        fake_cal.schedule.return_value = fake_sched
        monkeypatch.setattr(
            "src.ingestion.mcal.get_calendar", lambda name: fake_cal
        )

        engine, db_mgr = self._make_engine(cfg)
        result = engine.run_daily_load()
        assert result["status"] == "SKIPPED"
        assert "r2_sync" not in result

    def test_daily_load_auto_syncs_on_partial_with_zero_rows(
        self, cfg_enabled, mock_boto3_client, env_vars, tmp_path
    ):
        cfg = cfg_enabled
        cfg.database.duckdb_dir = str(tmp_path)
        cfg.database.duckdb_filename = "market_data.duckdb"
        open(cfg.db_path, "wb").write(b"data")
        cfg.ingestion.tickers = ["AAPL"]

        engine, db_mgr = self._make_engine(cfg)
        result = engine.run_daily_load()
        assert result["status"] == "PARTIAL"
        assert result["rows_upserted"] == 0
        assert result["r2_sync"]["status"] == "SUCCESS"
