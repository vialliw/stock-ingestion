"""Tests for the standalone APScheduler-based daily load scheduler."""

from __future__ import annotations

import json
import signal
import sys
from pathlib import Path
from unittest.mock import ANY, MagicMock, patch

import duckdb
import pyarrow.flight as flight
import pytest

# Add scripts dir to path so daily_load_once can be imported (same
# pattern as tests/test_poll_tasks.py).
SCRIPTS_DIR = Path(__file__).parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

import daily_load_once

from src.config import PipelineConfig, SchedulerConfig
from src.scheduler import PipelineScheduler


@pytest.fixture
def enabled_config(tmp_path):
    """A config with the scheduler enabled and an isolated database path."""
    return PipelineConfig(
        database={"duckdb_dir": str(tmp_path), "duckdb_filename": "test.duckdb"},
        scheduler=SchedulerConfig(enabled=True),
    )


@pytest.fixture
def disabled_config(tmp_path):
    """A config with the scheduler disabled."""
    return PipelineConfig(
        database={"duckdb_dir": str(tmp_path), "duckdb_filename": "test.duckdb"},
        scheduler=SchedulerConfig(enabled=False),
    )


def test_parse_schedule_time():
    assert PipelineScheduler._parse_schedule_time("09:05") == (9, 5)
    assert PipelineScheduler._parse_schedule_time("23:59") == (23, 59)
    assert PipelineScheduler._parse_schedule_time("garbage") == (16, 30)


def test_scheduler_disabled_does_not_start(disabled_config):
    scheduler = PipelineScheduler(disabled_config)
    with patch("src.scheduler.BackgroundScheduler") as mock_cls:
        scheduler.start(block=False)
        mock_cls.assert_not_called()


def test_scheduler_start_adds_cron_job(enabled_config):
    scheduler = PipelineScheduler(enabled_config)
    mock_scheduler = MagicMock()
    with patch("src.scheduler.BackgroundScheduler", return_value=mock_scheduler):
        scheduler.start(block=False)

    mock_scheduler.add_job.assert_called_once()
    args, kwargs = mock_scheduler.add_job.call_args
    assert args[1] == "cron"
    assert kwargs["hour"] == 16
    assert kwargs["minute"] == 30
    assert kwargs["timezone"] == "America/New_York"
    assert kwargs["id"] == "daily_load"
    assert kwargs["replace_existing"] is True
    mock_scheduler.start.assert_called_once()

    scheduler.shutdown()


def test_scheduler_start_blocks_until_shutdown(enabled_config):
    scheduler = PipelineScheduler(enabled_config)
    scheduler._scheduler = MagicMock()
    # Unblock the wait loop after the first iteration.
    scheduler._shutdown_event.set()
    scheduler.start(block=True)


@patch("src.scheduler.mcal.get_calendar")
@patch("src.scheduler.DatabaseManager")
@patch("src.scheduler.IngestionEngine")
@patch("src.scheduler.time.sleep")
def test_run_once_market_closed_skips(
    mock_sleep, mock_engine_cls, mock_db_cls, mock_get_calendar, enabled_config
):
    mock_cal = MagicMock()
    mock_cal.schedule.return_value.empty = True
    mock_get_calendar.return_value = mock_cal

    scheduler = PipelineScheduler(enabled_config)
    scheduler.run_once()

    mock_get_calendar.assert_called_once()
    mock_db_cls.assert_called_once_with(enabled_config.db_path)
    mock_engine_cls.assert_not_called()
    mock_db_cls.return_value.close.assert_called_once()


@patch("src.scheduler.mcal.get_calendar")
@patch("src.scheduler.DatabaseManager")
@patch("src.scheduler.IngestionEngine")
@patch("src.scheduler.time.sleep")
def test_run_once_market_open_runs_daily_load(
    mock_sleep, mock_engine_cls, mock_db_cls, mock_get_calendar, enabled_config
):
    mock_cal = MagicMock()
    mock_cal.schedule.return_value.empty = False
    mock_get_calendar.return_value = mock_cal

    mock_engine = MagicMock()
    mock_engine.run_daily_load.return_value = {
        "status": "SUCCESS",
        "rows_upserted": 42,
    }
    mock_engine_cls.return_value = mock_engine

    scheduler = PipelineScheduler(enabled_config)
    scheduler.run_once()

    mock_engine_cls.assert_called_once_with(mock_db_cls.return_value, enabled_config)
    mock_engine.run_daily_load.assert_called_once()
    mock_db_cls.return_value.close.assert_called_once()


@patch("src.scheduler.mcal.get_calendar")
@patch("src.scheduler.DatabaseManager")
@patch("src.scheduler.IngestionEngine")
@patch("src.scheduler.time.sleep")
def test_run_once_retries_then_succeeds_on_db_lock(
    mock_sleep, mock_engine_cls, mock_db_cls, mock_get_calendar, enabled_config
):
    mock_cal = MagicMock()
    mock_cal.schedule.return_value.empty = False
    mock_get_calendar.return_value = mock_cal

    mock_db = MagicMock()
    mock_db_cls.side_effect = [
        duckdb.IOException("locked"),
        duckdb.IOException("locked"),
        mock_db,
    ]

    scheduler = PipelineScheduler(enabled_config)
    scheduler.run_once()

    assert mock_db_cls.call_count == 3
    assert mock_sleep.call_count == 2
    mock_engine_cls.assert_called_once_with(mock_db, enabled_config)
    mock_db.close.assert_called_once()


@patch("src.scheduler.mcal.get_calendar")
@patch("src.scheduler.DatabaseManager")
@patch("src.scheduler.IngestionEngine")
@patch("src.scheduler.time.sleep")
def test_run_once_retries_exhausted_logs_error(
    mock_sleep, mock_engine_cls, mock_db_cls, mock_get_calendar, enabled_config
):
    mock_cal = MagicMock()
    mock_cal.schedule.return_value.empty = False
    mock_get_calendar.return_value = mock_cal

    mock_db_cls.side_effect = duckdb.IOException("locked")

    scheduler = PipelineScheduler(enabled_config)
    scheduler.run_once()

    assert mock_db_cls.call_count == 4
    assert mock_sleep.call_count == 3
    mock_engine_cls.assert_not_called()


@patch("src.scheduler.signal.signal")
def test_install_signal_handlers(mock_signal, enabled_config):
    scheduler = PipelineScheduler(enabled_config)
    scheduler.install_signal_handlers()
    mock_signal.assert_any_call(signal.SIGINT, ANY)
    mock_signal.assert_any_call(signal.SIGTERM, ANY)


@patch("src.scheduler.IngestionEngine")
def test_execute_daily_load_force_bypasses_market_gate(
    mock_engine_cls, enabled_config
):
    """F7-T15: force=True bypasses the NYSE market-closed gate entirely
    and refreshes the configured ``ingestion.daily_lookback_days`` window
    so a missed trading day is caught up (local one-shot runner path)."""
    mock_engine = MagicMock()
    mock_engine.run_daily_load.return_value = {
        "status": "SUCCESS",
        "rows_upserted": 7,
    }
    mock_engine_cls.return_value = mock_engine

    scheduler = PipelineScheduler(enabled_config)
    db_manager = MagicMock()
    with patch.object(
        PipelineScheduler, "_is_market_open_today", return_value=False
    ) as mock_gate:
        scheduler._execute_daily_load(db_manager, force=True)

    # The market-open gate was never consulted.
    mock_gate.assert_not_called()
    mock_engine_cls.assert_called_once_with(db_manager, enabled_config)
    mock_engine.run_daily_load.assert_called_once_with(
        lookback_days=enabled_config.ingestion.daily_lookback_days,
        force=True,
    )


@patch("src.scheduler.mcal.get_calendar")
@patch("src.scheduler.IngestionEngine")
def test_execute_daily_load_default_keeps_gate_and_lookback_1(
    mock_engine_cls, mock_get_calendar, enabled_config
):
    """Daemon default (force=False) is unchanged: the market-open gate is
    consulted and the engine is called with lookback_days=1 and no force
    flag, so the APScheduler path still skips on market-closed days."""
    mock_cal = MagicMock()
    mock_cal.schedule.return_value.empty = False
    mock_get_calendar.return_value = mock_cal

    mock_engine = MagicMock()
    mock_engine.run_daily_load.return_value = {
        "status": "SUCCESS",
        "rows_upserted": 1,
    }
    mock_engine_cls.return_value = mock_engine

    scheduler = PipelineScheduler(enabled_config)
    scheduler._execute_daily_load(MagicMock())

    mock_get_calendar.assert_called_once()
    mock_engine.run_daily_load.assert_called_once_with(lookback_days=1)


# =====================================================================
# F7-T16: scripts/daily_load_once.py — service-first Flight routing
# =====================================================================


def _mock_flight_module():
    """A stand-in for the ``pyarrow.flight`` module inside
    ``daily_load_once``: real value/exception classes (needed for
    ``except flight.<X>Error`` clauses and Action construction) but a
    mocked ``FlightClient``.
    """
    mock_flight = MagicMock()
    mock_flight.Action = flight.Action
    mock_flight.FlightCallOptions = flight.FlightCallOptions
    mock_flight.FlightError = flight.FlightError
    mock_flight.FlightServerError = flight.FlightServerError
    mock_flight.FlightTimedOutError = flight.FlightTimedOutError
    mock_flight.FlightUnavailableError = flight.FlightUnavailableError
    return mock_flight


def _flight_result(payload: dict):
    """Build a mock flight.Result-like object carrying a JSON body."""
    result = MagicMock()
    result.body.to_pybytes.return_value = json.dumps(payload).encode("utf-8")
    return result


class TestDailyLoadOnceFlightRouting:
    """daily_load_once._run_once routes the daily load through the
    running Flight service when reachable, and falls back to the direct
    DuckDB path only when the service is unreachable."""

    @pytest.fixture
    def config(self, tmp_path):
        """Enabled scheduler config with an isolated database path."""
        return PipelineConfig(
            database={
                "duckdb_dir": str(tmp_path),
                "duckdb_filename": "test.duckdb",
            },
            scheduler=SchedulerConfig(enabled=True),
        )

    @pytest.mark.parametrize("status", ["SUCCESS", "PARTIAL"])
    def test_flight_success_no_direct_db_open(self, config, status):
        """Service up + SUCCESS/PARTIAL -> exit 0 without ever opening
        DuckDB directly (the service stays the single writer)."""
        mock_flight = _mock_flight_module()
        mock_client = mock_flight.FlightClient.return_value
        mock_client.do_action.return_value = iter(
            [_flight_result({"status": status, "rows_upserted": 5})]
        )

        with patch.object(daily_load_once, "flight", mock_flight), patch.object(
            daily_load_once, "PipelineScheduler"
        ) as mock_sched_cls:
            code = daily_load_once._run_once(config)

        assert code == 0
        mock_sched = mock_sched_cls.return_value
        mock_sched._create_db_manager.assert_not_called()
        mock_sched._execute_daily_load.assert_not_called()

        # The action sent to the service is daily_load with force=true.
        action_arg = mock_client.do_action.call_args.args[0]
        assert action_arg.type == "daily_load"
        assert json.loads(action_arg.body.to_pybytes()) == {"force": True}

    @pytest.mark.parametrize(
        "error",
        [
            flight.FlightServerError("server exploded"),
            flight.FlightTimedOutError("deadline exceeded"),
        ],
        ids=["server_error", "timeout"],
    )
    def test_flight_server_error_or_timeout_returns_1(self, config, error):
        """Service up but the action fails -> exit 1, no direct-DB
        fallback (the service handled the request)."""
        mock_flight = _mock_flight_module()
        mock_client = mock_flight.FlightClient.return_value
        mock_client.do_action.side_effect = error

        with patch.object(daily_load_once, "flight", mock_flight), patch.object(
            daily_load_once, "PipelineScheduler"
        ) as mock_sched_cls:
            code = daily_load_once._run_once(config)

        assert code == 1
        mock_sched_cls.return_value._create_db_manager.assert_not_called()

    def test_flight_non_success_status_returns_1(self, config):
        """A reachable service that answers with a non-success status
        (e.g. SKIPPED from a stale server without force support) ->
        exit 1, no direct-DB fallback."""
        mock_flight = _mock_flight_module()
        mock_client = mock_flight.FlightClient.return_value
        mock_client.do_action.return_value = iter(
            [_flight_result({"status": "SKIPPED", "reason": "Market closed"})]
        )

        with patch.object(daily_load_once, "flight", mock_flight), patch.object(
            daily_load_once, "PipelineScheduler"
        ) as mock_sched_cls:
            code = daily_load_once._run_once(config)

        assert code == 1
        mock_sched_cls.return_value._create_db_manager.assert_not_called()

    def test_flight_unreachable_falls_back_to_direct(self, config):
        """Connection failure (server down) -> log + fall back to the
        existing direct path _execute_daily_load(db_manager, force=True).
        """
        mock_flight = _mock_flight_module()
        mock_client = mock_flight.FlightClient.return_value
        mock_client.wait_for_available.side_effect = (
            flight.FlightUnavailableError("connection refused")
        )

        with patch.object(daily_load_once, "flight", mock_flight), patch.object(
            daily_load_once, "PipelineScheduler"
        ) as mock_sched_cls:
            code = daily_load_once._run_once(config)

        assert code == 0
        mock_sched = mock_sched_cls.return_value
        mock_sched._create_db_manager.assert_called_once()
        mock_sched._execute_daily_load.assert_called_once_with(
            mock_sched._create_db_manager.return_value, force=True
        )
        mock_sched._create_db_manager.return_value.close.assert_called_once()

    def test_flight_unavailable_mid_call_falls_back(self, config):
        """If the server drops between the availability probe and the
        action call, the direct path still takes over."""
        mock_flight = _mock_flight_module()
        mock_client = mock_flight.FlightClient.return_value
        mock_client.do_action.side_effect = flight.FlightUnavailableError(
            "server went away"
        )

        with patch.object(daily_load_once, "flight", mock_flight), patch.object(
            daily_load_once, "PipelineScheduler"
        ) as mock_sched_cls:
            code = daily_load_once._run_once(config)

        assert code == 0
        mock_sched = mock_sched_cls.return_value
        mock_sched._execute_daily_load.assert_called_once_with(
            mock_sched._create_db_manager.return_value, force=True
        )

    def test_scheduler_disabled_returns_0_without_flight(self, tmp_path):
        """enabled=false -> 0 and the Flight service is never consulted."""
        config = PipelineConfig(
            database={
                "duckdb_dir": str(tmp_path),
                "duckdb_filename": "test.duckdb",
            },
            scheduler=SchedulerConfig(enabled=False),
        )
        mock_flight = _mock_flight_module()

        with patch.object(daily_load_once, "flight", mock_flight):
            code = daily_load_once._run_once(config)

        assert code == 0
        mock_flight.FlightClient.assert_not_called()
