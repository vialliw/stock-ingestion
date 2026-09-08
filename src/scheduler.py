"""Standalone scheduler for the stock-ingestion pipeline.

``PipelineScheduler`` uses APScheduler's ``BackgroundScheduler`` to trigger
``IngestionEngine.run_daily_load`` at a configurable wall-clock time.  It
opens its own DuckDB connection for each run, retries with backoff when the
file is locked by another process, and shuts down cleanly on SIGINT/SIGTERM.
"""

from __future__ import annotations

import logging
import signal
import sys
import threading
import time
from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo

import duckdb
import pandas_market_calendars as mcal
from apscheduler.schedulers.background import BackgroundScheduler

from src.config import PipelineConfig
from src.db import DatabaseManager
from src.ingestion import IngestionEngine

logger = logging.getLogger(__name__)

NYSE_CALENDAR = "NYSE"


class PipelineScheduler:
    """APScheduler wrapper that runs the daily ingestion load once per day.

    The scheduler is process-safe with respect to DuckDB: each invocation
    opens a fresh read-write connection, completes the load, and closes it.
    If another process currently holds the DuckDB write lock, the scheduler
    retries with exponential backoff before giving up for that cycle.
    """

    def __init__(self, cfg: PipelineConfig):
        self.cfg = cfg
        self._scheduler: Optional[BackgroundScheduler] = None
        self._shutdown_event = threading.Event()

    @staticmethod
    def _parse_schedule_time(time_str: str) -> tuple[int, int]:
        """Parse an ``HH:MM`` string into ``(hour, minute)`` integers.

        Falls back to the default 16:30 if the string is malformed.
        """
        try:
            parsed = datetime.strptime(time_str, "%H:%M")
        except ValueError:
            logger.warning(
                "Invalid schedule_time %r; using default 16:30.", time_str
            )
            return 16, 30
        return parsed.hour, parsed.minute

    @staticmethod
    def _is_market_open_today(date_str: str) -> bool:
        """Return ``True`` if the NYSE is open on ``date_str`` (YYYY-MM-DD)."""
        try:
            nyse = mcal.get_calendar(NYSE_CALENDAR)
            sched = nyse.schedule(start_date=date_str, end_date=date_str)
            return not sched.empty
        except Exception as exc:
            logger.error("Failed to query NYSE calendar for %s: %s", date_str, exc)
            # If the calendar cannot be determined, let the engine decide.
            return True

    def _create_db_manager(self) -> DatabaseManager:
        """Open a read-write ``DatabaseManager``, retrying if DuckDB is locked.

        Justified sequential retry loop: DuckDB only allows one writer at a
        time per file, so a separate scheduler process must back off and try
        again when the main process holds the lock.
        """
        last_error: Optional[Exception] = None
        backoff = (0.5, 1.0, 2.0, 4.0)
        # justified: DuckDB only permits one concurrent writer per file; the
        # standalone scheduler process must retry with backoff when another
        # process already holds the write lock.
        for attempt in range(len(backoff)):
            try:
                return DatabaseManager(self.cfg.db_path)
            except Exception as exc:
                last_error = exc
                if attempt == len(backoff) - 1:
                    break
                logger.warning(
                    "DuckDB open failed (attempt %d), retrying in %.1fs: %s",
                    attempt + 1,
                    backoff[attempt],
                    exc,
                )
                time.sleep(backoff[attempt])

        if last_error is None:
            raise RuntimeError("DuckDB connection unexpectedly unavailable")
        raise last_error

    def _execute_daily_load(self, db_manager: DatabaseManager) -> None:
        """Run the daily load if the NYSE is open today."""
        tz_name = self.cfg.scheduler.timezone
        try:
            tz = ZoneInfo(tz_name)
        except Exception:
            logger.warning("Unknown timezone %r; falling back to UTC.", tz_name)
            tz = ZoneInfo("UTC")

        today = datetime.now(tz).strftime("%Y-%m-%d")

        if not self._is_market_open_today(today):
            logger.info(
                "NYSE closed on %s; daily load skipped by scheduler.", today
            )
            return

        engine = IngestionEngine(db_manager, self.cfg)
        result = engine.run_daily_load()
        logger.info(
            "Daily load completed: status=%s, rows=%s",
            result.get("status", "UNKNOWN"),
            result.get("rows_upserted", 0),
        )

    def _run_with_retry(self) -> None:
        """Wrapper that acquires a DuckDB connection and runs the daily load."""
        if not self.cfg.scheduler.enabled:
            logger.info("Scheduler is disabled; skipping scheduled run.")
            return

        try:
            db_manager = self._create_db_manager()
        except Exception as exc:
            logger.error("Could not open DuckDB after retries: %s", exc)
            return

        try:
            self._execute_daily_load(db_manager)
        finally:
            db_manager.close()

    def run_once(self) -> None:
        """Manually trigger one daily-load cycle (used by tests and tooling)."""
        self._run_with_retry()

    def start(self, block: bool = True) -> None:
        """Start the background scheduler and optionally block until shutdown.

        When ``enabled`` is ``False`` the scheduler logs a warning and returns
        immediately without scheduling any jobs.
        """
        if not self.cfg.scheduler.enabled:
            logger.warning("Scheduler is not enabled in config; not starting.")
            return

        hour, minute = self._parse_schedule_time(self.cfg.scheduler.schedule_time)
        timezone = self.cfg.scheduler.timezone

        self._scheduler = BackgroundScheduler()
        self._scheduler.add_job(
            self._run_with_retry,
            "cron",
            hour=hour,
            minute=minute,
            timezone=timezone,
            id="daily_load",
            replace_existing=True,
        )
        self._scheduler.start()

        logger.info(
            "PipelineScheduler started: daily_load at %02d:%02d in %s",
            hour,
            minute,
            timezone,
        )

        if block:
            self._wait_until_shutdown()

    def _wait_until_shutdown(self) -> None:
        """Block the main thread until ``shutdown()`` is called."""
        while not self._shutdown_event.is_set():
            self._shutdown_event.wait(1.0)

    def shutdown(self) -> None:
        """Signal the wait loop and stop the APScheduler instance."""
        self._shutdown_event.set()
        if self._scheduler is not None:
            try:
                self._scheduler.shutdown(wait=False)
            except Exception as exc:
                logger.error("Error shutting down APScheduler: %s", exc)

    def install_signal_handlers(self) -> None:
        """Register SIGINT/SIGTERM handlers for graceful shutdown."""

        def _handler(signum, _frame):
            logger.warning("Shutdown signal %s received; stopping scheduler.", signum)
            self.shutdown()
            sys.exit(0)

        signal.signal(signal.SIGINT, _handler)
        signal.signal(signal.SIGTERM, _handler)


__all__ = ["PipelineScheduler"]
