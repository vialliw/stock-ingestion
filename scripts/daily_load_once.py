"""One-shot daily load runner for Windows Task Scheduler.

Runs a single daily-load cycle using the default ``src/config.yaml`` —
the same code path as the APScheduler daemon (NYSE market check, DuckDB
lock retry, ``run_daily_load``, optional R2 sync).  Unlike
``src/scheduler_main.py`` this does not block; the process exits when
the cycle finishes.

Exit codes (so Task Scheduler can detect failure):

- ``0`` — the cycle ran (or was deliberately skipped, e.g. market
  closed or ``scheduler.enabled: false``).
- ``1`` — the daily load could NOT run: DuckDB stayed locked after all
  retries, config/logging setup failed, or the load crashed.  The full
  traceback is written to ``cfg.logging.log_file`` before exiting.

Can be run manually:

    python scripts/daily_load_once.py
"""

from __future__ import annotations

import logging
import os
import sys

# Ensure src/ is importable regardless of the task's working directory.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _REPO_ROOT)

_CONFIG_PATH = os.path.join(_REPO_ROOT, "src", "config.yaml")
_FALLBACK_LOG = os.path.join(_REPO_ROOT, "logs", "daily_load_once_error.log")

from pythonjsonlogger import json as jsonlogger

from src.config import PipelineConfig
from src.scheduler import PipelineScheduler

logger = logging.getLogger(__name__)

_TEXT_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
_JSON_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"


def _configure_logging(config: PipelineConfig) -> None:
    """Log to ``cfg.logging.log_file`` (Task Scheduler has no console).

    Honors ``cfg.logging.log_format``: ``'json'`` emits
    pythonjsonlogger-formatted lines (same as
    ``src/scheduler_main.py``); anything else emits plain text.
    """
    log_file = config.logging.log_file
    log_dir = os.path.dirname(log_file)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)

    if config.logging.log_format == "json":
        formatter: logging.Formatter = jsonlogger.JsonFormatter(_JSON_FORMAT)
    else:
        formatter = logging.Formatter(_TEXT_FORMAT)

    root = logging.getLogger()
    root.setLevel(logging.INFO)

    handler = logging.FileHandler(log_file, encoding="utf-8")
    handler.setFormatter(formatter)
    root.addHandler(handler)


def _configure_fallback_logging() -> None:
    """Last-resort logging when the config (and thus ``log_file``)
    cannot be loaded — writes to a repo-local file plus stderr."""
    os.makedirs(os.path.dirname(_FALLBACK_LOG), exist_ok=True)
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    file_handler = logging.FileHandler(_FALLBACK_LOG, encoding="utf-8")
    file_handler.setFormatter(logging.Formatter(_TEXT_FORMAT))
    root.addHandler(file_handler)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(logging.Formatter(_TEXT_FORMAT))
    root.addHandler(stream_handler)


def _run_once(config: PipelineConfig) -> int:
    """Execute one daily-load cycle; return the process exit code.

    Replicates the thin sequence inside ``PipelineScheduler._run_with_retry``
    but propagates the DuckDB-lock failure instead of swallowing it:
    ``_create_db_manager`` raises once its retry/backoff attempts are
    exhausted, and that exception is logged + turned into exit code 1 so
    Task Scheduler records the run as failed.
    """
    scheduler = PipelineScheduler(config)

    if not config.scheduler.enabled:
        logger.warning(
            "scheduler.enabled is false in config; daily load not run."
        )
        return 0

    try:
        db_manager = scheduler._create_db_manager()
    except Exception:
        logger.exception(
            "Could not open DuckDB after retries; daily load did NOT run."
        )
        return 1

    try:
        scheduler._execute_daily_load(db_manager)
    finally:
        db_manager.close()
    return 0


def main() -> int:
    """Run one daily-load cycle and return the process exit code."""
    try:
        config = PipelineConfig.from_yaml(_CONFIG_PATH)
        _configure_logging(config)
    except Exception:
        # Config or log_file unavailable — fall back to a repo-local file
        # so the traceback is not lost to a discarded stderr stream.
        _configure_fallback_logging()
        logger.exception("daily_load_once failed during startup.")
        return 1

    try:
        return _run_once(config)
    except Exception:
        # Uncaught load exception: land the full traceback in the
        # configured log file, then exit non-zero.
        logger.exception("daily_load_once crashed with an uncaught exception.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
