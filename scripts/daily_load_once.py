"""One-shot daily load runner for Windows Task Scheduler.

Runs a single daily-load cycle using the default ``src/config.yaml``.

Service-first routing: when the Arrow Flight server is running (e.g. as
the ``StockIngestion-FlightServer`` startup task registered by
``scripts/register_flight_service.ps1``) it holds the DuckDB write lock
for its whole lifetime and must stay the single writer.  This runner
therefore first tries ``do_action("daily_load", {"force": true})`` on
``grpc://{server.flight_host}:{server.flight_port}`` with a short
connect probe.  Only when the service is unreachable does it fall back
to the direct path — the same code path as the APScheduler daemon
(DuckDB lock retry, ``run_daily_load``, optional R2 sync) invoked with
``force=True`` so the NYSE market-closed gate is bypassed and the
configured ``ingestion.daily_lookback_days`` window is refreshed instead
of the daemon's 1-day window.  This makes the scheduled run catch up on
missed trading days: if the PC was off on Friday and the task next runs
on Saturday, Friday's data is downloaded instead of the run being
skipped because Saturday is not a trading day.  Unlike
``src/scheduler_main.py`` this does not block; the process exits when
the cycle finishes.

Exit codes (so Task Scheduler can detect failure):

- ``0`` — the cycle ran (or was deliberately skipped, e.g.
  ``scheduler.enabled: false``); the Flight service reported SUCCESS or
  PARTIAL, or the direct fallback completed.
- ``1`` — the daily load could NOT run: the Flight service answered with
  an error/timeout or a non-success status, DuckDB stayed locked after
  all retries on the fallback path, config/logging setup failed, or the
  load crashed.  The full traceback is written to
  ``cfg.logging.log_file`` before exiting.

Can be run manually:

    python scripts/daily_load_once.py
"""

from __future__ import annotations

import json
import logging
import os
import sys
from typing import Optional

# Ensure src/ is importable regardless of the task's working directory.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _REPO_ROOT)

_CONFIG_PATH = os.path.join(_REPO_ROOT, "src", "config.yaml")
_FALLBACK_LOG = os.path.join(_REPO_ROOT, "logs", "daily_load_once_error.log")

import pyarrow.flight as flight
from pythonjsonlogger import json as jsonlogger

from src.config import PipelineConfig
from src.scheduler import PipelineScheduler

logger = logging.getLogger(__name__)

_TEXT_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
_JSON_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"

# Short probe answering "is the Flight service up?" — kept small so a
# stopped service does not stall the scheduled task before the direct
# fallback takes over.
_FLIGHT_CONNECT_TIMEOUT_S = 5
# Bound on the do_action call itself so the one-shot process cannot hang
# forever on a wedged server.  Generous because a forced catch-up load
# (multi-day lookback, possible split re-download) can take minutes.
_FLIGHT_ACTION_TIMEOUT_S = 600


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


def _run_via_flight(config: PipelineConfig) -> Optional[int]:
    """Run the daily load through the running Flight service.

    Sends ``do_action("daily_load", {"force": True})`` to
    ``grpc://{server.flight_host}:{server.flight_port}`` so the service —
    the process already holding the DuckDB write lock — performs the
    load and stays the single writer.

    Returns an exit code (``0`` on SUCCESS/PARTIAL, ``1`` on a Flight
    server error, call timeout, or a non-success status) when the
    service handled the request, or ``None`` when the service is
    unreachable — the caller should then fall back to the direct DB
    path.
    """
    location = (
        f"grpc://{config.server.flight_host}:{config.server.flight_port}"
    )
    try:
        client = flight.FlightClient(location)
        client.wait_for_available(timeout=_FLIGHT_CONNECT_TIMEOUT_S)
    except Exception as exc:
        logger.info(
            "Flight server at %s unreachable (%s); falling back to a "
            "direct DuckDB daily load.",
            location,
            exc,
        )
        return None

    logger.info(
        "Flight server reachable at %s; routing daily_load "
        "(force=True) through the service.",
        location,
    )
    try:
        action = flight.Action(
            "daily_load", json.dumps({"force": True}).encode("utf-8")
        )
        options = flight.FlightCallOptions(timeout=_FLIGHT_ACTION_TIMEOUT_S)
        results = list(client.do_action(action, options=options))
        payload = json.loads(results[0].body.to_pybytes().decode("utf-8"))
    except flight.FlightUnavailableError as exc:
        # The server dropped mid-call — the single writer is gone, so
        # the direct path can safely take over.
        logger.warning(
            "Flight server became unreachable during daily_load (%s); "
            "falling back to a direct DuckDB daily load.",
            exc,
        )
        return None
    except Exception as exc:
        logger.error(
            "Flight daily_load action failed (server error or timeout): "
            "%s",
            exc,
        )
        return 1

    status = payload.get("status")
    if status in ("SUCCESS", "PARTIAL"):
        logger.info(
            "Flight daily_load finished: status=%s rows_upserted=%s "
            "tickers_failed=%s",
            status,
            payload.get("rows_upserted"),
            payload.get("tickers_failed"),
        )
        return 0
    logger.error(
        "Flight daily_load returned non-success status %r: %s",
        status,
        payload,
    )
    return 1


def _run_once(config: PipelineConfig) -> int:
    """Execute one daily-load cycle; return the process exit code.

    Tries the running Flight service first (``_run_via_flight``) because
    a running server holds the DuckDB write lock for its whole lifetime
    and must stay the single writer.  When the service is unreachable,
    falls back to the direct path, which replicates the thin sequence
    inside ``PipelineScheduler._run_with_retry`` but with two deliberate
    differences: (1) the DuckDB-lock failure is propagated instead of
    swallowed — ``_create_db_manager`` raises once its retry/backoff
    attempts are exhausted, and that exception is logged + turned into
    exit code 1 so Task Scheduler records the run as failed; (2)
    ``_execute_daily_load`` is called with ``force=True`` so a missed
    trading day is caught up — the market-closed gate is bypassed and
    the configured ``ingestion.daily_lookback_days`` window is
    refreshed.
    """
    scheduler = PipelineScheduler(config)

    if not config.scheduler.enabled:
        logger.warning(
            "scheduler.enabled is false in config; daily load not run."
        )
        return 0

    # Service-first: route the load through the Flight server when it is
    # up so it remains the single DuckDB writer.
    flight_code = _run_via_flight(config)
    if flight_code is not None:
        return flight_code

    try:
        db_manager = scheduler._create_db_manager()
    except Exception:
        logger.exception(
            "Could not open DuckDB after retries; daily load did NOT run."
        )
        return 1

    try:
        # force=True: bypass the market-closed gate so a PC that was off
        # on a trading day catches up on the next run; the engine uses
        # cfg.ingestion.daily_lookback_days for the refresh window.
        scheduler._execute_daily_load(db_manager, force=True)
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
