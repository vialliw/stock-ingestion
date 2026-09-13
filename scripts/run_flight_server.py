"""Launch the Arrow Flight server from any working directory.

Cwd-safe wrapper around ``src.main.main()`` intended for the
``StockIngestion-FlightServer`` Windows scheduled task (registered by
``scripts/register_flight_service.ps1``) and for manual startup:

    python scripts/run_flight_server.py

Windows Task Scheduler does not guarantee a working directory, and
``src.main.main()`` loads ``src/config.yaml`` via a repo-relative path.
This script therefore anchors the repo root on ``sys.path`` (so ``src``
is importable) and makes it the process working directory before
delegating — the same header pattern used by
``scripts/daily_load_once.py``.

Restart-starvation hardening (challenger finding C1, F7-T18): when
``daily_load_once.py`` falls back to a direct DuckDB load because this
service is down, that fallback can hold the DuckDB write lock for
minutes.  Without an in-process retry, every one of the task's
``RestartCount=3`` / 1-minute restarts can collide with the lock and the
service stays dead until the next boot.  This wrapper therefore retries
``main()`` internally for up to ~10 minutes whenever the failure is the
DuckDB write-lock contention error — long enough to outlive one entire
worst-case fallback load (the do_action bound is 600 s).

Why this beats raising the task's RestartCount/RestartInterval
(Option B): the scheduled-task budget is fixed at registration time and
is *consumed* by each failed restart — any lock hold longer than
``RestartCount x RestartInterval`` still starves the service.  An
in-process retry window is instead *renewed* on every launch: each of
the 3 restarts gets a fresh ~10-minute window, compounding coverage
instead of spending it.  It is also self-contained — no re-registration
is needed for it to take effect.
"""

from __future__ import annotations

import logging
import os
import re
import sys
import time

# Ensure src/ is importable regardless of the task's working directory.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _REPO_ROOT)
os.chdir(_REPO_ROOT)

import duckdb

from src.main import main

logger = logging.getLogger(__name__)

# Total window a single launch keeps retrying when the DuckDB write lock
# is held by another process.  Sized to outlive one worst-case direct
# fallback daily load (~600 s, matching _FLIGHT_ACTION_TIMEOUT_S in
# scripts/daily_load_once.py).
_DB_OPEN_RETRY_WINDOW_S = 600.0

# Delay between attempts inside the retry window.  Short enough that a
# fresh window starts soon after the lock frees; long enough that ~40
# openings over 10 minutes cannot hammer the file.
_DB_OPEN_RETRY_DELAY_S = 15.0


# Message markers identifying *contention* IOErrors, verified live on
# duckdb 1.4.3 (two-process write lock):
#
#   Windows: IO Error: Cannot open file "<path>": The process cannot
#       access the file because it is being used by another process.
#       File is already open in <exe> (PID <pid>)
#   POSIX:   IO Error: Could not set lock on file "<path>": Conflicting
#       lock is held in <exe> (PID <pid>)
#
# The bare substring "lock" is deliberately NOT a marker (challenger
# finding N2, F7-T19): the message embeds the DB path inside double
# quotes, so a directory named e.g. "blocked_dir" substring-matched
# "lock" and turned a non-contention IOException into a 10-minute retry
# storm.  Markers are confined to phrases that appear only in the
# *reason* portion of contention errors, and quoted spans (the path) are
# stripped before matching as belt-and-braces — even an adversarial path
# like "C:\set lock\db.duckdb" cannot match.
_DB_LOCK_MSG_MARKERS = (
    "set lock",                       # POSIX: 'Could not set lock on file'
    "lock on",                        # POSIX phrasing variant
    "being used by another process",  # Windows file-sharing violation
    "already open in",                # DuckDB 'File is already open in ...'
)

# Double-quoted spans inside the error message (DuckDB wraps the file
# path in double quotes) are removed before marker matching so the path
# text can never produce a false positive.
_QUOTED_SPAN_RE = re.compile(r'"[^"]*"')


def _is_db_lock_error(exc: BaseException) -> bool:
    """Return True if ``exc`` is DuckDB's 'file held by another process'.

    Opening the database read-write while another process holds the write
    lock raises ``duckdb.IOException`` (message varies by platform — see
    ``_DB_LOCK_MSG_MARKERS``).  Only that contention error is retried;
    every other startup failure (bad config, port already bound, missing
    or corrupt DB file, ...) propagates immediately so a real crash is
    not masked behind a 10-minute retry storm.
    """
    if not isinstance(exc, duckdb.IOException):
        return False
    msg = _QUOTED_SPAN_RE.sub("", str(exc).lower())
    return any(marker in msg for marker in _DB_LOCK_MSG_MARKERS)


def _main_with_db_retry() -> None:
    """Run ``src.main.main()``, retrying DuckDB write-lock contention.

    Each attempt calls ``main()`` end-to-end; a lock-contention failure
    before ``server.serve()`` backs off and retries until
    ``_DB_OPEN_RETRY_WINDOW_S`` is exhausted, then re-raises so Task
    Scheduler records the failure and applies its own restart policy.
    A graceful shutdown inside ``main()`` raises ``SystemExit`` (a
    ``BaseException``), which is deliberately not caught.
    """
    deadline = time.monotonic() + _DB_OPEN_RETRY_WINDOW_S
    # main() attaches fresh root logging handlers on every call; remember
    # the pre-first-call baseline so retries can drop the duplicates.
    baseline_handlers = list(logging.getLogger().handlers)
    attempt = 0
    # justified: bounded sequential retry — DuckDB permits a single writer
    # per file, so the service must wait out a fallback daily load that
    # legitimately holds the write lock; there is no SQL/vectorized
    # equivalent for process-level contention retry.  The loop is bounded
    # by ``deadline`` (~10 minutes per launch).
    while True:
        try:
            main()
            return
        except Exception as exc:
            if not _is_db_lock_error(exc):
                raise
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                logger.error(
                    "DuckDB write lock still held after %.0f s of retries; "
                    "giving up so the task records the failure.",
                    _DB_OPEN_RETRY_WINDOW_S,
                )
                raise
            attempt += 1
            delay = min(_DB_OPEN_RETRY_DELAY_S, remaining)
            logger.warning(
                "DuckDB write lock held by another process "
                "(attempt %d: %s); retrying in %.0f s.",
                attempt,
                exc,
                delay,
            )
            logging.getLogger().handlers[:] = baseline_handlers
            time.sleep(delay)


if __name__ == "__main__":
    _main_with_db_retry()
