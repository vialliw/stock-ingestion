"""Launch the Arrow Flight server from any working directory.

Thin, cwd-safe wrapper around ``src.main.main()`` intended for the
``StockInestion-FlightServer`` Windows scheduled task (registered by
``scripts/register_flight_service.ps1``) and for manual startup:

    python scripts/run_flight_server.py

Windows Task Scheduler does not guarantee a working directory, and
``src.main.main()`` loads ``src/config.yaml`` via a repo-relative path.
This script therefore anchors the repo root on ``sys.path`` (so ``src``
is importable) and makes it the process working directory before
delegating — the same header pattern used by
``scripts/daily_load_once.py``.
"""

from __future__ import annotations

import os
import sys

# Ensure src/ is importable regardless of the task's working directory.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _REPO_ROOT)
os.chdir(_REPO_ROOT)

from src.main import main

if __name__ == "__main__":
    main()
