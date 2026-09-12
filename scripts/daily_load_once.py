"""One-shot daily load runner for Windows Task Scheduler.

Runs a single ``PipelineScheduler.run_once()`` cycle using the default
``src/config.yaml`` — the same code path as the APScheduler daemon
(NYSE market check, DuckDB lock retry, ``run_daily_load``, optional
R2 sync).  Unlike ``src/scheduler_main.py`` this does not block; the
process exits when the cycle finishes.

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

from src.config import PipelineConfig
from src.scheduler import PipelineScheduler

_TEXT_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"


def _configure_logging(log_file: str) -> None:
    """Log to the configured file (Task Scheduler has no console)."""
    log_dir = os.path.dirname(log_file)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)

    root = logging.getLogger()
    root.setLevel(logging.INFO)

    handler = logging.FileHandler(log_file, encoding="utf-8")
    handler.setFormatter(logging.Formatter(_TEXT_FORMAT))
    root.addHandler(handler)


def main() -> None:
    config = PipelineConfig.from_yaml(
        os.path.join(_REPO_ROOT, "src", "config.yaml")
    )
    _configure_logging(config.logging.log_file)
    PipelineScheduler(config).run_once()


if __name__ == "__main__":
    main()
