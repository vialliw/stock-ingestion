"""Standalone scheduler entrypoint.

Loads configuration, sets up logging, and starts the APScheduler-based daily
load scheduler.  This process does **not** start the Arrow Flight server.
"""

from __future__ import annotations

import logging
import os
import signal
import sys

from pythonjsonlogger import json as jsonlogger

from src.config import PipelineConfig
from src.scheduler import PipelineScheduler

logger = logging.getLogger(__name__)

_TEXT_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
_JSON_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"


def _configure_logging(config: PipelineConfig) -> None:
    """Configure root logging: console (stderr) + file at ``cfg.logging.log_file``.

    Mirrors the logging setup in ``src/main.py`` but avoids importing the
    Flight server module, keeping the scheduler process self-contained.
    """
    log_dir = os.path.dirname(config.logging.log_file)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)

    if config.logging.log_format == "json":
        formatter: logging.Formatter = jsonlogger.JsonFormatter(_JSON_FORMAT)
    else:
        formatter = logging.Formatter(_TEXT_FORMAT)

    root = logging.getLogger()
    root.setLevel(logging.INFO)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    root.addHandler(stream_handler)

    file_handler = logging.FileHandler(
        config.logging.log_file, encoding="utf-8"
    )
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)


def main() -> None:
    """Start the standalone scheduler process."""
    config = PipelineConfig.from_yaml("src/config.yaml")
    _configure_logging(config)

    scheduler = PipelineScheduler(config)

    def shutdown_handler(signum, _frame):
        logger.warning(
            "Shutdown signal %s trapped. Stopping scheduler...", signum
        )
        scheduler.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    logger.info("Starting standalone scheduler process.")
    scheduler.start(block=True)
    logger.info("Scheduler process exiting.")


if __name__ == "__main__":
    main()
