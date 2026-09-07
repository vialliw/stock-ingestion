"""Main entrypoint for the stock-ingestion pipeline.

Assembles the production modules (config -> DatabaseManager ->
IngestionEngine -> MarketDataFlightServer), installs SIGINT/SIGTERM
handlers that safely close the DuckDB file, and starts the Arrow Flight
daemon.

Spec reference: docs/tech_spec/gemini_tech_spec.1.0.2.md §10 (Reference
Implementation, main() function).

Uses the EXECUTOR's production modules (config.py, db.py, ingestion.py,
flight_server.py) — NOT the challenger variants.
"""

from __future__ import annotations

import logging
import os
import signal
import sys

from pythonjsonlogger import json as jsonlogger

from src.config import PipelineConfig
from src.db import DatabaseManager
from src.ingestion import IngestionEngine
from src.flight_server import MarketDataFlightServer

logger = logging.getLogger(__name__)

_TEXT_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
_JSON_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"


def _configure_logging(config: PipelineConfig) -> None:
    """Configure root logging: console (stderr) + file at cfg.logging.log_file.

    ``log_format`` selects the formatter: ``'text'`` (default) uses the
    human-readable plain-text format, ``'json'`` uses
    ``pythonjsonlogger.jsonlogger.JsonFormatter`` so every record is emitted
    as a single JSON line. All existing ``logger.*`` calls work unchanged.
    The log file's parent directory is created if absent.
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
    # justified: attaching two handler objects to the root logger is an
    # external API side-effect call, not a data transformation — there is
    # no SQL/vectorized equivalent for configuring logging handlers.
    for handler in (logging.StreamHandler(), logging.FileHandler(config.logging.log_file, encoding="utf-8")):
        handler.setFormatter(formatter)
        root.addHandler(handler)


def main() -> None:
    """Assemble and run the stock-ingestion Flight daemon."""
    config = PipelineConfig.from_yaml("src/config.yaml")
    _configure_logging(config)
    db_manager = DatabaseManager(config.db_path)
    ingest_engine = IngestionEngine(db_manager, config)

    def shutdown_handler(signum, frame):
        logger.warning(
            f"Shutdown signal {signum} trapped. "
            "Commencing emergency WAL checkpoint..."
        )
        db_manager.close()
        logger.info("Database closed safely. Exiting daemon process.")
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    location = f"grpc://{config.server.flight_host}:{config.server.flight_port}"
    server = MarketDataFlightServer(location, db_manager, ingest_engine, config)
    logger.info(f"Apache Arrow Flight Daemon operational on {location}")
    server.serve()


if __name__ == "__main__":
    main()
