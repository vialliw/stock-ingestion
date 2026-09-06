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
import signal
import sys

from src.config import PipelineConfig
from src.db import DatabaseManager
from src.ingestion import IngestionEngine
from src.flight_server import MarketDataFlightServer

# =====================================================================
# Logging — basic config so daemon output is visible on stderr/stdout.
# =====================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> None:
    """Assemble and run the stock-ingestion Flight daemon."""
    config = PipelineConfig.from_yaml("src/config.yaml")
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
