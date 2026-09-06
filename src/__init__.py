"""stock-ingestion source package.

Production modules:
  - config: Pydantic v2 nested config models + from_yaml loader
  - db: DuckDB DatabaseManager with thread-safe writes + read-only reads
  - ingestion: yfinance IngestionEngine with split detection (DELETE + re-download)
  - flight_server: Apache Arrow Flight server with SELECT-only do_get + 4 do_action endpoints
  - main: entrypoint that assembles all components

Challenger variants (*_challenger.py) remain in the package for reference
but are NOT imported here. They were validated via the multi-agent duel
workflow and passed all Critic tests, but the executor variants were
selected as production modules per the dev plan.
"""
from src.config import PipelineConfig
from src.db import DatabaseManager
from src.ingestion import IngestionEngine
from src.flight_server import MarketDataFlightServer

__all__ = [
    "PipelineConfig",
    "DatabaseManager",
    "IngestionEngine",
    "MarketDataFlightServer",
]
