"""Configuration module for the stock-ingestion pipeline.

Defines Pydantic v2 nested models that mirror the YAML structure in
``src/config.yaml``.  ``PipelineConfig.from_yaml`` loads the user's config
file (falling back to defaults when the file is missing) and field validators
enforce data hygiene on tickers and the Flight server port.

Spec reference: docs/tech_spec/gemini_tech_spec.1.0.2.md §3 (Configuration).
"""

from __future__ import annotations

import os
import re
from typing import List, Literal

import yaml
from pydantic import BaseModel, Field, field_validator


# =====================================================================
# 1. CONFIGURATION
# =====================================================================

class DatabaseConfig(BaseModel):
    """DuckDB storage location."""

    duckdb_dir: str = "./data"
    duckdb_filename: str = "market_data.duckdb"


class IngestionConfig(BaseModel):
    """Ticker universe + blacklist.

    Both lists are automatically uppercased, stripped of surrounding
    whitespace, and deduplicated (preserving first-seen order) via the
    ``normalize_tickers`` field validator.
    """

    tickers: List[str] = Field(default_factory=list)
    exclude_tickers: List[str] = Field(default_factory=list)

    @field_validator("tickers", "exclude_tickers", mode="before")
    @classmethod
    def normalize_tickers(cls, v) -> List[str]:
        """Uppercase, strip whitespace, deduplicate, preserve order.

        ``None`` (YAML ``null``) is treated as an empty list — null means
        "no value" and the default is ``[]``.  A bare scalar string is
        rejected (tickers must be a list).  Non-string elements are rejected.
        """
        if v is None:
            return []
        if isinstance(v, str):
            raise ValueError("tickers must be a list, got a string scalar")
        if not isinstance(v, (list, tuple)):
            raise ValueError(f"tickers must be a list, got {type(v).__name__}")
        seen: set[str] = set()
        result: List[str] = []
        # justified: per-element type validation cannot be vectorized —
        # we must raise ValueError on the first non-string element with
        # its value in the message. Vectorized str ops would silently
        # coerce or mask the error.
        for t in v:
            if not isinstance(t, str):
                raise ValueError(f"ticker must be a string, got {type(t).__name__} {t!r}")
            upper = t.strip().upper()
            if upper and upper not in seen:
                seen.add(upper)
                result.append(upper)
        return result


class ServerConfig(BaseModel):
    """Arrow Flight server bind settings."""

    flight_host: str = "127.0.0.1"
    flight_port: int = 8815
    query_timeout_seconds: int = Field(default=120, ge=1)

    @field_validator("flight_port", mode="before")
    @classmethod
    def validate_port(cls, v) -> int:
        """Ensure the Flight port is an int in the valid 1-65535 range.

        Booleans are rejected explicitly — in YAML ``true`` is a bool, not
        a port integer, and Pydantic's lax mode would silently coerce it
        to ``1``.
        """
        if isinstance(v, bool):
            raise ValueError(f"flight_port must be an integer 1-65535, got bool {v}")
        if not isinstance(v, int):
            raise ValueError(f"flight_port must be an integer 1-65535, got {type(v).__name__} {v}")
        if not 1 <= v <= 65535:
            raise ValueError(f"flight_port must be 1-65535, got {v}")
        return v


class LoggingConfig(BaseModel):
    """Log file path (parent directory created lazily by callers)."""

    log_file: str = "./logs/pipeline.log"
    log_format: Literal["text", "json"] = "text"


class ReportsConfig(BaseModel):
    """Directory for generated report JSON files."""

    report_dir: str = "./data/reports"


class R2Config(BaseModel):
    """Cloudflare R2 sync settings.

    Syncing is opt-in via ``enabled``.  Credentials are never stored here;
    they are read from environment variables at sync time.
    """

    enabled: bool = False
    bucket_name: str = ""
    prefix: str = "stock-ingestion/"


class SchedulerConfig(BaseModel):
    """APScheduler daily-load settings.

    The scheduler is opt-in via ``enabled``.  When enabled, it triggers
    ``IngestionEngine.run_daily_load`` once per trading day at
    ``schedule_time`` in the configured ``timezone``.
    """

    enabled: bool = False
    schedule_time: str = "16:30"
    timezone: str = "America/New_York"

    @field_validator("schedule_time", mode="before")
    @classmethod
    def validate_schedule_time(cls, v) -> str:
        """Ensure ``schedule_time`` is a string in ``HH:MM`` 24-hour form."""
        if v is None:
            return "16:30"
        if not isinstance(v, str):
            raise ValueError(f"schedule_time must be a string, got {type(v).__name__}")
        if not re.fullmatch(r"^(?:[01]\d|2[0-3]):[0-5]\d$", v):
            raise ValueError(f"schedule_time must be in HH:MM 24-hour format, got {v!r}")
        return v


class PipelineConfig(BaseModel):
    """Top-level pipeline configuration aggregating all sub-configs."""

    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    ingestion: IngestionConfig = Field(default_factory=IngestionConfig)
    server: ServerConfig = Field(default_factory=ServerConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    reports: ReportsConfig = Field(default_factory=ReportsConfig)
    r2: R2Config = Field(default_factory=R2Config)
    scheduler: SchedulerConfig = Field(default_factory=SchedulerConfig)

    @property
    def db_path(self) -> str:
        """Full path to the DuckDB file.

        Joins ``database.duckdb_dir`` and ``database.duckdb_filename``,
        creating ``duckdb_dir`` if it does not already exist.
        """
        os.makedirs(self.database.duckdb_dir, exist_ok=True)
        return os.path.join(self.database.duckdb_dir, self.database.duckdb_filename)

    @classmethod
    def from_yaml(cls, path: str = "src/config.yaml") -> "PipelineConfig":
        """Load configuration from a YAML file.

        If ``path`` does not exist, a ``PipelineConfig`` with all Pydantic
        defaults is returned.  Otherwise the YAML is parsed and its nested
        keys are mapped directly onto the nested sub-models.

        Note: the user's ``src/config.yaml`` stores Windows paths inside
        double-quoted YAML scalars (e.g. ``"C:\\Intel\\parquet"``).  Per the
        YAML spec double-quoted scalars interpret backslash escapes, and
        ``\\I`` is not a valid escape sequence, so a strict parser raises
        ``ScannerError``.  Because we must not modify the user's config file,
        we convert double-quoted scalars to single-quoted (literal) form
        before parsing — single-quoted scalars treat backslashes literally.
        This is safe for this file because no value contains an embedded
        single quote.
        """
        if not os.path.exists(path):
            return cls()
        with open(path) as f:
            raw = f.read()
        # Convert double-quoted scalars to single-quoted so backslashes in
        # Windows paths are treated literally instead of as escape chars.
        raw = _DOUBLE_QUOTED_TO_SINGLE.sub(r"'\1'", raw)
        data = yaml.safe_load(raw) or {}
        if not isinstance(data, dict):
            raise ValueError(
                f"Configuration file must contain a YAML mapping at the top level, "
                f"got {type(data).__name__}"
            )
        return cls(**data)


# Matches a double-quoted YAML scalar (no embedded double quotes in this file)
# so it can be rewritten as a single-quoted scalar for literal backslash handling.
_DOUBLE_QUOTED_TO_SINGLE = re.compile(r'"([^"]*)"')
