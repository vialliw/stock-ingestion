"""Challenger variant of the configuration module.

Structural difference from src/config.py: this implementation uses plain
Python ``dataclasses`` plus manual YAML parsing and hand-written validation
instead of Pydantic models. The public API is identical so callers can
substitute one for the other.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List

import yaml


# ---------------------------------------------------------------------------
# YAML loading
# ---------------------------------------------------------------------------

# Matches a double-quoted YAML scalar (no escaped quotes handled — none appear
# in the project config). Used only as a fallback for files that contain raw
# backslashes inside double quotes, which strict YAML rejects as unknown
# escapes (e.g. "C:\Intel\parquet").
_DQUOTED_RE = re.compile(r'"([^"]*)"')


def _to_single_quoted(content: str) -> str:
    """Wrap ``content`` as a YAML single-quoted scalar (backslash is literal)."""
    return "'" + content.replace("'", "''") + "'"


def _load_yaml_lenient(text: str) -> Any:
    """Parse YAML, tolerating raw backslashes in double-quoted scalars.

    PyYAML's safe loader rejects double-quoted strings containing unknown
    escape sequences such as ``\\I`` in Windows paths. Real-world config
    files (including this project's ``src/config.yaml``) use double quotes
    around Windows paths purely as delimiters, not to enable C-style escapes.
    On a ScannerError we rewrite every double-quoted scalar that contains a
    backslash into an equivalent single-quoted scalar — in single-quoted YAML
    a backslash is a literal character — and re-parse.
    """
    try:
        return yaml.safe_load(text)
    except yaml.YAMLError:
        def repl(m: "re.Match[str]") -> str:
            content = m.group(1)
            if "\\" in content:
                return _to_single_quoted(content)
            return m.group(0)
        fixed = _DQUOTED_RE.sub(repl, text)
        return yaml.safe_load(fixed)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _normalize_tickers(tickers: Any) -> List[str]:
    """Uppercase, strip, drop empties, deduplicate preserving first-seen order."""
    if tickers is None:
        return []
    if not isinstance(tickers, list):
        raise TypeError(f"tickers must be a list, got {type(tickers).__name__}")
    seen: set[str] = set()
    result: List[str] = []
    for t in tickers:
        if not isinstance(t, str):
            raise TypeError(f"ticker must be a str, got {type(t).__name__}")
        upper = t.strip().upper()
        if upper and upper not in seen:
            seen.add(upper)
            result.append(upper)
    return result


def _validate_port(port: Any) -> int:
    """Coerce to int and enforce the 1-65535 range."""
    if isinstance(port, bool) or not isinstance(port, int):
        # YAML may parse ports as int already; reject non-ints explicitly.
        if isinstance(port, str) and port.strip().lstrip("+-").isdigit():
            port = int(port)
        else:
            raise ValueError(f"flight_port must be an integer, got {port!r}")
    if not 1 <= port <= 65535:
        raise ValueError(f"flight_port must be 1-65535, got {port}")
    return port


def _coerce_str(value: Any, default: str) -> str:
    if value is None:
        return default
    if not isinstance(value, str):
        raise TypeError(f"expected str, got {type(value).__name__}")
    return value


# ---------------------------------------------------------------------------
# Nested config dataclasses
# ---------------------------------------------------------------------------

@dataclass
class DatabaseConfig:
    duckdb_dir: str = "./data"
    duckdb_filename: str = "market_data.duckdb"

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DatabaseConfig":
        data = data or {}
        return cls(
            duckdb_dir=_coerce_str(data.get("duckdb_dir"), "./data"),
            duckdb_filename=_coerce_str(data.get("duckdb_filename"), "market_data.duckdb"),
        )


@dataclass
class IngestionConfig:
    tickers: List[str] = field(default_factory=list)
    exclude_tickers: List[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "IngestionConfig":
        data = data or {}
        return cls(
            tickers=_normalize_tickers(data.get("tickers", [])),
            exclude_tickers=_normalize_tickers(data.get("exclude_tickers", [])),
        )


@dataclass
class ServerConfig:
    flight_host: str = "127.0.0.1"
    flight_port: int = 8815

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ServerConfig":
        data = data or {}
        return cls(
            flight_host=_coerce_str(data.get("flight_host"), "127.0.0.1"),
            flight_port=_validate_port(data.get("flight_port", 8815)),
        )


@dataclass
class LoggingConfig:
    log_file: str = "./logs/pipeline.log"

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "LoggingConfig":
        data = data or {}
        return cls(
            log_file=_coerce_str(data.get("log_file"), "./logs/pipeline.log"),
        )


@dataclass
class ReportsConfig:
    report_dir: str = "./data/reports"

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ReportsConfig":
        data = data or {}
        return cls(
            report_dir=_coerce_str(data.get("report_dir"), "./data/reports"),
        )


# ---------------------------------------------------------------------------
# Top-level config
# ---------------------------------------------------------------------------

@dataclass
class PipelineConfig:
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    ingestion: IngestionConfig = field(default_factory=IngestionConfig)
    server: ServerConfig = field(default_factory=ServerConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    reports: ReportsConfig = field(default_factory=ReportsConfig)

    # -- properties ---------------------------------------------------------

    @property
    def db_path(self) -> str:
        """Join ``duckdb_dir`` and ``duckdb_filename`` into a full DB path.

        Per spec §3 (line 151) ``duckdb_dir`` is "Created if absent":
        accessing this property materialises the directory with
        ``os.makedirs(..., exist_ok=True)`` so callers can immediately
        open a DuckDB file at the returned path without a separate
        mkdir step.
        """
        os.makedirs(self.database.duckdb_dir, exist_ok=True)
        return os.path.join(self.database.duckdb_dir, self.database.duckdb_filename)

    # -- loading ------------------------------------------------------------

    @classmethod
    def from_yaml(cls, path: str = "src/config.yaml") -> "PipelineConfig":
        """Load a PipelineConfig from a YAML file.

        If the file is missing, return a config populated entirely with
        defaults (no error raised).
        """
        if not path or not os.path.exists(path):
            return cls()

        with open(path, "r", encoding="utf-8") as fh:
            raw: Any = _load_yaml_lenient(fh.read())

        if raw is None:
            return cls()
        if not isinstance(raw, dict):
            raise ValueError(
                f"Top-level YAML in {path!r} must be a mapping, got {type(raw).__name__}"
            )

        # Only keys we know about; ignore extras silently to mirror the
        # lenient behaviour of the spec's example loader.
        return cls(
            database=DatabaseConfig.from_dict(raw.get("database", {})),
            ingestion=IngestionConfig.from_dict(raw.get("ingestion", {})),
            server=ServerConfig.from_dict(raw.get("server", {})),
            logging=LoggingConfig.from_dict(raw.get("logging", {})),
            reports=ReportsConfig.from_dict(raw.get("reports", {})),
        )


__all__ = [
    "DatabaseConfig",
    "IngestionConfig",
    "ServerConfig",
    "LoggingConfig",
    "ReportsConfig",
    "PipelineConfig",
]
