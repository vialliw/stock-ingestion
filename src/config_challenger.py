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
from datetime import datetime
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
        # justified: per-element type validation cannot be vectorized —
        # we must raise TypeError on the first non-string element with
        # its value in the message.
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


def _validate_daily_lookback_days(value: Any) -> int:
    """Coerce to int and enforce ``>= 1``.

    Mirrors the executor's ``daily_lookback_days: int = Field(default=4,
    ge=1)`` in ``src/config.py``: the window is a positive count of
    trailing calendar days.  ``None`` (YAML ``null``) means "no value"
    and falls back to the default — consistent with this module's
    ``_coerce_str`` convention.  Booleans and non-numeric values are
    rejected; digit strings are coerced like ``_validate_port``.
    """
    if value is None:
        return 4
    if isinstance(value, bool) or not isinstance(value, int):
        if isinstance(value, str) and value.strip().lstrip("+-").isdigit():
            value = int(value)
        else:
            raise ValueError(
                f"daily_lookback_days must be an integer, got {value!r}"
            )
    if value < 1:
        raise ValueError(f"daily_lookback_days must be >= 1, got {value}")
    return value


def _validate_initial_load_years(value: Any) -> int:
    """Coerce to int and enforce ``>= 1``.

    Mirrors the executor's ``initial_load_years: int = Field(default=6,
    ge=1)`` in ``src/config.py``: the depth of the initial historical
    load is a positive count of years.  ``None`` (YAML ``null`` or an
    absent key) falls back to the default — consistent with
    ``_validate_daily_lookback_days`` and this module's ``_coerce_str``
    convention.  Booleans and non-numeric values are rejected; digit
    strings are coerced like ``_validate_port``.
    """
    if value is None:
        return 6
    if isinstance(value, bool) or not isinstance(value, int):
        if isinstance(value, str) and value.strip().lstrip("+-").isdigit():
            value = int(value)
        else:
            raise ValueError(
                f"initial_load_years must be an integer, got {value!r}"
            )
    if value < 1:
        raise ValueError(f"initial_load_years must be >= 1, got {value}")
    return value


def _validate_market_breadth_years(value: Any) -> int:
    """Coerce to int and enforce ``>= 0``.

    Mirrors the executor's ``market_breadth_years: int = Field(default=3,
    ge=0)`` in ``src/config.py``: the default trailing-years window for
    the market-breadth dashboard.  ``0`` is valid and means "all data"
    (the legacy ``years=None`` passthrough).  ``None`` (YAML ``null`` or
    an absent key) falls back to the default — consistent with
    ``_validate_initial_load_years`` and this module's ``_coerce_str``
    convention.  Booleans and non-numeric values are rejected; digit
    strings are coerced like ``_validate_port``.
    """
    if value is None:
        return 3
    if isinstance(value, bool) or not isinstance(value, int):
        if isinstance(value, str) and value.strip().lstrip("+-").isdigit():
            value = int(value)
        else:
            raise ValueError(
                f"market_breadth_years must be an integer, got {value!r}"
            )
    if value < 0:
        raise ValueError(f"market_breadth_years must be >= 0, got {value}")
    return value


def _validate_fg_enabled(value: Any) -> bool:
    """Coerce ``fear_greed.enabled``; ``None`` falls back to True."""
    if value is None:
        return True
    if not isinstance(value, bool):
        raise ValueError(f"fear_greed.enabled must be a bool, got {value!r}")
    return value


def _validate_fg_start_date(value: Any) -> str:
    """``fear_greed.start_date`` must be a real ``YYYY-MM-DD`` date.

    Mirrors the executor's ``FearGreedConfig.validate_start_date`` in
    ``src/config.py``: the value is interpolated verbatim into the CNN
    graphdata URL, so strict shape + real-calendar validation applies.
    ``None`` (YAML ``null`` / absent key) falls back to the default.
    """
    if value is None:
        return "2014-01-01"
    if not isinstance(value, str):
        raise ValueError(
            f"fear_greed.start_date must be a YYYY-MM-DD string, "
            f"got {type(value).__name__} {value!r}"
        )
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise ValueError(
            f"fear_greed.start_date must be in YYYY-MM-DD format, got {value!r}"
        )
    try:
        datetime.strptime(value, "%Y-%m-%d")
    except ValueError:
        raise ValueError(
            f"fear_greed.start_date is not a real calendar date, got {value!r}"
        )
    return value


def _validate_fg_timeout(value: Any) -> float:
    """Coerce ``fear_greed.timeout_seconds`` to a positive number.

    Mirrors ``Field(default=15, gt=0)`` in the executor's
    ``FearGreedConfig``.  ``None`` falls back to the default; bools and
    non-numeric values are rejected; numeric strings are coerced like
    ``_validate_port``.
    """
    if value is None:
        return 15.0
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        if isinstance(value, str):
            try:
                value = float(value)
            except ValueError:
                raise ValueError(
                    f"fear_greed.timeout_seconds must be a number, got {value!r}"
                )
        else:
            raise ValueError(
                f"fear_greed.timeout_seconds must be a number, got {value!r}"
            )
    if value <= 0:
        raise ValueError(
            f"fear_greed.timeout_seconds must be > 0, got {value}"
        )
    return float(value)


def _validate_fg_alert_threshold(value: Any) -> float:
    """Coerce ``fear_greed.alert_threshold`` into the 0-100 band.

    Mirrors ``Field(default=25.0, ge=0, le=100)`` in the executor's
    ``FearGreedConfig`` — the Fear & Greed score is a 0-100 index, so
    the alert level must live in that range.
    """
    if value is None:
        return 25.0
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        if isinstance(value, str):
            try:
                value = float(value)
            except ValueError:
                raise ValueError(
                    f"fear_greed.alert_threshold must be a number, got {value!r}"
                )
        else:
            raise ValueError(
                f"fear_greed.alert_threshold must be a number, got {value!r}"
            )
    if not 0 <= value <= 100:
        raise ValueError(
            f"fear_greed.alert_threshold must be in [0, 100], got {value}"
        )
    return float(value)


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
    # Trailing calendar-day refresh window for run_daily_load — parity
    # with src/config.py IngestionConfig (F7-T19/N1).  Required by
    # flight_server_challenger._handle_daily_load, which reads
    # cfg.ingestion.daily_lookback_days; without it that pairing raised
    # AttributeError.
    daily_lookback_days: int = 4
    # Historical depth (years) for run_initial_load when the caller does
    # not pass an explicit value — parity with src/config.py
    # IngestionConfig (F8-T1).  Read by
    # ingestion_challenger.run_initial_load when years=None.
    initial_load_years: int = 6

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "IngestionConfig":
        data = data or {}
        return cls(
            tickers=_normalize_tickers(data.get("tickers", [])),
            exclude_tickers=_normalize_tickers(data.get("exclude_tickers", [])),
            daily_lookback_days=_validate_daily_lookback_days(
                data.get("daily_lookback_days")
            ),
            initial_load_years=_validate_initial_load_years(
                data.get("initial_load_years")
            ),
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
    # Default trailing-years window for the market-breadth dashboard
    # (scripts/breadth_dashboard.py + the "Market Breadth" report_app
    # tab) — parity with src/config.py ReportsConfig (F11-T1).  0 =
    # all data; the cutoff is applied post-query in pandas.
    market_breadth_years: int = 3

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ReportsConfig":
        data = data or {}
        return cls(
            report_dir=_coerce_str(data.get("report_dir"), "./data/reports"),
            market_breadth_years=_validate_market_breadth_years(
                data.get("market_breadth_years")
            ),
        )


@dataclass
class FearGreedConfig:
    """CNN Fear & Greed index ingestion settings (F16-T1) — parity
    with ``FearGreedConfig`` in ``src/config.py``."""

    enabled: bool = True
    start_date: str = "2014-01-01"
    timeout_seconds: float = 15.0
    alert_threshold: float = 25.0

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "FearGreedConfig":
        data = data or {}
        return cls(
            enabled=_validate_fg_enabled(data.get("enabled")),
            start_date=_validate_fg_start_date(data.get("start_date")),
            timeout_seconds=_validate_fg_timeout(data.get("timeout_seconds")),
            alert_threshold=_validate_fg_alert_threshold(
                data.get("alert_threshold")
            ),
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
    # CNN Fear & Greed ingestion (F16-T1) — read by
    # ingestion_challenger.run_fear_greed_load / _fear_greed_bridge.
    fear_greed: FearGreedConfig = field(default_factory=FearGreedConfig)

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
            fear_greed=FearGreedConfig.from_dict(raw.get("fear_greed", {})),
        )


__all__ = [
    "DatabaseConfig",
    "IngestionConfig",
    "ServerConfig",
    "LoggingConfig",
    "ReportsConfig",
    "FearGreedConfig",
    "PipelineConfig",
]
