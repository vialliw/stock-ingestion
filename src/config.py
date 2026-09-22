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
from datetime import datetime
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
    """Ticker universe, blacklist and daily-load lookback window.

    Both lists are automatically uppercased, stripped of surrounding
    whitespace, and deduplicated (preserving first-seen order) via the
    ``normalize_tickers`` field validator.
    """

    tickers: List[str] = Field(default_factory=list)
    exclude_tickers: List[str] = Field(default_factory=list)
    daily_lookback_days: int = Field(default=4, ge=1)
    initial_load_years: int = Field(default=6, ge=1)

    @field_validator("initial_load_years", mode="before")
    @classmethod
    def validate_initial_load_years(cls, v) -> int:
        """``None`` (YAML ``null`` / absent key) falls back to the
        default — consistent with ``normalize_tickers`` treating null as
        "no value".  Booleans are rejected: a bool is not a year count
        (same rationale as ``validate_port``).
        """
        if v is None:
            return 6
        if isinstance(v, bool):
            raise ValueError(
                f"initial_load_years must be an integer >= 1, got bool {v}"
            )
        return v

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
    """Directory for generated report files + report-specific knobs.

    ``market_breadth_years`` is the default trailing-years window for
    the market-breadth dashboard (``scripts/breadth_dashboard.py`` and
    the "Market Breadth" tab in ``scripts/report_app.py``): ``0`` means
    "all data" (maps to the legacy ``years=None`` passthrough), any
    positive int applies a trailing-N-years pandas cutoff *after* the
    query so the SMA_200/252-day window lookbacks still see pre-cutoff
    history.
    """

    report_dir: str = "./data/reports"
    market_breadth_years: int = Field(default=3, ge=0)

    @field_validator("market_breadth_years", mode="before")
    @classmethod
    def validate_market_breadth_years(cls, v) -> int:
        """``None`` (YAML ``null`` / absent key) falls back to the
        default — consistent with ``validate_initial_load_years``.
        Booleans and non-integer values are rejected (a bool is not a
        year count — same rationale as ``validate_port``); digit
        strings are coerced like ``_validate_port`` does in the
        challenger.  ``0`` is valid (all data); negatives are rejected
        by ``ge=0``.
        """
        if v is None:
            return 3
        if isinstance(v, str) and v.strip().lstrip("+-").isdigit():
            v = int(v)
        if isinstance(v, bool) or not isinstance(v, int):
            raise ValueError(
                f"market_breadth_years must be an integer >= 0, got {v!r}"
            )
        return v


class FearGreedConfig(BaseModel):
    """CNN Fear & Greed index ingestion settings (F16-T1).

    ``enabled`` gates the best-effort ``_fear_greed_bridge`` merged onto
    completed loads.  ``start_date`` is the backfill horizon embedded in
    the CNN graphdata URL.  ``timeout_seconds`` bounds the HTTP GET
    (the API returns 418 without browser headers and can hang).  When
    the latest ``fg_score`` is <= ``alert_threshold`` the load result
    carries ``alert_triggered=True``.
    """

    enabled: bool = True
    start_date: str = "2020-08-01"
    timeout_seconds: float = Field(default=15.0, gt=0)
    alert_threshold: float = Field(default=25.0, ge=0, le=100)

    @field_validator("enabled", mode="before")
    @classmethod
    def validate_enabled(cls, v) -> bool:
        """``None`` falls back to the default; only real bools are
        accepted (Pydantic's lax mode would silently coerce strings
        like ``"yes"``)."""
        if v is None:
            return True
        if not isinstance(v, bool):
            raise ValueError(f"enabled must be a bool, got {v!r}")
        return v

    @field_validator("start_date", mode="before")
    @classmethod
    def validate_start_date(cls, v) -> str:
        """``start_date`` must be a real calendar date in strict
        ``YYYY-MM-DD`` form — it is interpolated verbatim into the CNN
        graphdata URL."""
        if v is None:
            return "2020-08-01"
        if not isinstance(v, str):
            raise ValueError(
                f"start_date must be a YYYY-MM-DD string, got {type(v).__name__} {v!r}"
            )
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", v):
            raise ValueError(f"start_date must be in YYYY-MM-DD format, got {v!r}")
        try:
            datetime.strptime(v, "%Y-%m-%d")
        except ValueError:
            raise ValueError(f"start_date is not a real calendar date, got {v!r}")
        return v

    @field_validator("timeout_seconds", "alert_threshold", mode="before")
    @classmethod
    def validate_numbers(cls, v):
        """Reject bools — ``gt=0`` / ``ge=0, le=100`` would otherwise
        silently accept ``True`` as ``1``."""
        if isinstance(v, bool):
            raise ValueError(f"expected a number, got bool {v}")
        return v


class SignalsConfig(BaseModel):
    """Signal-pipeline settings (F24-T1).

    Server-side port of the legacy ``quant_strategy_building/signal_input``
    orchestrator constants.  ``folder_path`` is the directory scanned for
    ``*_active_signal_combined.xlsx`` files.  The ``min_reward_risk`` /
    ``min_target_pct`` prune thresholds stay ACTIVE (legacy 1.39 / 5.5);
    only the cluster/model exclusion lists default to empty — the
    orchestrator excludes nothing.
    """

    folder_path: str = r"C:\Intel\parquet\quant_strategy_building"
    backdate_days: int = Field(default=3, ge=1)
    max_holding_days: int = Field(default=8, ge=1)
    overwrite_existing: bool = True
    min_reward_risk: float = 1.39
    min_target_pct: float = 5.5
    exclude_clusters: List[int] = Field(default_factory=list)
    exclude_models: List[str] = Field(default_factory=list)
    filter_below_open: bool = False

    @field_validator(
        "backdate_days", "max_holding_days", mode="before"
    )
    @classmethod
    def validate_int_days(cls, v, info) -> int:
        """``None`` falls back to the field default; reject bools —
        ``ge=1`` would silently accept ``True``."""
        if v is None:
            return 3 if info.field_name == "backdate_days" else 8
        if isinstance(v, bool):
            raise ValueError(
                f"{info.field_name} must be an integer >= 1, "
                f"got bool {v}"
            )
        return v

    @field_validator(
        "min_reward_risk", "min_target_pct", mode="before"
    )
    @classmethod
    def validate_thresholds(cls, v, info):
        """``None`` falls back to the field default; reject bools —
        a bool is not a threshold number."""
        if v is None:
            return 1.39 if info.field_name == "min_reward_risk" else 5.5
        if isinstance(v, bool):
            raise ValueError(
                f"{info.field_name} must be a number, got bool {v}"
            )
        return v

    @field_validator(
        "overwrite_existing", "filter_below_open", mode="before"
    )
    @classmethod
    def validate_bools(cls, v, info) -> bool:
        """``None`` falls back to the field default; only real bools
        are accepted (no string coercion)."""
        if v is None:
            return info.field_name == "overwrite_existing"
        if not isinstance(v, bool):
            raise ValueError(
                f"{info.field_name} must be a bool, "
                f"got {type(v).__name__} {v!r}"
            )
        return v

    @field_validator("exclude_clusters", mode="before")
    @classmethod
    def validate_exclude_clusters(cls, v) -> List[int]:
        """``None`` -> ``[]``; every element must be an int (not bool)."""
        if v is None:
            return []
        if not isinstance(v, (list, tuple)):
            raise ValueError(
                f"exclude_clusters must be a list, got {type(v).__name__}"
            )
        # justified: per-element type validation cannot be vectorized —
        # we must raise ValueError on the first non-int element.
        for c in v:
            if isinstance(c, bool) or not isinstance(c, int):
                raise ValueError(
                    f"exclude_clusters elements must be integers, got {c!r}"
                )
        return list(v)

    @field_validator("exclude_models", mode="before")
    @classmethod
    def validate_exclude_models(cls, v) -> List[str]:
        """``None`` -> ``[]``; every element must be a string."""
        if v is None:
            return []
        if not isinstance(v, (list, tuple)):
            raise ValueError(
                f"exclude_models must be a list, got {type(v).__name__}"
            )
        # justified: per-element type validation cannot be vectorized —
        # we must raise ValueError on the first non-string element.
        for m in v:
            if not isinstance(m, str):
                raise ValueError(
                    f"exclude_models elements must be strings, got {m!r}"
                )
        return list(v)


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
    fear_greed: FearGreedConfig = Field(default_factory=FearGreedConfig)
    signals: SignalsConfig = Field(default_factory=SignalsConfig)

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
