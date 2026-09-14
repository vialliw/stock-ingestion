"""Tests for the ``ingestion.initial_load_years`` config field (F8-T1).

Parametrized over BOTH implementations:
  - ``src.config.PipelineConfig``            (executor, Pydantic v2)
  - ``src.config_challenger.PipelineConfig`` (challenger, dataclasses)

The field controls the historical depth (in years) of
``IngestionEngine.run_initial_load`` when the caller does not pass an
explicit ``years`` value.  Required semantics (both impls):

  - default is 6 when the key is absent / the file is missing
  - an explicit YAML value is honored
  - values < 1 are rejected (executor: ``Field(ge=1)`` -> ValidationError,
    a ValueError subclass; challenger: ``_validate_initial_load_years``
    -> ValueError)
  - ``null`` (None) means "no value" and falls back to the default
  - booleans are rejected (a bool is not a year count)
  - digit strings are coerced (executor: Pydantic lax coercion;
    challenger: same coercion as ``_validate_daily_lookback_days`` /
    ``_validate_port``)
"""

import sys
import textwrap
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.config import PipelineConfig as ExecutorConfig  # noqa: E402
from src.config_challenger import (  # noqa: E402
    PipelineConfig as ChallengerConfig,
)

IMPLS = [
    pytest.param(ExecutorConfig, id="executor:src.config"),
    pytest.param(ChallengerConfig, id="challenger:src.config_challenger"),
]


def _write_yaml(tmp_path: Path, body: str) -> str:
    p = tmp_path / "cfg.yaml"
    p.write_text(textwrap.dedent(body), encoding="utf-8")
    return str(p)


# ===========================================================================
# Defaults
# ===========================================================================

@pytest.mark.parametrize("cfg_cls", IMPLS)
def test_default_is_six(cfg_cls):
    """Bare construction -> initial_load_years defaults to 6."""
    assert cfg_cls().ingestion.initial_load_years == 6


@pytest.mark.parametrize("cfg_cls", IMPLS)
def test_missing_file_returns_default_six(cfg_cls, tmp_path):
    """from_yaml on a nonexistent path -> all defaults, including 6."""
    cfg = cfg_cls.from_yaml(str(tmp_path / "nonexistent.yaml"))
    assert cfg.ingestion.initial_load_years == 6


@pytest.mark.parametrize("cfg_cls", IMPLS)
def test_absent_key_returns_default_six(cfg_cls, tmp_path):
    """An ``ingestion:`` block without the key -> default 6."""
    path = _write_yaml(tmp_path, """
        ingestion:
          daily_lookback_days: 4
    """)
    cfg = cfg_cls.from_yaml(path)
    assert cfg.ingestion.initial_load_years == 6


@pytest.mark.parametrize("cfg_cls", IMPLS)
def test_null_falls_back_to_default_six(cfg_cls, tmp_path):
    """``initial_load_years: null`` means "no value" -> default 6."""
    path = _write_yaml(tmp_path, """
        ingestion:
          initial_load_years: null
    """)
    cfg = cfg_cls.from_yaml(path)
    assert cfg.ingestion.initial_load_years == 6


# ===========================================================================
# Explicit values
# ===========================================================================

@pytest.mark.parametrize("cfg_cls", IMPLS)
def test_yaml_value_is_read(cfg_cls, tmp_path):
    path = _write_yaml(tmp_path, """
        ingestion:
          initial_load_years: 10
    """)
    cfg = cfg_cls.from_yaml(path)
    assert cfg.ingestion.initial_load_years == 10


@pytest.mark.parametrize("cfg_cls", IMPLS)
def test_boundary_value_one_accepted(cfg_cls, tmp_path):
    """ge=1: the lower bound itself is valid."""
    path = _write_yaml(tmp_path, """
        ingestion:
          initial_load_years: 1
    """)
    cfg = cfg_cls.from_yaml(path)
    assert cfg.ingestion.initial_load_years == 1


@pytest.mark.parametrize("cfg_cls", IMPLS)
def test_digit_string_coerced(cfg_cls, tmp_path):
    """A digit string is coerced to int (same convention as the port /
    daily_lookback_days validators)."""
    path = _write_yaml(tmp_path, """
        ingestion:
          initial_load_years: "12"
    """)
    cfg = cfg_cls.from_yaml(path)
    assert cfg.ingestion.initial_load_years == 12


# ===========================================================================
# Validation failures
# ===========================================================================

@pytest.mark.parametrize("cfg_cls", IMPLS)
@pytest.mark.parametrize("bad", ["0", "-3"])
def test_rejects_values_below_one(cfg_cls, tmp_path, bad):
    """ge=1: zero and negative depths must raise ValueError."""
    path = _write_yaml(tmp_path, f"""
        ingestion:
          initial_load_years: {bad}
    """)
    with pytest.raises(ValueError):
        cfg_cls.from_yaml(path)


@pytest.mark.parametrize("cfg_cls", IMPLS)
def test_boolean_rejected(cfg_cls, tmp_path):
    """``initial_load_years: true`` is a bool, not a year count."""
    path = _write_yaml(tmp_path, """
        ingestion:
          initial_load_years: true
    """)
    with pytest.raises(ValueError):
        cfg_cls.from_yaml(path)


@pytest.mark.parametrize("cfg_cls", IMPLS)
def test_non_numeric_string_rejected(cfg_cls, tmp_path):
    path = _write_yaml(tmp_path, """
        ingestion:
          initial_load_years: "abc"
    """)
    with pytest.raises(ValueError):
        cfg_cls.from_yaml(path)


# ===========================================================================
# Real project config
# ===========================================================================

@pytest.mark.parametrize("cfg_cls", IMPLS)
def test_real_config_yaml_sets_ten(cfg_cls):
    """src/config.yaml pins ``initial_load_years: 10`` (F8-T1 req 3)."""
    cfg = cfg_cls.from_yaml("src/config.yaml")
    assert cfg.ingestion.initial_load_years == 10
