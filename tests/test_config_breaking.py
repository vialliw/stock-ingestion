"""Critic D — BREAKING tests for the config module.

These tests probe edge cases NOT covered by tests/test_config.py and are
designed to expose behavioural divergences / spec-compliance bugs between
the executor (src/config.py, Pydantic) and challenger
(src/config_challenger.py, dataclasses) implementations.

Each test encodes the behaviour the Critic judges to be spec-correct per
docs/tech_spec/gemini_tech_spec.1.0.2.md. Where the two implementations
diverge, the test is expected to FAIL on the non-compliant one — that is
the point: this is the Critic's breaking-test pass.

Spec references:
  - §3 table line 151: duckdb_dir "Created if absent"
  - §3 table line 153/154: tickers/exclude_tickers "Automatically uppercased
    and deduplicated", default []
  - §3 table line 156: flight_port int, "Must be 1-65535. Raises ValueError"
  - §3 validators line 199: port out of range raises ValueError

Findings the Critic is surfacing (PM to adjudicate):
  D1. flight_port: true  -> executor silently accepts as port 1; challenger rejects.
       A boolean is not a port integer; silent acceptance is a data-hygiene bug.
  D2. tickers: null      -> executor raises; challenger returns [].
       null in YAML for a list field means "no value"; default is []; returning []
       is the user-friendly, spec-aligned behaviour.
  D3. exclude_tickers: null -> same as D2.
  D4. db_path side effect -> executor creates duckdb_dir (spec: "Created if absent");
       challenger does NOT create it anywhere -> spec-non-compliant.
  D5. top-level YAML list  -> executor raises TypeError (internal); challenger raises
       ValueError (clean config error). Malformed config should surface as ValueError.
"""

import sys
import textwrap
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.config import PipelineConfig as ExecutorConfig  # noqa: E402
from src.config_challenger import PipelineConfig as ChallengerConfig  # noqa: E402

IMPLS = [
    pytest.param(ExecutorConfig, id="executor:src.config"),
    pytest.param(ChallengerConfig, id="challenger:src.config_challenger"),
]


def _write_yaml(tmp_path: Path, body: str) -> str:
    p = tmp_path / "cfg.yaml"
    p.write_text(textwrap.dedent(body), encoding="utf-8")
    return str(p)


# ===========================================================================
# D1 — Boolean port must be rejected (a bool is not a port integer)
#   Spec: flight_port: int, "Must be 1-65535. Raises ValueError".
#   Executor BUG: silently coerces YAML `true` -> port 1.
#   Challenger: raises ValueError (correct).
# ===========================================================================

@pytest.mark.parametrize("cfg_cls", IMPLS)
def test_boolean_port_rejected(cfg_cls, tmp_path):
    """YAML `flight_port: true` is a boolean, not a port integer; must raise."""
    path = _write_yaml(tmp_path, """
        server:
          flight_port: true
    """)
    with pytest.raises(ValueError):
        cfg_cls.from_yaml(path)


# ===========================================================================
# D2 — tickers: null should yield [] (null means "no value"; default is [])
#   Spec: tickers default [], "Automatically uppercased and deduplicated".
#   Executor BUG: raises ValidationError on null instead of treating as empty.
#   Challenger: returns [] (correct).
# ===========================================================================

@pytest.mark.parametrize("cfg_cls", IMPLS)
def test_tickers_null_returns_empty_list(cfg_cls, tmp_path):
    path = _write_yaml(tmp_path, """
        ingestion:
          tickers: null
    """)
    c = cfg_cls.from_yaml(path)
    assert c.ingestion.tickers == []


# ===========================================================================
# D3 — exclude_tickers: null should yield [] (same rationale as D2)
#   Executor BUG: raises ValidationError on null.
#   Challenger: returns [] (correct).
# ===========================================================================

@pytest.mark.parametrize("cfg_cls", IMPLS)
def test_exclude_tickers_null_returns_empty_list(cfg_cls, tmp_path):
    path = _write_yaml(tmp_path, """
        ingestion:
          exclude_tickers: null
    """)
    c = cfg_cls.from_yaml(path)
    assert c.ingestion.exclude_tickers == []


# ===========================================================================
# D4 — duckdb_dir must be created if absent (spec line 151: "Created if absent")
#   Executor: db_path creates the dir (spec-compliant).
#   Challenger BUG: never creates duckdb_dir anywhere -> spec-non-compliant.
# ===========================================================================

@pytest.mark.parametrize("cfg_cls", IMPLS)
def test_db_path_creates_duckdb_dir_if_absent(cfg_cls, tmp_path):
    db_dir = tmp_path / "fresh_dbdir"
    assert not db_dir.exists()
    path = _write_yaml(tmp_path, f"""
        database:
          duckdb_dir: "{db_dir}"
          duckdb_filename: "test.duckdb"
    """)
    c = cfg_cls.from_yaml(path)
    # Accessing db_path must materialise duckdb_dir per spec ("Created if absent").
    _ = c.db_path
    assert db_dir.exists(), "duckdb_dir must be created if absent (spec line 151)"


# ===========================================================================
# D5 — Malformed top-level YAML (a list, not a mapping) must raise ValueError
#   Executor BUG: raises TypeError (internal Python error leaking through).
#   Challenger: raises ValueError (clean config error, correct).
# ===========================================================================

@pytest.mark.parametrize("cfg_cls", IMPLS)
def test_malformed_top_level_list_raises_value_error(cfg_cls, tmp_path):
    path = _write_yaml(tmp_path, """
        - foo
        - bar
    """)
    with pytest.raises(ValueError):
        cfg_cls.from_yaml(path)


# ===========================================================================
# Robustness checks — both implementations should agree and comply with spec.
# These are not "breaking" but confirm shared spec compliance on edge cases
# the existing test_config.py omits.
# ===========================================================================

@pytest.mark.parametrize("cfg_cls", IMPLS)
def test_float_port_raises_value_error(cfg_cls, tmp_path):
    """A fractional port is not a valid integer port; must raise ValueError."""
    path = _write_yaml(tmp_path, """
        server:
          flight_port: 8815.5
    """)
    with pytest.raises(ValueError):
        cfg_cls.from_yaml(path)


@pytest.mark.parametrize("cfg_cls", IMPLS)
def test_string_scalar_tickers_raises(cfg_cls, tmp_path):
    """tickers must be a list; a bare scalar string is malformed."""
    path = _write_yaml(tmp_path, """
        ingestion:
          tickers: "aapl"
    """)
    with pytest.raises((ValueError, TypeError)):
        cfg_cls.from_yaml(path)


@pytest.mark.parametrize("cfg_cls", IMPLS)
def test_non_string_ticker_raises(cfg_cls, tmp_path):
    """A ticker that is not a string is malformed."""
    path = _write_yaml(tmp_path, """
        ingestion:
          tickers: [123]
    """)
    with pytest.raises((ValueError, TypeError)):
        cfg_cls.from_yaml(path)


@pytest.mark.parametrize("cfg_cls", IMPLS)
def test_empty_and_whitespace_tickers_dropped(cfg_cls, tmp_path):
    """Empty / whitespace-only tickers must be dropped (spec: dedup + strip)."""
    path = _write_yaml(tmp_path, """
        ingestion:
          tickers: ["", "  ", "AAPL"]
    """)
    c = cfg_cls.from_yaml(path)
    assert c.ingestion.tickers == ["AAPL"]
