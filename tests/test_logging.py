"""Critic tests for JSON logging + log_file path + format validation.

Covers TASKS.json F7-T1c acceptance criteria:
  (4) log_format field exists in config with validation (Literal['text','json'])
  (5) main.py writes to configured log_file path (not just console)
  (6) JSON format produces valid JSON parseable lines
  (7) text format produces human-readable lines
"""

import json
import logging
import os
import sys
import textwrap
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.config import PipelineConfig, LoggingConfig  # noqa: E402
from src.main import _configure_logging, _TEXT_FORMAT, _JSON_FORMAT  # noqa: E402


def _write_yaml(tmp_path: Path, body: str) -> str:
    p = tmp_path / "cfg.yaml"
    p.write_text(textwrap.dedent(body), encoding="utf-8")
    return str(p)


def _reset_root_logger():
    """Remove all handlers from the root logger (cleanup between tests)."""
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)
        try:
            h.close()
        except Exception:
            pass


# ===========================================================================
# (4) log_format field exists in config with validation
# ===========================================================================

def test_log_format_field_exists_with_default():
    """LoggingConfig has a log_format field defaulting to 'text'."""
    cfg = LoggingConfig()
    assert cfg.log_format == "text"


def test_log_format_accepts_json():
    cfg = LoggingConfig(log_format="json")
    assert cfg.log_format == "json"


def test_log_format_rejects_invalid_value():
    """Pydantic Literal['text','json'] rejects values outside the allowed set."""
    from pydantic import ValidationError
    with pytest.raises(ValidationError):
        LoggingConfig(log_format="xml")


def test_log_format_rejects_empty_string():
    from pydantic import ValidationError
    with pytest.raises(ValidationError):
        LoggingConfig(log_format="")


def test_log_format_rejects_none():
    from pydantic import ValidationError
    with pytest.raises(ValidationError):
        LoggingConfig(log_format=None)


def test_log_format_loaded_from_yaml():
    """log_format is loaded from YAML config."""
    p = _write_yaml(Path(os.path.dirname(__file__)) / "tmp_dummy", """
        logging:
          log_format: "json"
    """) if False else None  # use tmp_path instead
    # This test uses tmp_path fixture below — placeholder removed


def test_log_format_loaded_from_yaml_via_tmp(tmp_path):
    p = _write_yaml(tmp_path, """
        logging:
          log_format: "json"
    """)
    c = PipelineConfig.from_yaml(p)
    assert c.logging.log_format == "json"


def test_log_format_defaults_to_text_when_not_in_yaml(tmp_path):
    p = _write_yaml(tmp_path, """
        logging:
          log_file: "/tmp/x.log"
    """)
    c = PipelineConfig.from_yaml(p)
    assert c.logging.log_format == "text"


# ===========================================================================
# (5) main.py writes to configured log_file path (not just console)
# ===========================================================================

def test_configure_logging_creates_log_file(tmp_path):
    """_configure_logging creates a file handler at the configured path."""
    _reset_root_logger()
    log_path = tmp_path / "subdir" / "pipeline.log"
    p = _write_yaml(tmp_path, f"""
        logging:
          log_file: "{log_path}"
          log_format: "text"
    """)
    cfg = PipelineConfig.from_yaml(p)
    _configure_logging(cfg)
    logger = logging.getLogger("test_writes_to_file")
    logger.info("hello file")
    # Flush all handlers
    for h in logging.getLogger().handlers:
        h.flush()
    assert log_path.exists(), "log_file must be created by _configure_logging"
    content = log_path.read_text(encoding="utf-8")
    assert "hello file" in content
    _reset_root_logger()


def test_configure_logging_creates_nested_log_dir(tmp_path):
    """Parent directory of log_file is created if absent."""
    _reset_root_logger()
    log_path = tmp_path / "a" / "b" / "c" / "deep.log"
    p = _write_yaml(tmp_path, f"""
        logging:
          log_file: "{log_path}"
          log_format: "text"
    """)
    cfg = PipelineConfig.from_yaml(p)
    _configure_logging(cfg)
    logging.getLogger("test_nested").info("nested dir test")
    for h in logging.getLogger().handlers:
        h.flush()
    assert log_path.exists()
    _reset_root_logger()


def test_configure_logging_has_both_stream_and_file_handlers(tmp_path):
    """_configure_logging attaches both a StreamHandler and a FileHandler."""
    _reset_root_logger()
    log_path = tmp_path / "both.log"
    p = _write_yaml(tmp_path, f"""
        logging:
          log_file: "{log_path}"
          log_format: "text"
    """)
    cfg = PipelineConfig.from_yaml(p)
    _configure_logging(cfg)
    root = logging.getLogger()
    has_stream = any(isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler) for h in root.handlers)
    has_file = any(isinstance(h, logging.FileHandler) for h in root.handlers)
    assert has_stream, "must have a StreamHandler (console)"
    assert has_file, "must have a FileHandler (file)"
    _reset_root_logger()


# ===========================================================================
# (6) JSON format produces valid JSON parseable lines
# ===========================================================================

def test_json_format_produces_valid_json(tmp_path):
    """Each log line in JSON mode is a valid JSON object parseable by json.loads."""
    _reset_root_logger()
    log_path = tmp_path / "json.log"
    p = _write_yaml(tmp_path, f"""
        logging:
          log_file: "{log_path}"
          log_format: "json"
    """)
    cfg = PipelineConfig.from_yaml(p)
    _configure_logging(cfg)
    logger = logging.getLogger("test_json")
    logger.info("hello json")
    logger.warning("warn message")
    for h in logging.getLogger().handlers:
        h.flush()
    content = log_path.read_text(encoding="utf-8").strip()
    lines = [l for l in content.splitlines() if l.strip()]
    assert len(lines) >= 2, "expected at least 2 JSON log lines"
    for line in lines:
        obj = json.loads(line)  # raises if not valid JSON
        assert "message" in obj
        assert "levelname" in obj
    _reset_root_logger()


def test_json_format_includes_required_fields(tmp_path):
    """JSON log lines include asctime, levelname, name, message fields."""
    _reset_root_logger()
    log_path = tmp_path / "json_fields.log"
    p = _write_yaml(tmp_path, f"""
        logging:
          log_file: "{log_path}"
          log_format: "json"
    """)
    cfg = PipelineConfig.from_yaml(p)
    _configure_logging(cfg)
    logging.getLogger("test_json_fields").info("field check")
    for h in logging.getLogger().handlers:
        h.flush()
    line = log_path.read_text(encoding="utf-8").strip().splitlines()[0]
    obj = json.loads(line)
    assert "asctime" in obj
    assert "levelname" in obj
    assert "name" in obj
    assert "message" in obj
    assert obj["message"] == "field check"
    assert obj["levelname"] == "INFO"
    _reset_root_logger()


def test_json_format_exception_traceback(tmp_path):
    """JSON format logs exception info as a valid JSON line."""
    _reset_root_logger()
    log_path = tmp_path / "json_exc.log"
    p = _write_yaml(tmp_path, f"""
        logging:
          log_file: "{log_path}"
          log_format: "json"
    """)
    cfg = PipelineConfig.from_yaml(p)
    _configure_logging(cfg)
    logger = logging.getLogger("test_json_exc")
    try:
        raise ValueError("test exc")
    except ValueError:
        logger.exception("exception occurred")
    for h in logging.getLogger().handlers:
        h.flush()
    lines = [l for l in log_path.read_text(encoding="utf-8").strip().splitlines() if l.strip()]
    # At least one line should be valid JSON with exc_info
    found_exc = False
    for line in lines:
        obj = json.loads(line)
        if obj.get("message") == "exception occurred":
            found_exc = True
    assert found_exc, "exception log line not found"
    _reset_root_logger()


# ===========================================================================
# (7) Text format produces human-readable lines
# ===========================================================================

def test_text_format_produces_readable_lines(tmp_path):
    """Text format produces human-readable plain-text lines with timestamp + level."""
    _reset_root_logger()
    log_path = tmp_path / "text.log"
    p = _write_yaml(tmp_path, f"""
        logging:
          log_file: "{log_path}"
          log_format: "text"
    """)
    cfg = PipelineConfig.from_yaml(p)
    _configure_logging(cfg)
    logging.getLogger("test_text").info("hello text")
    for h in logging.getLogger().handlers:
        h.flush()
    content = log_path.read_text(encoding="utf-8").strip()
    # Should NOT be JSON — should be plain text containing the message
    assert "hello text" in content
    assert "[INFO]" in content
    # Should not be parseable as JSON (it's plain text)
    with pytest.raises(json.JSONDecodeError):
        json.loads(content.splitlines()[0])
    _reset_root_logger()


def test_text_format_includes_levelname_and_name(tmp_path):
    """Text format includes [LEVEL] and logger name."""
    _reset_root_logger()
    log_path = tmp_path / "text_fields.log"
    p = _write_yaml(tmp_path, f"""
        logging:
          log_file: "{log_path}"
          log_format: "text"
    """)
    cfg = PipelineConfig.from_yaml(p)
    _configure_logging(cfg)
    logging.getLogger("my_module").warning("warn text")
    for h in logging.getLogger().handlers:
        h.flush()
    content = log_path.read_text(encoding="utf-8").strip()
    assert "[WARNING]" in content
    assert "my_module" in content
    assert "warn text" in content
    _reset_root_logger()


def test_text_format_all_log_levels(tmp_path):
    """Text format emits DEBUG/INFO/WARNING/ERROR/CRITICAL."""
    _reset_root_logger()
    log_path = tmp_path / "text_levels.log"
    p = _write_yaml(tmp_path, f"""
        logging:
          log_file: "{log_path}"
          log_format: "text"
    """)
    cfg = PipelineConfig.from_yaml(p)
    _configure_logging(cfg)
    logger = logging.getLogger("test_levels")
    logger.setLevel(logging.DEBUG)
    logger.debug("dbg")
    logger.info("inf")
    logger.warning("wrn")
    logger.error("err")
    logger.critical("crit")
    for h in logging.getLogger().handlers:
        h.flush()
    content = log_path.read_text(encoding="utf-8")
    assert "[DEBUG]" in content
    assert "[INFO]" in content
    assert "[WARNING]" in content
    assert "[ERROR]" in content
    assert "[CRITICAL]" in content
    _reset_root_logger()


# ===========================================================================
# Real config.yaml loads log_format correctly
# ===========================================================================

def test_real_config_has_log_format():
    """The user's real src/config.yaml has a log_format field."""
    c = PipelineConfig.from_yaml(str(PROJECT_ROOT / "src" / "config.yaml"))
    assert c.logging.log_format in ("text", "json")
