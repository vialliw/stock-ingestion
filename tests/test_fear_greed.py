"""Tests for the CNN Fear & Greed ingestion layer (F16-T1).

Parametrized over BOTH implementations:
  - src.ingestion.IngestionEngine            + src.db.DatabaseManager
  - src.ingestion_challenger.IngestionEngine + src.db_challenger.DatabaseManager
  - src.flight_server / src.flight_server_challenger (action wiring)
  - src.config / src.config_challenger (FearGreedConfig validation)

NO real network calls: ``requests.get`` is monkeypatched with a
synthetic CNN payload everywhere.  The live payload shape (verified
2026-09-20) differs from docs/enhancements/2260920.md — the real keys
are ``fear_and_greed_historical`` + the 9 sub-indicator blocks whose
``data`` points carry the RAW metric in ``y`` (not a 0-100 score).

All DB access uses tmp_path-isolated DuckDB files.
"""

from __future__ import annotations

import datetime as _dt
import importlib
import json
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pandas as pd
import pyarrow.flight as flight
import pytest
import requests

from src.config import PipelineConfig
from src.db import (
    SCHEMA_VERSION as _EXECUTOR_SCHEMA_VERSION,
    DatabaseManager as _ExecutorDatabaseManager,
)
from src.db_challenger import (
    SCHEMA_VERSION as _CHALLENGER_SCHEMA_VERSION,
    DatabaseManager as _ChallengerDatabaseManager,
)
from src.ingestion import IngestionEngine as _ExecutorIngestionEngine
from src.ingestion_challenger import (
    IngestionEngine as _ChallengerIngestionEngine,
)


IMPLEMENTATIONS = [
    pytest.param(
        (
            _ExecutorIngestionEngine,
            _ExecutorDatabaseManager,
            "src.ingestion",
            _EXECUTOR_SCHEMA_VERSION,
        ),
        id="executor:src.ingestion",
    ),
    pytest.param(
        (
            _ChallengerIngestionEngine,
            _ChallengerDatabaseManager,
            "src.ingestion_challenger",
            _CHALLENGER_SCHEMA_VERSION,
        ),
        id="challenger:src.ingestion_challenger",
    ),
]

SUB_KEYS = [
    "stock_price_strength",
    "stock_price_breadth",
    "market_momentum_sp500",
    "market_momentum_sp125",
    "market_volatility_vix",
    "market_volatility_vix_50",
    "put_call_options",
    "junk_bond_demand",
    "safe_haven_demand",
]

EXPECTED_TABLE_COLS = (
    ["date", "fg_score", "fg_rating"]
    + [c for k in SUB_KEYS for c in (k, f"{k}_rating")]
    + ["ingested_at"]
)

# 2024-09-12 / 2024-09-13 UTC in ms-epoch (float, like the live API).
X1 = 1726099200000.0
X2 = 1726185600000.0
D1 = _dt.date(2024, 9, 12)
D2 = _dt.date(2024, 9, 13)


def make_payload(**overrides) -> Dict[str, Any]:
    """Synthetic CNN payload: composite history on D1/D2 plus all 9
    sub-indicator blocks (one point on D2 each)."""
    payload: Dict[str, Any] = {
        "fear_and_greed": {"score": 55.0, "rating": "neutral"},
        "fear_and_greed_historical": {
            "timestamp": int(X2),
            "score": 55.0,
            "rating": "neutral",
            "data": [
                {"x": X1, "y": 20.5, "rating": "extreme fear"},
                {"x": X2, "y": 55.0, "rating": "neutral"},
            ],
        },
        "unknown_extra_key": {"data": [{"x": X2, "y": 1.0}]},
    }
    payload.update(
        {
            key: {
                "timestamp": int(X2),
                "score": 50.0,
                "rating": "neutral",
                "data": [
                    {"x": X1, "y": float(i), "rating": "fear"},
                    {"x": X2, "y": float(i) + 100.0, "rating": "neutral"},
                ],
            }
            for i, key in enumerate(SUB_KEYS, start=1)
        }
    )
    payload.update(overrides)
    return payload


def fake_response(payload: Dict[str, Any]) -> MagicMock:
    resp = MagicMock()
    resp.json.return_value = payload
    resp.raise_for_status = MagicMock()
    return resp


@pytest.fixture
def impl(request):
    return request.param


@pytest.fixture
def db(impl, tmp_path):
    _, DbCls, _, _ = impl
    mgr = DbCls(str(tmp_path / "test_fear_greed.duckdb"))
    try:
        yield mgr
    finally:
        mgr.close()


@pytest.fixture
def cfg():
    """Executor PipelineConfig (carries fear_greed); empty tickers so the
    metadata bridge is a no-op and no yfinance call is ever reachable."""
    c = PipelineConfig()
    c.ingestion.tickers = []
    c.ingestion.exclude_tickers = []
    return c


@pytest.fixture
def engine(impl, db, cfg):
    EngineCls, _, _, _ = impl
    return EngineCls(db, cfg)


@pytest.fixture
def module_name(impl):
    """The engine module path (for patching module-local imports)."""
    return impl[2]


def _rows(db) -> Dict[str, list]:
    return db.execute_query(
        "SELECT * FROM fear_greed_history ORDER BY date"
    ).to_pydict()


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("impl", IMPLEMENTATIONS)
def test_fear_greed_table_schema_and_version(impl, db):
    """fear_greed_history exists with the declared columns (interleaved
    <key>/<key>_rating pairs) and schema_version is bumped."""
    _, _, _, schema_version = impl
    cols = db.execute_query(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'fear_greed_history' "
        "ORDER BY ordinal_position"
    ).column("column_name").to_pylist()
    assert cols == EXPECTED_TABLE_COLS
    types = dict(
        zip(
            cols,
            db.execute_query(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_name = 'fear_greed_history' "
                "ORDER BY ordinal_position"
            ).to_pydict()["data_type"],
        )
    )
    assert types["date"] == "DATE"
    assert types["fg_score"] == "DOUBLE"
    assert types["fg_rating"] == "VARCHAR"
    assert types["ingested_at"].startswith("TIMESTAMP")
    ver = db.execute_query(
        "SELECT MAX(version) AS v FROM schema_version"
    ).column("v").to_pylist()[0]
    assert ver == schema_version


# ---------------------------------------------------------------------------
# run_fear_greed_load — happy path
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("impl", IMPLEMENTATIONS)
def test_happy_path_parse_and_upsert(engine, db, cfg, monkeypatch):
    """All 10 series parse into one wide frame; ms epoch -> UTC date;
    every expected column incl. _rating is persisted."""
    monkeypatch.setattr(
        "requests.get", MagicMock(return_value=fake_response(make_payload()))
    )
    result = engine.run_fear_greed_load()

    assert result["status"] == "SUCCESS"
    assert result["rows_upserted"] == 2
    assert result["latest_date"] == "2024-09-13"
    assert result["fg_score"] == 55.0
    assert result["fg_rating"] == "neutral"
    assert result["missing_series"] == []
    assert result["start_date_fallback"] is False
    assert result["fetched_url"].endswith(
        f"/graphdata/{cfg.fear_greed.start_date}"
    )
    assert isinstance(result["duration_ms"], int)

    rows = _rows(db)
    assert rows["date"] == [D1, D2]
    assert rows["fg_score"] == [20.5, 55.0]
    assert rows["fg_rating"] == ["extreme fear", "neutral"]
    for i, key in enumerate(SUB_KEYS, start=1):
        assert rows[key] == [float(i), float(i) + 100.0]
        assert rows[f"{key}_rating"] == ["fear", "neutral"]
    assert all(t is not None for t in rows["ingested_at"])


@pytest.mark.parametrize("impl", IMPLEMENTATIONS)
def test_request_uses_config_url_headers_timeout(
    engine, db, cfg, monkeypatch
):
    """GET targets graphdata/{cfg.fear_greed.start_date} with the
    browser headers and the configured timeout."""
    cfg.fear_greed.start_date = "2014-01-01"
    cfg.fear_greed.timeout_seconds = 15
    mock_get = MagicMock(return_value=fake_response(make_payload()))
    monkeypatch.setattr("requests.get", mock_get)
    engine.run_fear_greed_load()
    args, kwargs = mock_get.call_args
    assert args[0].endswith("/graphdata/2014-01-01")
    assert kwargs["timeout"] == 15
    for header in ("User-Agent", "Referer", "Origin", "Accept"):
        assert header in kwargs["headers"]
    assert kwargs["headers"]["Referer"] == (
        "https://www.cnn.com/markets/fear-and-greed"
    )


@pytest.mark.parametrize("impl", IMPLEMENTATIONS)
def test_explicit_start_date_used_in_url(engine, db, cfg, monkeypatch):
    mock_get = MagicMock(return_value=fake_response(make_payload()))
    monkeypatch.setattr("requests.get", mock_get)
    engine.run_fear_greed_load(start_date="2020-06-30")
    assert mock_get.call_args.args[0].endswith("/graphdata/2020-06-30")


@pytest.mark.parametrize("impl", IMPLEMENTATIONS)
@pytest.mark.parametrize(
    "bad", ["not-a-date", "2014-13-99", "2014-1-1", 20140101, True]
)
def test_bad_start_date_rejected(engine, bad):
    with pytest.raises(ValueError):
        engine.run_fear_greed_load(start_date=bad)


# ---------------------------------------------------------------------------
# start_date fallback (CNN retains only ~6 years of history)
# ---------------------------------------------------------------------------


def _http_error_response(message: str) -> MagicMock:
    """A response whose raise_for_status() raises requests.HTTPError."""
    resp = MagicMock()
    resp.raise_for_status.side_effect = requests.HTTPError(message)
    return resp


@pytest.mark.parametrize("impl", IMPLEMENTATIONS)
def test_http_error_falls_back_to_undated_url(
    engine, db, cfg, monkeypatch
):
    """HTTPError on the dated URL -> ONE retry on the undated base URL;
    start_date_fallback=True and fetched_url is the undated URL."""
    cfg.fear_greed.start_date = "2014-01-01"
    mock_get = MagicMock(
        side_effect=[
            _http_error_response("500 Server Error"),
            fake_response(make_payload()),
        ]
    )
    monkeypatch.setattr("requests.get", mock_get)
    result = engine.run_fear_greed_load()

    assert mock_get.call_count == 2
    dated_call, fallback_call = mock_get.call_args_list
    assert dated_call.args[0].endswith("/graphdata/2014-01-01")
    # The undated base URL — no trailing date segment.
    assert fallback_call.args[0].endswith("/graphdata")
    assert fallback_call.kwargs["timeout"] == cfg.fear_greed.timeout_seconds
    assert result["status"] == "SUCCESS"
    assert result["start_date_fallback"] is True
    assert result["fetched_url"] == fallback_call.args[0]
    assert result["fetched_url"].endswith("/graphdata")
    assert result["rows_upserted"] == 2


@pytest.mark.parametrize("impl", IMPLEMENTATIONS)
def test_fallback_request_failure_propagates(
    engine, db, cfg, monkeypatch
):
    """If the undated retry ALSO raises HTTPError, it propagates (the
    bridge converts it to FAILED)."""
    mock_get = MagicMock(
        side_effect=[
            _http_error_response("500 dated"),
            _http_error_response("500 undated"),
        ]
    )
    monkeypatch.setattr("requests.get", mock_get)
    with pytest.raises(requests.HTTPError, match="500 undated"):
        engine.run_fear_greed_load()
    assert mock_get.call_count == 2


@pytest.mark.parametrize("impl", IMPLEMENTATIONS)
@pytest.mark.parametrize(
    "exc",
    [requests.Timeout("cnn slow"), ConnectionError("cnn down")],
    ids=["timeout", "connection_error"],
)
def test_non_http_error_does_not_fall_back(engine, db, monkeypatch, exc):
    """Timeout/ConnectionError on the dated URL -> NO retry: the error
    propagates and requests.get is called exactly once."""
    mock_get = MagicMock(side_effect=exc)
    monkeypatch.setattr("requests.get", mock_get)
    with pytest.raises(type(exc)):
        engine.run_fear_greed_load()
    assert mock_get.call_count == 1


@pytest.mark.parametrize("impl", IMPLEMENTATIONS)
def test_normal_response_reports_no_fallback(engine, db, cfg, monkeypatch):
    """A normal 200 on the dated URL -> start_date_fallback=False and
    fetched_url is the dated URL (single GET)."""
    cfg.fear_greed.start_date = "2020-08-01"
    mock_get = MagicMock(return_value=fake_response(make_payload()))
    monkeypatch.setattr("requests.get", mock_get)
    result = engine.run_fear_greed_load()
    assert mock_get.call_count == 1
    assert result["start_date_fallback"] is False
    assert result["fetched_url"].endswith("/graphdata/2020-08-01")


# ---------------------------------------------------------------------------
# Resilience
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("impl", IMPLEMENTATIONS)
def test_missing_historical_key_raises_named_error(
    engine, db, monkeypatch
):
    """A missing fear_and_greed_historical block is a hard error that
    NAMES the key — never a bare KeyError."""
    payload = make_payload()
    del payload["fear_and_greed_historical"]
    monkeypatch.setattr(
        "requests.get", MagicMock(return_value=fake_response(payload))
    )
    with pytest.raises(ValueError, match="fear_and_greed_historical"):
        engine.run_fear_greed_load()


@pytest.mark.parametrize("impl", IMPLEMENTATIONS)
def test_empty_historical_data_raises_named_error(
    engine, db, monkeypatch
):
    payload = make_payload()
    payload["fear_and_greed_historical"] = {"data": []}
    monkeypatch.setattr(
        "requests.get", MagicMock(return_value=fake_response(payload))
    )
    with pytest.raises(ValueError, match="fear_and_greed_historical"):
        engine.run_fear_greed_load()


@pytest.mark.parametrize("impl", IMPLEMENTATIONS)
def test_missing_sub_indicator_is_null_not_error(
    engine, db, monkeypatch, caplog
):
    """A missing/empty sub-indicator block -> NULL columns, the key in
    missing_series, a warning log — and NO exception."""
    payload = make_payload()
    del payload["put_call_options"]
    payload["junk_bond_demand"] = {"data": []}
    payload["safe_haven_demand"] = None
    monkeypatch.setattr(
        "requests.get", MagicMock(return_value=fake_response(payload))
    )
    result = engine.run_fear_greed_load()

    assert result["status"] == "SUCCESS"
    assert set(result["missing_series"]) == {
        "put_call_options",
        "junk_bond_demand",
        "safe_haven_demand",
    }
    rows = _rows(db)
    assert rows["put_call_options"] == [None, None]
    assert rows["put_call_options_rating"] == [None, None]
    assert rows["junk_bond_demand"] == [None, None]
    assert rows["safe_haven_demand_rating"] == [None, None]
    # Present series unaffected.
    assert rows["stock_price_strength"] == [1.0, 101.0]


@pytest.mark.parametrize("impl", IMPLEMENTATIONS)
def test_malformed_points_become_null(engine, db, monkeypatch):
    """Points missing y/rating -> NULL; points missing x are skipped."""
    payload = make_payload()
    payload["fear_and_greed_historical"]["data"] = [
        {"x": X1, "y": 30.0, "rating": "fear"},
        {"x": X2, "rating": "greed"},          # missing y -> NULL
        {"y": 99.0, "rating": "greed"},        # missing x -> skipped
        {"x": "not-a-number"},                 # unusable x -> skipped
    ]
    payload["stock_price_strength"]["data"] = [
        {"x": X2, "y": 5.0},                   # missing rating -> NULL
    ]
    monkeypatch.setattr(
        "requests.get", MagicMock(return_value=fake_response(payload))
    )
    result = engine.run_fear_greed_load()
    assert result["status"] == "SUCCESS"
    rows = _rows(db)
    assert rows["date"] == [D1, D2]
    assert rows["fg_score"] == [30.0, None]
    assert rows["fg_rating"] == ["fear", "greed"]
    assert rows["stock_price_strength_rating"] == [None, None]


@pytest.mark.parametrize("impl", IMPLEMENTATIONS)
def test_duplicate_dates_keep_last(engine, db, monkeypatch):
    """Two points on the same ms-day -> the LAST point wins."""
    payload = make_payload()
    payload["fear_and_greed_historical"]["data"] = [
        {"x": X1, "y": 11.0, "rating": "extreme fear"},
        {"x": X1 + 3600000.0, "y": 22.0, "rating": "fear"},  # same day
    ]
    monkeypatch.setattr(
        "requests.get", MagicMock(return_value=fake_response(payload))
    )
    result = engine.run_fear_greed_load()
    # The composite series collapses to one point on D1 (last wins);
    # the sub-indicator blocks still contribute a D2 row via the outer
    # merge, with NULL fg_* columns on that date.
    assert result["rows_upserted"] == 2
    rows = _rows(db)
    assert rows["date"] == [D1, D2]
    assert rows["fg_score"] == [22.0, None]
    assert rows["fg_rating"] == ["fear", None]


@pytest.mark.parametrize("impl", IMPLEMENTATIONS)
def test_unknown_payload_keys_ignored(engine, db, monkeypatch):
    """Unknown extra top-level keys are ignored silently."""
    payload = make_payload()
    payload["brand_new_indicator"] = {"data": [{"x": X2, "y": 1.0}]}
    monkeypatch.setattr(
        "requests.get", MagicMock(return_value=fake_response(payload))
    )
    result = engine.run_fear_greed_load()
    assert result["status"] == "SUCCESS"
    assert result["rows_upserted"] == 2


# ---------------------------------------------------------------------------
# Upsert idempotency / update-on-change
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("impl", IMPLEMENTATIONS)
def test_upsert_idempotent_and_updates(engine, db, monkeypatch):
    """Re-run with the same payload -> same row count; changed values
    UPDATE the row in place (no duplicate dates)."""
    mock_get = MagicMock(return_value=fake_response(make_payload()))
    monkeypatch.setattr("requests.get", mock_get)

    engine.run_fear_greed_load()
    engine.run_fear_greed_load()
    rows = _rows(db)
    assert len(rows["date"]) == 2
    assert rows["fg_score"] == [20.5, 55.0]

    # Change the latest score — the row is updated, not duplicated.
    changed = make_payload()
    changed["fear_and_greed_historical"]["data"][1]["y"] = 61.0
    mock_get.return_value = fake_response(changed)
    engine.run_fear_greed_load()
    rows = _rows(db)
    assert len(rows["date"]) == 2
    assert rows["fg_score"] == [20.5, 61.0]


# ---------------------------------------------------------------------------
# Alert flag
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("impl", IMPLEMENTATIONS)
def test_alert_triggered_below_threshold(engine, db, cfg, monkeypatch):
    """latest fg_score <= alert_threshold -> alert_triggered + the
    'extreme fear' sub-indicators listed in alert_detail."""
    payload = make_payload()
    payload["fear_and_greed_historical"]["data"][1]["y"] = 12.0
    payload["stock_price_strength"]["rating"] = "extreme fear"
    payload["market_volatility_vix"]["rating"] = "Extreme Fear"
    monkeypatch.setattr(
        "requests.get", MagicMock(return_value=fake_response(payload))
    )
    cfg.fear_greed.alert_threshold = 25.0
    result = engine.run_fear_greed_load()
    assert result["alert_triggered"] is True
    assert set(result["alert_detail"]) == {
        "stock_price_strength",
        "market_volatility_vix",
    }


@pytest.mark.parametrize("impl", IMPLEMENTATIONS)
def test_alert_not_triggered_above_threshold(
    engine, db, cfg, monkeypatch
):
    monkeypatch.setattr(
        "requests.get", MagicMock(return_value=fake_response(make_payload()))
    )
    cfg.fear_greed.alert_threshold = 25.0
    result = engine.run_fear_greed_load()
    assert result["fg_score"] == 55.0
    assert result["alert_triggered"] is False
    assert result["alert_detail"] == []


# ---------------------------------------------------------------------------
# Bridge — merged under 'fear_greed' on the load paths; never raises
# ---------------------------------------------------------------------------


def _open_market_calendar():
    sched = pd.DataFrame(
        {
            "market_open": [pd.Timestamp("2024-01-02 09:30")],
            "market_close": [pd.Timestamp("2024-01-02 16:00")],
        },
        index=[pd.Timestamp("2024-01-02")],
    )
    cal = MagicMock()
    cal.schedule.return_value = sched
    return cal


@pytest.mark.parametrize("impl", IMPLEMENTATIONS)
def test_bridge_merged_on_initial_and_on_request_load(
    engine, db, cfg, module_name
):
    """run_initial_load + run_on_request_load merge the bridge result
    under result['fear_greed'] (mirrors result['weekly'])."""
    with patch(
        f"{module_name}.requests.get",
        MagicMock(return_value=fake_response(make_payload())),
    ):
        init = engine.run_initial_load()
        assert init["fear_greed"]["status"] == "SUCCESS"
        assert init["fear_greed"]["rows_upserted"] == 2

        req = engine.run_on_request_load([], "2024-01-01", "2024-01-05")
        assert req["fear_greed"]["status"] == "SUCCESS"


@pytest.mark.parametrize("impl", IMPLEMENTATIONS)
def test_bridge_merged_on_daily_load_success(engine, db, cfg, module_name):
    """run_daily_load (market open) merges result['fear_greed'];
    the SKIPPED early-return path stays bare."""
    with patch(
        f"{module_name}.mcal.get_calendar",
        return_value=_open_market_calendar(),
    ), patch(
        f"{module_name}.requests.get",
        MagicMock(return_value=fake_response(make_payload())),
    ):
        res = engine.run_daily_load()
        assert res.get("status") != "SKIPPED"
        assert res["fear_greed"]["status"] == "SUCCESS"
        assert res["fear_greed"]["rows_upserted"] == 2

    empty_cal = MagicMock()
    empty_cal.schedule.return_value = pd.DataFrame()
    with patch(
        f"{module_name}.mcal.get_calendar", return_value=empty_cal
    ):
        skipped = engine.run_daily_load()
        assert skipped["status"] == "SKIPPED"
        assert "fear_greed" not in skipped


@pytest.mark.parametrize("impl", IMPLEMENTATIONS)
def test_bridge_never_raises_on_fetch_failure(engine, db, module_name):
    """A requests.get exception -> {'status': 'FAILED', 'error': ...},
    not an exception — and the load result is unaffected."""
    with patch(
        f"{module_name}.requests.get",
        MagicMock(side_effect=ConnectionError("cnn down")),
    ):
        out = engine._fear_greed_bridge()
        assert out["status"] == "FAILED"
        assert "cnn down" in out["error"]

        res = engine.run_on_request_load([], "2024-01-01", "2024-01-05")
        assert res["fear_greed"]["status"] == "FAILED"


@pytest.mark.parametrize("impl", IMPLEMENTATIONS)
def test_bridge_disabled_returns_disabled(engine, db, cfg, module_name):
    """fear_greed.enabled=false -> {'status': 'DISABLED'} and no HTTP
    call is made."""
    cfg.fear_greed.enabled = False
    mock_get = MagicMock()
    with patch(f"{module_name}.requests.get", mock_get):
        out = engine._fear_greed_bridge()
        res = engine.run_on_request_load([], "2024-01-01", "2024-01-05")
    assert out == {"status": "DISABLED"}
    assert res["fear_greed"] == {"status": "DISABLED"}
    mock_get.assert_not_called()


# ---------------------------------------------------------------------------
# Flight action wiring (both servers)
# ---------------------------------------------------------------------------


def _make_server(module):
    """A MarketDataFlightServer with mocked deps (no real gRPC bind)."""
    db_mgr = MagicMock()
    ingest_engine = MagicMock()
    cfg = MagicMock()

    class _TestServer(module.MarketDataFlightServer):
        def __init__(self, db_mgr, ingest_engine, cfg):
            self.db = db_mgr
            self.engine = ingest_engine
            self.cfg = cfg

    return _TestServer(db_mgr, ingest_engine, cfg), ingest_engine


def _make_action(action_type, payload=None):
    action = MagicMock()
    action.type = action_type
    if payload is not None:
        action.body = MagicMock()
        action.body.to_pybytes.return_value = json.dumps(payload).encode(
            "utf-8"
        )
    else:
        action.body = None
    return action


def _parse(result):
    return json.loads(result.body.to_pybytes().decode("utf-8"))


@pytest.mark.parametrize(
    "module_name",
    ["src.flight_server", "src.flight_server_challenger"],
    ids=["executor:src.flight_server", "challenger:src.flight_server_challenger"],
)
def test_fear_greed_refresh_action_success(module_name):
    module = importlib.import_module(module_name)
    server, engine = _make_server(module)
    engine.run_fear_greed_load.return_value = {
        "status": "SUCCESS",
        "rows_upserted": 2,
        "latest_date": "2024-09-13",
        "fg_score": 55.0,
        "fg_rating": "neutral",
        "alert_triggered": False,
        "alert_detail": [],
        "missing_series": [],
        "duration_ms": 5,
    }
    data = _parse(
        server.do_action(MagicMock(), _make_action("fear_greed_refresh"))
    )
    assert data["status"] == "SUCCESS"
    assert data["rows_upserted"] == 2
    engine.run_fear_greed_load.assert_called_once_with(start_date=None)


@pytest.mark.parametrize(
    "module_name",
    ["src.flight_server", "src.flight_server_challenger"],
    ids=["executor:src.flight_server", "challenger:src.flight_server_challenger"],
)
def test_fear_greed_refresh_action_forwards_start_date(module_name):
    module = importlib.import_module(module_name)
    server, engine = _make_server(module)
    engine.run_fear_greed_load.return_value = {"status": "SUCCESS"}
    server.do_action(
        MagicMock(),
        _make_action("fear_greed_refresh", {"start_date": "2020-01-01"}),
    )
    engine.run_fear_greed_load.assert_called_once_with(
        start_date="2020-01-01"
    )


@pytest.mark.parametrize(
    "module_name",
    ["src.flight_server", "src.flight_server_challenger"],
    ids=["executor:src.flight_server", "challenger:src.flight_server_challenger"],
)
@pytest.mark.parametrize("bad", ["not-a-date", "2020-13-40", 20200101])
def test_fear_greed_refresh_bad_start_date_is_flight_error(
    module_name, bad
):
    """A malformed start_date payload -> FlightServerError, and the
    engine is never called."""
    module = importlib.import_module(module_name)
    server, engine = _make_server(module)
    with pytest.raises(flight.FlightServerError):
        server.do_action(
            MagicMock(),
            _make_action("fear_greed_refresh", {"start_date": bad}),
        )
    engine.run_fear_greed_load.assert_not_called()


@pytest.mark.parametrize(
    "module_name",
    ["src.flight_server", "src.flight_server_challenger"],
    ids=["executor:src.flight_server", "challenger:src.flight_server_challenger"],
)
def test_fear_greed_refresh_engine_failure_is_failed_body(module_name):
    """Engine exceptions surface as status=FAILED in the JSON body —
    not a transport error (mirrors weekly_refresh)."""
    module = importlib.import_module(module_name)
    server, engine = _make_server(module)
    engine.run_fear_greed_load.side_effect = RuntimeError("boom")
    data = _parse(
        server.do_action(MagicMock(), _make_action("fear_greed_refresh"))
    )
    assert data["status"] == "FAILED"
    assert "boom" in data["error"]


# ---------------------------------------------------------------------------
# Config validation (both config impls)
# ---------------------------------------------------------------------------


def test_executor_config_defaults_and_yaml():
    cfg = PipelineConfig()
    assert cfg.fear_greed.enabled is True
    assert cfg.fear_greed.start_date == "2014-01-01"
    assert cfg.fear_greed.timeout_seconds == 15
    assert cfg.fear_greed.alert_threshold == 25.0


@pytest.mark.parametrize(
    "bad_section",
    [
        {"start_date": "not-a-date"},
        {"start_date": "2014-13-99"},
        {"start_date": 20140101},
        {"timeout_seconds": 0},
        {"timeout_seconds": -5},
        {"timeout_seconds": "fast"},
        {"alert_threshold": -1},
        {"alert_threshold": 101},
        {"alert_threshold": True},
        {"enabled": "yes"},
    ],
)
def test_executor_fear_greed_config_rejects_bad_values(bad_section):
    with pytest.raises(Exception):
        PipelineConfig(fear_greed=bad_section)


def test_challenger_config_defaults():
    from src.config_challenger import FearGreedConfig as CcFg

    cfg = CcFg()
    assert cfg.enabled is True
    assert cfg.start_date == "2014-01-01"
    assert cfg.timeout_seconds == 15.0
    assert cfg.alert_threshold == 25.0


@pytest.mark.parametrize(
    "bad_section",
    [
        {"start_date": "not-a-date"},
        {"start_date": "2014-13-99"},
        {"start_date": 20140101},
        {"timeout_seconds": 0},
        {"timeout_seconds": -5},
        {"timeout_seconds": "fast"},
        {"alert_threshold": -1},
        {"alert_threshold": 101},
        {"alert_threshold": True},
        {"enabled": "yes"},
    ],
)
def test_challenger_fear_greed_config_rejects_bad_values(bad_section):
    from src.config_challenger import FearGreedConfig as CcFg

    with pytest.raises(Exception):
        CcFg.from_dict(bad_section)


def test_both_configs_read_fear_greed_from_yaml(tmp_path):
    """The shipped src/config.yaml fear_greed section loads in BOTH
    config impls."""
    from src.config_challenger import PipelineConfig as ChallengerCfg

    cfg = PipelineConfig.from_yaml("src/config.yaml")
    cfgc = ChallengerCfg.from_yaml("src/config.yaml")
    assert cfg.fear_greed.enabled is True
    assert cfg.fear_greed.start_date == "2020-08-01"
    assert cfgc.fear_greed.enabled is True
    assert cfgc.fear_greed.start_date == "2020-08-01"
    assert cfgc.fear_greed.alert_threshold == 25.0
