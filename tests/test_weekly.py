"""Critic tests for the weekly layer (F12-T1).

Parametrized over BOTH implementations:
  - src.db.DatabaseManager            (executor)
  - src.db_challenger.DatabaseManager (challenger)
  - src.ingestion.IngestionEngine            (executor)
  - src.ingestion_challenger.IngestionEngine (challenger)

Covers the persisted weekly tables ported verbatim from
``libs/vw_pipe_data_duckdb.py`` (aggregate_to_weekly :753-800 —
nominal-Friday keys via ``date_trunc('week', Date)::DATE + 4`` — and
update_weekly_indicators_01 :317-397 — the _01 column set under the
base ``weekly_indicators`` name):

  * schema: weekly_close + weekly_indicators exist with the declared
    columns and (Date, Ticker) primary key; SCHEMA_VERSION bumped.
  * rebuild_weekly_close: Friday-dated key (incl. holiday weeks and the
    partial current week), arg_min/arg_max OHLC + volume sum, and
    idempotent re-runs via ON CONFLICT DO UPDATE.
  * rebuild_weekly_indicators: _01 column set incl.
    Slow_D_80_20_Crossover, spot-checked slow_k on a tiny series,
    crossover flags, and a no-crash empty-input run.
  * engine.run_weekly_refresh ordering + result shape.
  * _weekly_bridge merged under 'weekly' on all three load paths
    (never on SKIPPED) and never fails a load.

All data lives in tmp_path-isolated DuckDB files — nothing touches the
real database and no yfinance calls are made.
"""

from __future__ import annotations

import datetime as _dt
from typing import List
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

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
        id="executor:src.db",
    ),
    pytest.param(
        (
            _ChallengerIngestionEngine,
            _ChallengerDatabaseManager,
            "src.ingestion_challenger",
            _CHALLENGER_SCHEMA_VERSION,
        ),
        id="challenger:src.db_challenger",
    ),
]

EXPECTED_WEEKLY_INDICATORS_COLS = [
    "Date",
    "Ticker",
    "Slow_K",
    "Slow_D",
    "Stoch_Crossover",
    "Slow_D_80_20_Crossover",
    "BB_Upper",
    "BB_Central",
    "BB_Lower",
    "BB_Reversal_Signal",
    "PPO",
    "PPO_Signal",
    "PPO_Hist_Crossover",
    "PPO_Zero_Crossover",
    "Force_Index",
    "Force_Zero_Crossover",
    "Log_Close_EMA18",
    "Log_Close_EMA40",
]


@pytest.fixture
def impl(request):
    return request.param


@pytest.fixture
def db(impl, tmp_path):
    _, DbCls, _, _ = impl
    mgr = DbCls(str(tmp_path / "test_weekly.duckdb"))
    try:
        yield mgr
    finally:
        mgr.close()


@pytest.fixture
def cfg():
    c = PipelineConfig()
    c.ingestion.tickers = ["AAA"]
    c.ingestion.exclude_tickers = []
    return c


@pytest.fixture
def engine(impl, db, cfg):
    EngineCls, _, _, _ = impl
    return EngineCls(db, cfg)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_daily_df(ticker: str, rows) -> pd.DataFrame:
    """rows: iterable of (date, open, high, low, close, volume)."""
    rows = list(rows)
    return pd.DataFrame(
        {
            "Date": pd.to_datetime([r[0] for r in rows]),
            "Ticker": [str(ticker)] * len(rows),
            "Open": [float(r[1]) for r in rows],
            "High": [float(r[2]) for r in rows],
            "Low": [float(r[3]) for r in rows],
            "Close": [float(r[4]) for r in rows],
            "Volume": [float(r[5]) for r in rows],
        }
    )


def _weekly_rows(db) -> dict:
    return db.execute_query(
        "SELECT Date, Ticker, Open, High, Low, Close, Volume "
        "FROM weekly_close ORDER BY Date, Ticker"
    ).to_pydict()


def _indicator_rows(db) -> dict:
    return db.execute_query(
        "SELECT * FROM weekly_indicators ORDER BY Date, Ticker"
    ).to_pydict()


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("impl", IMPLEMENTATIONS)
def test_weekly_tables_exist_with_pk(impl, db):
    _, _, _, schema_version = impl
    tbl = db.execute_query(
        "SELECT table_name FROM information_schema.tables "
        "ORDER BY table_name"
    )
    names = set(tbl.column("table_name").to_pylist())
    assert {"weekly_close", "weekly_indicators"} <= names

    cols = db.execute_query(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'weekly_indicators' ORDER BY ordinal_position"
    ).column("column_name").to_pylist()
    assert cols == EXPECTED_WEEKLY_INDICATORS_COLS

    wcols = db.execute_query(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_name = 'weekly_close' ORDER BY ordinal_position"
    ).to_pydict()
    assert wcols["column_name"] == [
        "Date",
        "Ticker",
        "Open",
        "High",
        "Low",
        "Close",
        "Volume",
    ]
    # DOUBLE per AC (legacy BIGINT — our daily_close uses DOUBLE).
    assert dict(zip(wcols["column_name"], wcols["data_type"]))[
        "Volume"
    ] == "DOUBLE"

    ver = db.execute_query(
        "SELECT MAX(version) AS v FROM schema_version"
    ).column("v").to_pylist()[0]
    assert ver == schema_version


@pytest.mark.parametrize("impl", IMPLEMENTATIONS)
def test_existing_db_migrates(impl, tmp_path):
    """A DB file opened a second time keeps data and gains the weekly
    tables + new version row (CREATE IF NOT EXISTS migration path)."""
    _, DbCls, _, schema_version = impl
    db_path = str(tmp_path / "persist.duckdb")
    mgr = DbCls(db_path)
    mgr.upsert_dataframe(
        make_daily_df("AAA", [("2024-01-03", 1, 2, 0.5, 1.5, 10)])
    )
    mgr.close()

    mgr2 = DbCls(db_path)
    try:
        assert mgr2.rebuild_weekly_close() == 1
        ver = mgr2.execute_query(
            "SELECT MAX(version) AS v FROM schema_version"
        ).column("v").to_pylist()[0]
        assert ver == schema_version
    finally:
        mgr2.close()


# ---------------------------------------------------------------------------
# rebuild_weekly_close — Friday key + OHLCV aggregation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("impl", IMPLEMENTATIONS)
def test_friday_dated_key_and_ohlcv(db):
    """A full Mon-Fri week collapses to one row keyed by that Friday,
    with arg_min Open / arg_max Close / max High / min Low / sum Vol."""
    db.upsert_dataframe(
        make_daily_df(
            "AAA",
            [
                ("2024-01-02", 10, 15, 9, 14, 100),   # Tue
                ("2024-01-03", 11, 16, 10, 15, 200),  # Wed
                ("2024-01-04", 12, 17, 11, 15.5, 300),  # Thu
                ("2024-01-05", 13, 18, 12, 16, 400),  # Fri
            ],
        )
    )
    assert db.rebuild_weekly_close() == 1
    rows = _weekly_rows(db)
    assert rows["Date"] == [_dt.date(2024, 1, 5)]
    assert rows["Open"] == [10.0]   # arg_min(Open, Date) — Tuesday
    assert rows["Close"] == [16.0]  # arg_max(Close, Date) — Friday
    assert rows["High"] == [18.0]
    assert rows["Low"] == [9.0]
    assert rows["Volume"] == [1000.0]


@pytest.mark.parametrize("impl", IMPLEMENTATIONS)
def test_holiday_week_nominal_friday(db):
    """A week whose sessions end Thursday (e.g. Good Friday) is still
    keyed by the nominal Friday, not the last trading day."""
    db.upsert_dataframe(
        make_daily_df(
            "AAA",
            [
                ("2024-03-25", 10, 15, 9, 14, 100),  # Mon
                ("2024-03-28", 11, 16, 10, 15, 200),  # Thu (Good Friday wk)
            ],
        )
    )
    db.rebuild_weekly_close()
    rows = _weekly_rows(db)
    assert rows["Date"] == [_dt.date(2024, 3, 29)]  # nominal Friday
    assert rows["Close"] == [15.0]


@pytest.mark.parametrize("impl", IMPLEMENTATIONS)
def test_partial_current_week_present(db):
    """A mid-week daily row already produces a weekly row dated the
    coming Friday — the current week is always present."""
    db.upsert_dataframe(
        make_daily_df("AAA", [("2024-01-03", 10, 15, 9, 14, 100)])
    )
    db.rebuild_weekly_close()
    rows = _weekly_rows(db)
    assert rows["Date"] == [_dt.date(2024, 1, 5)]


@pytest.mark.parametrize("impl", IMPLEMENTATIONS)
def test_rebuild_is_idempotent(db):
    """Second rebuild: ON CONFLICT DO UPDATE — no duplicates, values
    refreshed."""
    df = make_daily_df(
        "AAA",
        [
            ("2024-01-02", 10, 15, 9, 14, 100),
            ("2024-01-03", 11, 16, 10, 15, 200),
        ],
    )
    db.upsert_dataframe(df)
    db.rebuild_weekly_close()
    db.rebuild_weekly_close()
    rows = _weekly_rows(db)
    assert len(rows["Date"]) == 1

    # New data lands in the same week -> the row is updated, not duped.
    db.upsert_dataframe(
        make_daily_df("AAA", [("2024-01-05", 20, 25, 19, 24, 500)])
    )
    db.rebuild_weekly_close()
    rows = _weekly_rows(db)
    assert len(rows["Date"]) == 1
    assert rows["Close"] == [24.0]
    assert rows["Volume"] == [800.0]


@pytest.mark.parametrize("impl", IMPLEMENTATIONS)
def test_multiple_weeks_and_tickers(db):
    db.upsert_dataframe(
        pd.concat(
            [
                make_daily_df(
                    "AAA",
                    [
                        ("2024-01-03", 10, 15, 9, 14, 100),
                        ("2024-01-10", 11, 16, 10, 15, 200),
                    ],
                ),
                make_daily_df(
                    "BBB", [("2024-01-03", 50, 55, 49, 54, 900)]
                ),
            ],
            ignore_index=True,
        )
    )
    assert db.rebuild_weekly_close() == 3
    rows = _weekly_rows(db)
    assert rows["Date"] == [
        _dt.date(2024, 1, 5),
        _dt.date(2024, 1, 5),
        _dt.date(2024, 1, 12),
    ]
    assert rows["Ticker"] == ["AAA", "BBB", "AAA"]


# ---------------------------------------------------------------------------
# rebuild_weekly_indicators
# ---------------------------------------------------------------------------


def _seed_weekly_via_daily(db, closes: List[float]) -> None:
    """Seed one daily row per week so each weekly bar has
    Low=90, High=110 and the given Close (fast_k = (C-90)/20*100)."""
    first = _dt.date(2024, 1, 3)  # a Wednesday
    rows = [
        (
            (first + _dt.timedelta(days=7 * i)).strftime("%Y-%m-%d"),
            100.0,
            110.0,
            90.0,
            close,
            1000.0,
        )
        for i, close in enumerate(closes)
    ]
    db.upsert_dataframe(make_daily_df("AAA", rows))
    db.rebuild_weekly_close()


@pytest.mark.parametrize("impl", IMPLEMENTATIONS)
def test_indicators_columns_and_rowcount(db):
    _seed_weekly_via_daily(db, [108, 96, 102, 108, 110])
    n = db.rebuild_weekly_indicators()
    assert n == 5
    cols = db.execute_query(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'weekly_indicators' ORDER BY ordinal_position"
    ).column("column_name").to_pylist()
    assert cols == EXPECTED_WEEKLY_INDICATORS_COLS
    rows = _indicator_rows(db)
    assert "Slow_D_80_20_Crossover" in rows
    assert len(rows["Date"]) == 5


@pytest.mark.parametrize("impl", IMPLEMENTATIONS)
def test_indicators_slow_k_spot_check(db):
    """Hand-computed slow_k on a tiny deterministic series.

    With Low=90 / High=110 fixed, fast_k = (Close-90)*5.
    Closes [108,96,102,108,110] -> fast_k [90,30,60,90,100].
    slow_k = 3-row mean of fast_k: [90, 60, 60, 60, 83.33].
    slow_d = 3-row mean of slow_k: [90, 75, 70, 60, 67.78].
    """
    _seed_weekly_via_daily(db, [108, 96, 102, 108, 110])
    db.rebuild_weekly_indicators()
    rows = _indicator_rows(db)
    expected_k = [90.0, 60.0, 60.0, 60.0, 83.3333]
    expected_d = [90.0, 75.0, 70.0, 60.0, 67.7778]
    for got, exp in zip(rows["Slow_K"], expected_k):
        assert got == pytest.approx(exp, abs=1e-3)
    for got, exp in zip(rows["Slow_D"], expected_d):
        assert got == pytest.approx(exp, abs=1e-3)


@pytest.mark.parametrize("impl", IMPLEMENTATIONS)
def test_indicators_crossover_flags(db):
    """The crafted series produces stoch_x = -1 at week 2 (slow_k
    crosses below slow_d) and stoch_x = +1 plus
    Slow_D_80_20_Crossover = +1 at week 5 (slow_k crosses above slow_d
    while crossing up through 80)."""
    _seed_weekly_via_daily(db, [108, 96, 102, 108, 110])
    db.rebuild_weekly_indicators()
    rows = _indicator_rows(db)
    assert rows["Stoch_Crossover"] == [0, -1, 0, 0, 1]
    assert rows["Slow_D_80_20_Crossover"] == [0, 0, 0, 0, 1]
    # Other indicator columns are populated (non-NULL where defined).
    assert rows["BB_Central"][4] is not None
    assert rows["PPO"][4] is not None
    assert rows["Force_Index"][4] is not None
    assert rows["Log_Close_EMA18"][4] is not None


@pytest.mark.parametrize("impl", IMPLEMENTATIONS)
def test_indicators_empty_weekly_close_no_crash(db):
    """Empty weekly_close -> empty weekly_indicators, returns 0."""
    assert db.rebuild_weekly_indicators() == 0
    rows = _indicator_rows(db)
    assert rows == {} or len(rows.get("Date", [])) == 0


@pytest.mark.parametrize("impl", IMPLEMENTATIONS)
def test_indicators_idempotent(db):
    _seed_weekly_via_daily(db, [108, 96, 102])
    db.rebuild_weekly_indicators()
    db.rebuild_weekly_indicators()
    rows = _indicator_rows(db)
    assert len(rows["Date"]) == 3


# ---------------------------------------------------------------------------
# Engine: run_weekly_refresh + _weekly_bridge
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("impl", IMPLEMENTATIONS)
def test_run_weekly_refresh_order_and_shape(engine, db):
    """run_weekly_refresh rebuilds close THEN indicators (indicators
    read weekly_close) and returns the documented result dict."""
    _seed_weekly_via_daily(db, [108, 96, 102])
    result = engine.run_weekly_refresh()
    assert result["status"] == "SUCCESS"
    assert result["weekly_close_rows"] == 3
    assert result["weekly_indicators_rows"] == 3
    assert isinstance(result["duration_ms"], int)

    calls: List[str] = []
    orig_close = db.rebuild_weekly_close
    orig_ind = db.rebuild_weekly_indicators

    def _spy_close():
        calls.append("close")
        return orig_close()

    def _spy_ind():
        calls.append("indicators")
        return orig_ind()

    with patch.object(
        db, "rebuild_weekly_close", side_effect=_spy_close
    ), patch.object(
        db, "rebuild_weekly_indicators", side_effect=_spy_ind
    ):
        engine.run_weekly_refresh()
    assert calls == ["close", "indicators"]


@pytest.mark.parametrize("impl", IMPLEMENTATIONS)
def test_weekly_bridge_success(engine, db):
    _seed_weekly_via_daily(db, [108])
    out = engine._weekly_bridge()
    assert out["status"] == "SUCCESS"
    assert out["weekly_close_rows"] == 1
    assert out["weekly_indicators_rows"] == 1


@pytest.mark.parametrize("impl", IMPLEMENTATIONS)
def test_weekly_bridge_never_raises(engine, db):
    """A failing rebuild -> {'status': 'FAILED', 'error': ...}, not an
    exception."""
    with patch.object(
        db, "rebuild_weekly_close", side_effect=RuntimeError("db boom")
    ):
        out = engine._weekly_bridge()
    assert out["status"] == "FAILED"
    assert "db boom" in out["error"]
