"""Tests for DatabaseManager (F2-T3) and check_daily_close_health (F9-T5).

Parametrized over BOTH implementations:
  - src.db.DatabaseManager            (executor, F2-T1)
  - src.db_challenger.DatabaseManager (challenger, F2-T2)

All tests use ``tmp_path``-isolated DuckDB files and never touch the real
database at ``C:\\Intel\\parquet``.

Known platform notes accounted for here:
  * DuckDB 1.4.3 on Windows cannot open a ``read_only=True`` connection to a
    file that already has an RW connection open. The two implementations
    handle this differently (executor: cursor + ``BEGIN TRANSACTION READ
    ONLY``; challenger: cursor pool + SELECT-only SQL validation). The tests
    only assert that read methods work and that writes via ``execute_query``
    raise -- they do NOT assume a specific read-only mechanism.
  * pandas 3.x exposes a native ``str`` dtype that DuckDB 1.4.x cannot scan.
    Test DataFrames are built with explicit dtypes (object for strings,
    datetime64 for dates, float64 for numerics) via the ``make_df`` helper.
"""

from __future__ import annotations

import json
import threading
from datetime import datetime, timezone

import pandas as pd
import pytest

from src.db import DatabaseManager as _ExecutorDatabaseManager
from src.db_challenger import DatabaseManager as _ChallengerDatabaseManager

IMPLEMENTATIONS = [
    pytest.param(_ExecutorDatabaseManager, id="executor:src.db"),
    pytest.param(_ChallengerDatabaseManager, id="challenger:src.db_challenger"),
]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def manager(request, tmp_path):
    """Build a DatabaseManager backed by an isolated tmp_path DuckDB file."""
    cls = request.param
    db_path = str(tmp_path / "test.duckdb")
    mgr = cls(db_path)
    try:
        yield mgr
    finally:
        mgr.close()


# ---------------------------------------------------------------------------
# Test-data helper
# ---------------------------------------------------------------------------


def make_df(ticker, dates, close_prices, volumes):
    """Build a valid daily_close DataFrame with explicit, safe dtypes.

    - Date   -> datetime64 (DuckDB casts to DATE)
    - Ticker -> object      (avoids pandas 3.x 'str' dtype)
    - OHLCV  -> float64
    """
    n = len(dates)
    return pd.DataFrame(
        {
            "Date": pd.to_datetime(list(dates)),
            "Ticker": [str(ticker)] * n,
            "Open": [float(c) * 0.99 for c in close_prices],
            "High": [float(c) * 1.01 for c in close_prices],
            "Low": [float(c) * 0.98 for c in close_prices],
            "Close": [float(c) for c in close_prices],
            "Volume": [float(v) for v in volumes],
        }
    )


# ---------------------------------------------------------------------------
# 1. Schema creates 3 tables
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("manager", IMPLEMENTATIONS, indirect=True)
def test_schema_creates_three_tables(manager):
    tbl = manager.execute_query(
        "SELECT table_name FROM information_schema.tables ORDER BY table_name"
    )
    names = set(tbl.column("table_name").to_pylist())
    assert {"daily_close", "splits_processed", "schema_version"} <= names


# ---------------------------------------------------------------------------
# 2. Upsert inserts new rows
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("manager", IMPLEMENTATIONS, indirect=True)
def test_upsert_inserts_new_rows(manager):
    df = make_df("AAPL", ["2026-01-01", "2026-01-02"], [100.0, 101.0], [1000.0, 2000.0])
    n = manager.upsert_dataframe(df)
    assert n == 2
    tbl = manager.execute_query("SELECT COUNT(*) AS c FROM daily_close")
    assert tbl.column("c").to_pylist()[0] == 2


# ---------------------------------------------------------------------------
# 3. Upsert updates existing rows (MERGE / ON CONFLICT)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("manager", IMPLEMENTATIONS, indirect=True)
def test_upsert_updates_existing_rows(manager):
    df = make_df("AAPL", ["2026-01-01", "2026-01-02"], [100.0, 101.0], [1000.0, 2000.0])
    manager.upsert_dataframe(df)
    df2 = make_df("AAPL", ["2026-01-01", "2026-01-02"], [8.8, 9.9], [1000.0, 2000.0])
    manager.upsert_dataframe(df2)

    # Row count must NOT grow -- same (Date, Ticker) PK.
    tbl = manager.execute_query("SELECT COUNT(*) AS c FROM daily_close")
    assert tbl.column("c").to_pylist()[0] == 2

    # Close values must be the updated ones, in date order.
    closes = manager.execute_query(
        "SELECT Close FROM daily_close WHERE Ticker = 'AAPL' ORDER BY Date"
    )
    assert closes.column("Close").to_pylist() == [8.8, 9.9]


# ---------------------------------------------------------------------------
# 4. Upsert empty df returns 0, no error
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("manager", IMPLEMENTATIONS, indirect=True)
def test_upsert_empty_df_returns_zero(manager):
    df = pd.DataFrame(
        columns=["Date", "Ticker", "Open", "High", "Low", "Close", "Volume"]
    )
    n = manager.upsert_dataframe(df)
    assert n == 0
    tbl = manager.execute_query("SELECT COUNT(*) AS c FROM daily_close")
    assert tbl.column("c").to_pylist()[0] == 0


# ---------------------------------------------------------------------------
# 5. Read-only enforcement: execute_query rejects a write statement
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("manager", IMPLEMENTATIONS, indirect=True)
def test_execute_query_rejects_write(manager):
    # Both implementations must raise *some* exception for a DELETE routed
    # through the read-only execute_query path. The exact exception type
    # differs (executor: duckdb.TransactionException; challenger: ValueError),
    # so we only assert that it raises.
    with pytest.raises(Exception):
        manager.execute_query("DELETE FROM daily_close")


# ---------------------------------------------------------------------------
# 6. detect_new_tickers returns correct diff
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("manager", IMPLEMENTATIONS, indirect=True)
def test_detect_new_tickers(manager):
    manager.upsert_dataframe(make_df("AAPL", ["2026-01-01"], [100.0], [1000.0]))
    manager.upsert_dataframe(make_df("MSFT", ["2026-01-01"], [200.0], [2000.0]))
    new = manager.detect_new_tickers(["AAPL", "MSFT", "NVDA"])
    assert new == ["NVDA"]


# ---------------------------------------------------------------------------
# 7. get_aligned_start_date returns MIN(Date) as ISO string or None
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("manager", IMPLEMENTATIONS, indirect=True)
def test_get_aligned_start_date_empty_returns_none(manager):
    assert manager.get_aligned_start_date() is None


@pytest.mark.parametrize("manager", IMPLEMENTATIONS, indirect=True)
def test_get_aligned_start_date_returns_min(manager):
    df = make_df(
        "AAPL",
        ["2026-01-05", "2026-01-01", "2026-01-03"],
        [100.0, 101.0, 102.0],
        [1000.0, 2000.0, 3000.0],
    )
    manager.upsert_dataframe(df)
    d = manager.get_aligned_start_date()
    assert d is not None
    assert isinstance(d, str)
    assert d.startswith("2026-01-01")


# ---------------------------------------------------------------------------
# 8. generate_top_20_dollar_volume returns sorted DESC, limit 20
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("manager", IMPLEMENTATIONS, indirect=True)
def test_generate_top_20_dollar_volume(manager):
    # 25 tickers on the same date, each with a distinct Close*Volume so the
    # ordering is deterministic. T00 -> 1000, T01 -> 1010, ... T24 -> 1240.
    tickers = [f"T{i:02d}" for i in range(25)]
    closes = [float(100 + i) for i in range(25)]
    volumes = [10.0] * 25
    df = pd.DataFrame(
        {
            "Date": pd.to_datetime(["2026-01-01"] * 25),
            "Ticker": tickers,
            "Open": [c * 0.99 for c in closes],
            "High": [c * 1.01 for c in closes],
            "Low": [c * 0.98 for c in closes],
            "Close": closes,
            "Volume": volumes,
        }
    )
    manager.upsert_dataframe(df)

    result = manager.generate_top_20_dollar_volume()
    assert len(result) == 20

    # Must be sorted by dollar_volume DESC.
    dvs = [row["dollar_volume"] for row in result]
    assert dvs == sorted(dvs, reverse=True)

    # Highest dollar_volume is T24 -> 124 * 10 = 1240.
    assert result[0]["Ticker"] == "T24"
    assert result[0]["dollar_volume"] == pytest.approx(1240.0)
    # 20th entry is T05 -> 105 * 10 = 1050.
    assert result[-1]["Ticker"] == "T05"
    assert result[-1]["dollar_volume"] == pytest.approx(1050.0)


# ---------------------------------------------------------------------------
# 9. record_split + is_split_processed idempotency
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("manager", IMPLEMENTATIONS, indirect=True)
def test_record_split_idempotency(manager):
    assert manager.is_split_processed("AAPL", "2026-01-01") is False
    manager.record_split("AAPL", "2026-01-01", 2.0)
    assert manager.is_split_processed("AAPL", "2026-01-01") is True

    # Recording the same split a second time must NOT add a duplicate row.
    manager.record_split("AAPL", "2026-01-01", 2.0)
    assert manager.is_split_processed("AAPL", "2026-01-01") is True

    tbl = manager.execute_query("SELECT COUNT(*) AS c FROM splits_processed")
    assert tbl.column("c").to_pylist()[0] == 1


# ---------------------------------------------------------------------------
# 10. delete_ticker_data removes all rows for a ticker
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("manager", IMPLEMENTATIONS, indirect=True)
def test_delete_ticker_data(manager):
    manager.upsert_dataframe(make_df("AAPL", ["2026-01-01", "2026-01-02"], [100.0, 101.0], [1000.0, 2000.0]))
    manager.upsert_dataframe(make_df("MSFT", ["2026-01-01", "2026-01-02"], [200.0, 201.0], [2000.0, 3000.0]))

    manager.delete_ticker_data("AAPL")

    existing = manager.get_existing_tickers()
    assert existing == {"MSFT"}

    tbl = manager.execute_query("SELECT COUNT(*) AS c FROM daily_close")
    assert tbl.column("c").to_pylist()[0] == 2


# ---------------------------------------------------------------------------
# 11. Thread safety: concurrent writes don't corrupt
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("manager", IMPLEMENTATIONS, indirect=True)
def test_concurrent_writes_thread_safety(manager):
    tickers = ["AAPL", "MSFT", "NVDA", "GOOG"]
    errors: list[BaseException] = []

    def worker(ticker: str):
        try:
            df = make_df(
                ticker,
                ["2026-01-01", "2026-01-02"],
                [100.0, 101.0],
                [1000.0, 2000.0],
            )
            manager.upsert_dataframe(df)
        except BaseException as exc:  # noqa: BLE001 - record any failure
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(t,)) for t in tickers]
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    assert errors == [], f"concurrent upsert errors: {errors}"

    existing = manager.get_existing_tickers()
    assert existing == set(tickers)

    tbl = manager.execute_query("SELECT COUNT(*) AS c FROM daily_close")
    assert tbl.column("c").to_pylist()[0] == len(tickers) * 2


# ---------------------------------------------------------------------------
# 12. check_daily_close_health (F9-T5) — report-only health checks
# ---------------------------------------------------------------------------

# A fixed Mon-Thu NYSE session window, injected via ``trading_days`` so
# the tests never depend on the real calendar or the wall clock.
HEALTH_DAYS = ["2026-01-05", "2026-01-06", "2026-01-07", "2026-01-08"]


def _raw_write(manager, sql):
    """Run a write statement through the manager's write connection.

    The two implementations expose different write handles:
    executor -> ``manager.conn``; challenger -> ``manager._write``.
    Only literal SQL is used here so no parameter binding is needed.
    """
    if hasattr(manager, "conn"):
        manager.conn.execute(sql)
    else:
        with manager._write.connection() as conn:
            conn.execute(sql)


def _seed_current(manager, tickers, days=HEALTH_DAYS):
    """Seed each ticker with a clean row on every day in ``days``."""
    for t in tickers:
        manager.upsert_dataframe(
            make_df(t, days, [100.0] * len(days), [1000.0] * len(days))
        )


@pytest.mark.parametrize("manager", IMPLEMENTATIONS, indirect=True)
def test_health_ok_when_data_current(manager):
    """Clean DB + all configured tickers present and current -> OK."""
    _seed_current(manager, ["AAPL", "MSFT"])
    report = manager.check_daily_close_health(
        tickers=["AAPL", "MSFT"], trading_days=HEALTH_DAYS
    )
    assert report["health_status"] == "OK"
    assert report["mode"] == "light"
    assert report["latest_trading_day"] == "2026-01-08"
    checks = report["checks"]
    assert checks["staleness"]["stale_count"] == 0
    assert checks["coverage"]["missing_count"] == 0
    assert checks["integrity"]["violation_count"] == 0
    assert checks["duplicates"]["duplicate_count"] == 0
    # Light mode never runs gap detection.
    assert "gaps" not in checks
    # The whole report must be JSON-serialisable (servers json.dumps it).
    json.dumps(report)


@pytest.mark.parametrize("manager", IMPLEMENTATIONS, indirect=True)
def test_health_stale_ticker_detected(manager):
    """A ticker whose MAX(Date) < latest trading day is stale -> WARN."""
    _seed_current(manager, ["AAPL"])
    _seed_current(manager, ["MSFT"], days=HEALTH_DAYS[:2])
    report = manager.check_daily_close_health(
        tickers=["AAPL", "MSFT"], trading_days=HEALTH_DAYS
    )
    assert report["health_status"] == "WARN"
    staleness = report["checks"]["staleness"]
    assert staleness["stale_count"] == 1
    assert staleness["stale_tickers"] == [
        {"ticker": "MSFT", "last_date": "2026-01-06"}
    ]


@pytest.mark.parametrize("manager", IMPLEMENTATIONS, indirect=True)
def test_health_coverage_missing_ticker(manager):
    """A configured ticker absent from daily_close -> WARN + coverage."""
    _seed_current(manager, ["AAPL", "MSFT"])
    report = manager.check_daily_close_health(
        tickers=["AAPL", "MSFT", "NVDA"], trading_days=HEALTH_DAYS
    )
    assert report["health_status"] == "WARN"
    coverage = report["checks"]["coverage"]
    assert coverage["configured_count"] == 3
    assert coverage["missing_count"] == 1
    assert coverage["missing_tickers"] == ["NVDA"]


@pytest.mark.parametrize("manager", IMPLEMENTATIONS, indirect=True)
def test_health_integrity_violations(manager):
    """NULL/non-positive OHLC, High<Low and negative Volume -> WARN."""
    _seed_current(manager, ["AAPL"])
    # Each statement seeds a distinct violation on its own date.
    _raw_write(
        manager,
        "INSERT INTO daily_close VALUES "
        "('2026-01-09','AAPL',NULL,105,95,102,1000),"   # NULL Open
        "('2026-01-10','AAPL',0,105,95,102,1000),"      # non-positive Open
        "('2026-01-11','AAPL',100,90,95,102,1000),"     # High < Low
        "('2026-01-12','AAPL',100,105,95,102,-5)",      # Volume < 0
    )
    report = manager.check_daily_close_health(
        tickers=["AAPL"], trading_days=HEALTH_DAYS + ["2026-01-12"]
    )
    assert report["health_status"] == "WARN"
    integrity = report["checks"]["integrity"]
    assert integrity["violation_count"] == 4
    reasons = integrity["reason_counts"]
    assert reasons["null_or_nonpositive_ohlc"] == 2
    assert reasons["high_lt_low"] == 1
    assert reasons["negative_volume"] == 1
    found = {(v["ticker"], v["date"], v["reason"])
             for v in integrity["violations"]}
    assert ("AAPL", "2026-01-09", "null_or_nonpositive_ohlc") in found
    assert ("AAPL", "2026-01-10", "null_or_nonpositive_ohlc") in found
    assert ("AAPL", "2026-01-11", "high_lt_low") in found
    assert ("AAPL", "2026-01-12", "negative_volume") in found


@pytest.mark.parametrize("manager", IMPLEMENTATIONS, indirect=True)
def test_health_duplicate_keys_detected(manager):
    """Duplicate (Ticker, Date) pairs are flagged -> WARN.

    The PRIMARY KEY (Date, Ticker) prevents duplicates in production, so
    the table is recreated without it to exercise the defensive check.
    """
    _seed_current(manager, ["AAPL"], days=HEALTH_DAYS[:1])
    _raw_write(manager, "ALTER TABLE daily_close RENAME TO daily_close_pk")
    _raw_write(
        manager,
        "CREATE TABLE daily_close (Date DATE, Ticker VARCHAR, "
        "Open DOUBLE, High DOUBLE, Low DOUBLE, Close DOUBLE, "
        "Volume DOUBLE)",
    )
    _raw_write(manager, "INSERT INTO daily_close SELECT * FROM daily_close_pk")
    _raw_write(manager, "DROP TABLE daily_close_pk")
    # Same (Ticker, Date) inserted twice — now possible without the PK.
    _raw_write(
        manager,
        "INSERT INTO daily_close VALUES "
        "('2026-01-06','AAPL',100,105,95,102,1000),"
        "('2026-01-06','AAPL',101,106,96,103,1001)",
    )
    report = manager.check_daily_close_health(
        tickers=["AAPL"], trading_days=["2026-01-06"]
    )
    assert report["health_status"] == "WARN"
    dup = report["checks"]["duplicates"]
    assert dup["duplicate_count"] == 1
    assert dup["duplicates"] == [
        {"ticker": "AAPL", "date": "2026-01-06", "count": 2}
    ]


@pytest.mark.parametrize("manager", IMPLEMENTATIONS, indirect=True)
def test_health_gap_detection_full_mode(manager):
    """Full mode flags per-ticker missing NYSE sessions in the window."""
    _seed_current(manager, ["AAPL"])
    # MSFT is missing the middle session of the injected window.
    _seed_current(manager, ["MSFT"], days=["2026-01-05", "2026-01-07", "2026-01-08"])
    report = manager.check_daily_close_health(
        tickers=["AAPL", "MSFT"],
        include_gaps=True,
        gap_days=4,
        trading_days=HEALTH_DAYS,
    )
    assert report["health_status"] == "WARN"
    assert report["mode"] == "full"
    gaps = report["checks"]["gaps"]
    assert gaps["gap_days"] == 4
    assert gaps["window_start"] == "2026-01-05"
    assert gaps["window_end"] == "2026-01-08"
    assert gaps["expected_sessions"] == 4
    assert gaps["tickers_with_gaps"] == 1
    assert gaps["total_missing_days"] == 1
    assert gaps["per_ticker"] == [
        {"ticker": "MSFT", "missing_count": 1,
         "missing_days": ["2026-01-06"]}
    ]
    json.dumps(report)


@pytest.mark.parametrize("manager", IMPLEMENTATIONS, indirect=True)
def test_health_gap_window_bounded_by_gap_days(manager):
    """Only the last ``gap_days`` sessions of the calendar are checked."""
    _seed_current(manager, ["AAPL"], days=["2026-01-08"])
    report = manager.check_daily_close_health(
        tickers=["AAPL"],
        include_gaps=True,
        gap_days=2,
        trading_days=HEALTH_DAYS,
    )
    gaps = report["checks"]["gaps"]
    assert gaps["expected_sessions"] == 2
    assert gaps["window_start"] == "2026-01-07"
    assert gaps["window_end"] == "2026-01-08"
    assert gaps["per_ticker"] == [
        {"ticker": "AAPL", "missing_count": 1,
         "missing_days": ["2026-01-07"]}
    ]


@pytest.mark.parametrize("manager", IMPLEMENTATIONS, indirect=True)
def test_health_check_failed_on_broken_db(manager):
    """A failing check (daily_close missing) -> CHECK_FAILED, no raise."""
    _raw_write(manager, "DROP TABLE daily_close")
    report = manager.check_daily_close_health(
        tickers=["AAPL"], trading_days=HEALTH_DAYS
    )
    assert report["health_status"] == "CHECK_FAILED"
    assert "error" in report
    json.dumps(report)


@pytest.mark.parametrize("manager", IMPLEMENTATIONS, indirect=True)
def test_health_check_failed_on_empty_calendar(manager):
    """An undeterminable trading calendar -> CHECK_FAILED."""
    report = manager.check_daily_close_health(
        tickers=["AAPL"], trading_days=[]
    )
    assert report["health_status"] == "CHECK_FAILED"
    assert "error" in report


@pytest.mark.parametrize("manager", IMPLEMENTATIONS, indirect=True)
def test_health_invalid_gap_days_check_failed(manager):
    """A non-positive/non-int gap_days -> CHECK_FAILED, never raises."""
    _seed_current(manager, ["AAPL"])
    for bad in (0, -1, "x", True):
        report = manager.check_daily_close_health(
            include_gaps=True, gap_days=bad, trading_days=HEALTH_DAYS
        )
        assert report["health_status"] == "CHECK_FAILED", bad


@pytest.mark.parametrize("manager", IMPLEMENTATIONS, indirect=True)
def test_health_real_nyse_calendar_path(manager):
    """Without injection, pandas_market_calendars derives the latest
    trading day (offline bundled calendar — no network)."""
    _seed_current(manager, ["AAPL"], days=["2026-01-05"])
    report = manager.check_daily_close_health(tickers=["AAPL"])
    assert report["health_status"] in ("OK", "WARN")
    assert report["latest_trading_day"] is not None
    # 2026-01-05 is far in the past -> the ticker must be stale.
    assert report["checks"]["staleness"]["stale_count"] == 1


@pytest.mark.parametrize("manager", IMPLEMENTATIONS, indirect=True)
def test_health_lists_are_bounded(manager):
    """Per-ticker lists never exceed ``max_items``; totals stay exact."""
    _seed_current(manager, ["AAPL"], days=HEALTH_DAYS[:1])
    report = manager.check_daily_close_health(
        tickers=[f"T{i:02d}" for i in range(60)] + ["AAPL"],
        trading_days=HEALTH_DAYS,
        max_items=5,
    )
    coverage = report["checks"]["coverage"]
    assert coverage["missing_count"] == 60
    assert len(coverage["missing_tickers"]) == 5


# ---------------------------------------------------------------------------
# 13. Ended-session filtering (F9-T7) — only sessions whose market_close +
#     grace_minutes <= now count as expected.
# ---------------------------------------------------------------------------

# Real NYSE sessions around the regression seen live on 2026-09-15: the
# bundled mcal calendar is offline, so no network is needed.  2026-09-15
# (Tue) is a session; ENDED_DAYS are the four sessions before it.
PENDING_DAY = "2026-09-15"
PRIOR_DAY = "2026-09-14"
ENDED_DAYS = ["2026-09-09", "2026-09-10", "2026-09-11", "2026-09-14"]

# The 2026-09-15 NYSE close is 16:00 ET = 20:00 UTC; with the default
# 120-minute grace the session counts from 22:00 UTC onward.
PRE_CLOSE_NOWS = [
    pytest.param(
        datetime(2026, 9, 15, 12, 0, tzinfo=timezone.utc),
        id="pre-open-08:00-ET",
    ),
    pytest.param(
        datetime(2026, 9, 15, 19, 0, tzinfo=timezone.utc),
        id="mid-session-15:00-ET",
    ),
    pytest.param(
        datetime(2026, 9, 15, 21, 0, tzinfo=timezone.utc),
        id="in-grace-17:00-ET",
    ),
]

POST_GRACE_NOWS = [
    pytest.param(
        datetime(2026, 9, 15, 22, 0, tzinfo=timezone.utc),
        id="exact-boundary-close+grace",
    ),
    pytest.param(
        datetime(2026, 9, 15, 23, 0, tzinfo=timezone.utc),
        id="post-grace-19:00-ET",
    ),
]


@pytest.mark.parametrize("manager", IMPLEMENTATIONS, indirect=True)
@pytest.mark.parametrize("now", PRE_CLOSE_NOWS)
def test_health_pending_session_not_expected_pre_close(manager, now):
    """Pre-close/in-grace ``now``: today's session is not yet expected.

    Regression pin for F9-T7 — before the fix a pre-open call compared
    tickers against the *scheduled* session and flagged every ticker as
    stale for missing today's not-yet-finished session.
    """
    _seed_current(manager, ["AAPL", "MSFT"], days=ENDED_DAYS)
    report = manager.check_daily_close_health(
        tickers=["AAPL", "MSFT"], as_of=PENDING_DAY, now=now
    )
    assert report["health_status"] == "OK"
    assert report["latest_trading_day"] == PRIOR_DAY
    assert report["pending_session"] == PENDING_DAY
    assert report["checks"]["staleness"]["stale_count"] == 0
    json.dumps(report)


@pytest.mark.parametrize("manager", IMPLEMENTATIONS, indirect=True)
@pytest.mark.parametrize("now", POST_GRACE_NOWS)
def test_health_post_grace_today_expected(manager, now):
    """After ``market_close + grace_minutes`` today IS expected."""
    _seed_current(manager, ["AAPL", "MSFT"], days=ENDED_DAYS)
    report = manager.check_daily_close_health(
        tickers=["AAPL", "MSFT"], as_of=PENDING_DAY, now=now
    )
    assert report["health_status"] == "WARN"
    assert report["latest_trading_day"] == PENDING_DAY
    assert report["pending_session"] is None
    staleness = report["checks"]["staleness"]
    assert staleness["stale_count"] == 2
    assert staleness["stale_tickers"] == [
        {"ticker": "AAPL", "last_date": PRIOR_DAY},
        {"ticker": "MSFT", "last_date": PRIOR_DAY},
    ]


@pytest.mark.parametrize("manager", IMPLEMENTATIONS, indirect=True)
def test_health_post_grace_data_present_ok(manager):
    """Post-grace with today loaded -> OK, nothing pending."""
    _seed_current(manager, ["AAPL"], days=ENDED_DAYS + [PENDING_DAY])
    report = manager.check_daily_close_health(
        tickers=["AAPL"],
        as_of=PENDING_DAY,
        now=datetime(2026, 9, 15, 23, 0, tzinfo=timezone.utc),
    )
    assert report["health_status"] == "OK"
    assert report["latest_trading_day"] == PENDING_DAY
    assert report["pending_session"] is None


@pytest.mark.parametrize("manager", IMPLEMENTATIONS, indirect=True)
def test_health_gap_window_drops_pending_session(manager):
    """Full mode: the gap window excludes the not-yet-ended session."""
    _seed_current(manager, ["AAPL"], days=ENDED_DAYS)
    report = manager.check_daily_close_health(
        tickers=["AAPL"],
        include_gaps=True,
        gap_days=4,
        as_of=PENDING_DAY,
        now=datetime(2026, 9, 15, 12, 0, tzinfo=timezone.utc),
    )
    assert report["health_status"] == "OK"
    assert report["pending_session"] == PENDING_DAY
    gaps = report["checks"]["gaps"]
    assert gaps["expected_sessions"] == 4
    assert gaps["window_start"] == ENDED_DAYS[0]
    assert gaps["window_end"] == PRIOR_DAY
    assert gaps["total_missing_days"] == 0


@pytest.mark.parametrize("manager", IMPLEMENTATIONS, indirect=True)
def test_health_gap_window_includes_today_post_grace(manager):
    """Full mode post-grace: today joins the window and is flagged."""
    _seed_current(manager, ["AAPL"], days=ENDED_DAYS)
    report = manager.check_daily_close_health(
        tickers=["AAPL"],
        include_gaps=True,
        gap_days=4,
        as_of=PENDING_DAY,
        now=datetime(2026, 9, 15, 23, 0, tzinfo=timezone.utc),
    )
    assert report["health_status"] == "WARN"
    assert report["pending_session"] is None
    gaps = report["checks"]["gaps"]
    assert gaps["expected_sessions"] == 4
    assert gaps["window_end"] == PENDING_DAY
    assert gaps["per_ticker"] == [
        {"ticker": "AAPL", "missing_count": 1,
         "missing_days": [PENDING_DAY]}
    ]


@pytest.mark.parametrize("manager", IMPLEMENTATIONS, indirect=True)
def test_health_naive_now_treated_as_utc(manager):
    """A tz-naive ``now`` is interpreted as UTC."""
    _seed_current(manager, ["AAPL"], days=ENDED_DAYS)
    report = manager.check_daily_close_health(
        tickers=["AAPL"],
        as_of=PENDING_DAY,
        now=datetime(2026, 9, 15, 12, 0),  # naive -> 12:00 UTC -> pre-open
    )
    assert report["health_status"] == "OK"
    assert report["latest_trading_day"] == PRIOR_DAY
    assert report["pending_session"] == PENDING_DAY


@pytest.mark.parametrize("manager", IMPLEMENTATIONS, indirect=True)
def test_health_as_of_without_now_counts_as_of_session(manager):
    """as_of alone preserves the original semantics: the whole as_of day
    counts as elapsed, so a session on as_of is still expected."""
    _seed_current(manager, ["AAPL"], days=ENDED_DAYS + [PENDING_DAY])
    report = manager.check_daily_close_health(
        tickers=["AAPL"], as_of=PENDING_DAY
    )
    assert report["health_status"] == "OK"
    assert report["latest_trading_day"] == PENDING_DAY
    assert report["pending_session"] is None


@pytest.mark.parametrize("manager", IMPLEMENTATIONS, indirect=True)
def test_health_now_without_as_of_bounds_calendar(manager):
    """``now`` alone pins the calendar to now's date, so injected clocks
    can exercise historical sessions."""
    _seed_current(manager, ["AAPL"], days=ENDED_DAYS)
    report = manager.check_daily_close_health(
        tickers=["AAPL"],
        now=datetime(2026, 9, 15, 23, 0, tzinfo=timezone.utc),
    )
    assert report["as_of"] == PENDING_DAY
    assert report["latest_trading_day"] == PENDING_DAY
    assert report["health_status"] == "WARN"


@pytest.mark.parametrize("manager", IMPLEMENTATIONS, indirect=True)
def test_health_injected_trading_days_bypass_pending(manager):
    """Injected ``trading_days``: every listed session is expected and
    nothing is pending, regardless of ``now``."""
    _seed_current(manager, ["AAPL"], days=HEALTH_DAYS)
    report = manager.check_daily_close_health(
        tickers=["AAPL"],
        trading_days=HEALTH_DAYS,
        # A now far before any real close must not filter the injected
        # calendar.
        now=datetime(2020, 1, 1, tzinfo=timezone.utc),
    )
    assert report["health_status"] == "OK"
    assert report["latest_trading_day"] == "2026-01-08"
    assert report["pending_session"] is None


@pytest.mark.parametrize("manager", IMPLEMENTATIONS, indirect=True)
def test_health_weekend_as_of_has_no_pending(manager):
    """A non-session as_of (Sunday) leaves nothing pending."""
    _seed_current(manager, ["AAPL"], days=ENDED_DAYS[:3])
    report = manager.check_daily_close_health(
        tickers=["AAPL"],
        as_of="2026-09-13",
        now=datetime(2026, 9, 13, 12, 0, tzinfo=timezone.utc),
    )
    # Sunday: the schedule stops at Friday 2026-09-11 — already ended.
    assert report["latest_trading_day"] == "2026-09-11"
    assert report["pending_session"] is None
    assert report["health_status"] == "OK"


# ---------------------------------------------------------------------------
# 14. F17-T1: check_fear_greed_health / check_weekly_health /
#     check_metadata_health — report-only checks for the new tables
# ---------------------------------------------------------------------------

from src.db import FEAR_GREED_SUB_INDICATORS  # noqa: E402


def _fg_sql_literal(v):
    """Render a Python scalar as a SQL literal (None -> NULL)."""
    if v is None:
        return "NULL"
    if isinstance(v, str):
        return "'" + v.replace("'", "''") + "'"
    return repr(float(v))


def _seed_fear_greed(manager, rows, series_value=42.0):
    """Seed fear_greed_history; each row is ``(date, fg_score, fg_rating)``.

    All nine sub-indicator value columns get ``series_value`` and every
    ``<key>_rating`` gets 'neutral' so the series check passes; pass
    ``series_value=None`` to leave every series NULL (missing_series).
    """
    series_cols = ", ".join(
        f"{k}, {k}_rating" for k in FEAR_GREED_SUB_INDICATORS
    )
    if series_value is None:
        series_vals = ", ".join(
            "NULL, NULL" for _ in FEAR_GREED_SUB_INDICATORS
        )
    else:
        series_vals = ", ".join(
            f"{series_value}, 'neutral'" for _ in FEAR_GREED_SUB_INDICATORS
        )
    for date, score, rating in rows:
        _raw_write(
            manager,
            "INSERT INTO fear_greed_history "
            f"(date, fg_score, fg_rating, {series_cols}) VALUES "
            f"('{date}', {_fg_sql_literal(score)}, "
            f"{_fg_sql_literal(rating)}, {series_vals})",
        )


@pytest.mark.parametrize("manager", IMPLEMENTATIONS, indirect=True)
def test_fg_health_ok(manager):
    """Current, well-formed fear_greed_history -> OK."""
    _seed_fear_greed(manager, [(d, 25.0, "fear") for d in HEALTH_DAYS])
    report = manager.check_fear_greed_health(trading_days=HEALTH_DAYS)
    assert report["health_status"] == "OK"
    assert report["row_count"] == len(HEALTH_DAYS)
    assert report["latest_trading_day"] == "2026-01-08"
    assert report["pending_session"] is None
    checks = report["checks"]
    assert checks["presence"]["empty"] is False
    assert checks["staleness"]["stale"] is False
    assert checks["staleness"]["latest_date"] == "2026-01-08"
    assert checks["staleness"]["expected_date"] == "2026-01-08"
    assert checks["staleness"]["days_behind"] == 0
    assert checks["integrity"]["null_fg_score_count"] == 0
    assert checks["integrity"]["out_of_range_count"] == 0
    assert checks["integrity"]["invalid_rating_count"] == 0
    assert checks["series"]["missing_series"] == []
    assert checks["series"]["tracked"] == 9
    json.dumps(report)


@pytest.mark.parametrize("manager", IMPLEMENTATIONS, indirect=True)
def test_fg_health_empty_table_warns(manager):
    """An empty fear_greed_history -> WARN (presence), stale=True."""
    report = manager.check_fear_greed_health(trading_days=HEALTH_DAYS)
    assert report["health_status"] == "WARN"
    assert report["row_count"] == 0
    assert report["checks"]["presence"]["empty"] is True
    st = report["checks"]["staleness"]
    assert st["stale"] is True
    assert st["latest_date"] is None
    assert st["expected_date"] == "2026-01-08"
    json.dumps(report)


@pytest.mark.parametrize("manager", IMPLEMENTATIONS, indirect=True)
def test_fg_health_stale_warns(manager):
    """MAX(date) behind the latest ended session -> WARN + evidence."""
    _seed_fear_greed(
        manager, [(d, 50.0, "neutral") for d in HEALTH_DAYS[:2]]
    )
    report = manager.check_fear_greed_health(trading_days=HEALTH_DAYS)
    assert report["health_status"] == "WARN"
    st = report["checks"]["staleness"]
    assert st["stale"] is True
    assert st["latest_date"] == "2026-01-06"
    assert st["expected_date"] == "2026-01-08"
    assert st["days_behind"] == 2


@pytest.mark.parametrize("manager", IMPLEMENTATIONS, indirect=True)
def test_fg_health_pending_session_grace(manager):
    """Pre-close ``now``: today's session is pending, not expected.

    Same boundary as the daily check — the pending session is reported
    but does not count toward staleness until close + grace_minutes.
    """
    _seed_fear_greed(
        manager, [(d, 55.0, "greed") for d in ENDED_DAYS]
    )
    report = manager.check_fear_greed_health(
        as_of=PENDING_DAY,
        now=datetime(2026, 9, 15, 21, 0, tzinfo=timezone.utc),
    )
    assert report["health_status"] == "OK"
    assert report["latest_trading_day"] == PRIOR_DAY
    assert report["pending_session"] == PENDING_DAY
    assert report["checks"]["staleness"]["stale"] is False

    # Post-grace the same session IS expected -> stale.
    report = manager.check_fear_greed_health(
        as_of=PENDING_DAY,
        now=datetime(2026, 9, 15, 23, 0, tzinfo=timezone.utc),
    )
    assert report["health_status"] == "WARN"
    assert report["pending_session"] is None
    assert report["checks"]["staleness"]["expected_date"] == PENDING_DAY
    assert report["checks"]["staleness"]["stale"] is True


@pytest.mark.parametrize("manager", IMPLEMENTATIONS, indirect=True)
def test_fg_health_integrity_violations(manager):
    """Out-of-range fg_score, invalid fg_rating, NULL fg_score -> WARN."""
    _seed_fear_greed(manager, [(d, 50.0, "neutral") for d in HEALTH_DAYS])
    # Bad rows on extra (off-window) dates so MAX(date) stays current.
    _raw_write(
        manager,
        "INSERT INTO fear_greed_history (date, fg_score, fg_rating) VALUES "
        "('2026-01-02', -5, 'neutral'),"      # fg_score < 0
        "('2026-01-03', 150, 'greed'),"       # fg_score > 100
        "('2026-01-04', 50, 'bogus'),"        # invalid rating
        "('2026-01-10', NULL, NULL)",         # NULL fg_score
    )
    report = manager.check_fear_greed_health(trading_days=HEALTH_DAYS)
    assert report["health_status"] == "WARN"
    integ = report["checks"]["integrity"]
    assert integ["out_of_range_count"] == 2
    assert integ["invalid_rating_count"] == 1
    assert integ["null_fg_score_count"] == 1
    oor = {(s["date"], s["fg_score"]) for s in integ["out_of_range_samples"]}
    assert ("2026-01-02", -5.0) in oor
    assert ("2026-01-03", 150.0) in oor
    assert integ["invalid_rating_samples"] == [
        {"date": "2026-01-04", "fg_rating": "bogus"}
    ]
    # NULL rating is allowed — the NULL-score row must NOT also count as
    # an invalid rating.
    assert integ["invalid_rating_count"] == 1
    json.dumps(report)


@pytest.mark.parametrize("manager", IMPLEMENTATIONS, indirect=True)
def test_fg_health_fully_null_series(manager):
    """All-NULL sub-indicator columns -> missing_series lists them."""
    _seed_fear_greed(
        manager,
        [(d, 50.0, "neutral") for d in HEALTH_DAYS],
        series_value=None,
    )
    report = manager.check_fear_greed_health(trading_days=HEALTH_DAYS)
    assert report["health_status"] == "WARN"
    assert report["checks"]["series"]["missing_series"] == list(
        FEAR_GREED_SUB_INDICATORS
    )


@pytest.mark.parametrize("manager", IMPLEMENTATIONS, indirect=True)
def test_fg_health_disabled(manager):
    """enabled=False -> DISABLED immediately (no staleness/integrity)."""
    report = manager.check_fear_greed_health(
        enabled=False, trading_days=HEALTH_DAYS
    )
    assert report["health_status"] == "DISABLED"
    assert "reason" in report
    json.dumps(report)


@pytest.mark.parametrize("manager", IMPLEMENTATIONS, indirect=True)
def test_fg_health_check_failed_on_missing_table(manager):
    """A missing table -> CHECK_FAILED, never raises."""
    _raw_write(manager, "DROP TABLE fear_greed_history")
    report = manager.check_fear_greed_health(trading_days=HEALTH_DAYS)
    assert report["health_status"] == "CHECK_FAILED"
    assert "error" in report
    json.dumps(report)


@pytest.mark.parametrize("manager", IMPLEMENTATIONS, indirect=True)
def test_fg_health_samples_bounded(manager):
    """Bounded samples: counts stay exact while lists cap at max_items."""
    _seed_fear_greed(manager, [(d, 50.0, "neutral") for d in HEALTH_DAYS])
    # 60 out-of-range rows on distinct off-window dates.
    values = ", ".join(
        f"('2025-06-{d:02d}', 200, 'greed')" for d in range(1, 31)
    ) + ", " + ", ".join(
        f"('2025-07-{d:02d}', 200, 'greed')" for d in range(1, 31)
    )
    _raw_write(
        manager,
        "INSERT INTO fear_greed_history (date, fg_score, fg_rating) "
        f"VALUES {values}",
    )
    report = manager.check_fear_greed_health(
        trading_days=HEALTH_DAYS, max_items=5
    )
    integ = report["checks"]["integrity"]
    assert integ["out_of_range_count"] == 60
    assert len(integ["out_of_range_samples"]) == 5


# -- weekly ------------------------------------------------------------

# HEALTH_DAYS is Mon 2026-01-05 .. Thu 2026-01-08; the nominal-Friday key
# of that ISO week is 2026-01-09.
EXPECTED_WEEK = "2026-01-09"
PRIOR_WEEK = "2026-01-02"


def _seed_weekly(manager, ticker_weeks, indicators=True):
    """Seed weekly_close (and weekly_indicators for the same keys).

    ``ticker_weeks`` maps ticker -> [week-friday dates]; rows get clean
    OHLCV values.  ``indicators=False`` skips the indicators insert so
    the parity check sees missing keys.
    """
    for ticker, weeks in ticker_weeks.items():
        for w in weeks:
            _raw_write(
                manager,
                "INSERT INTO weekly_close VALUES "
                f"('{w}', '{ticker}', 100, 105, 95, 102, 1000)",
            )
            if indicators:
                _raw_write(
                    manager,
                    "INSERT INTO weekly_indicators (Date, Ticker) VALUES "
                    f"('{w}', '{ticker}')",
                )


@pytest.mark.parametrize("manager", IMPLEMENTATIONS, indirect=True)
def test_weekly_health_ok(manager):
    """weekly_close + weekly_indicators current and in parity -> OK."""
    _seed_current(manager, ["AAPL", "MSFT"])
    _seed_weekly(
        manager, {"AAPL": [EXPECTED_WEEK], "MSFT": [EXPECTED_WEEK]}
    )
    report = manager.check_weekly_health(trading_days=HEALTH_DAYS)
    assert report["health_status"] == "OK"
    checks = report["checks"]
    assert checks["staleness"]["stale"] is False
    assert checks["staleness"]["latest_week"] == EXPECTED_WEEK
    assert checks["staleness"]["expected_week"] == EXPECTED_WEEK
    assert checks["parity"]["row_count_match"] is True
    assert checks["parity"]["missing_indicator_keys"] == 0
    assert checks["stale_tickers"]["count"] == 0
    assert checks["integrity"]["null_ohlc_count"] == 0
    assert checks["integrity"]["negative_volume_count"] == 0
    json.dumps(report)


@pytest.mark.parametrize("manager", IMPLEMENTATIONS, indirect=True)
def test_weekly_health_stale_warns(manager):
    """MAX(weekly_close.Date) < expected Friday key -> WARN."""
    _seed_current(manager, ["AAPL"])
    _seed_weekly(manager, {"AAPL": [PRIOR_WEEK]})
    report = manager.check_weekly_health(trading_days=HEALTH_DAYS)
    assert report["health_status"] == "WARN"
    st = report["checks"]["staleness"]
    assert st["stale"] is True
    assert st["latest_week"] == PRIOR_WEEK
    assert st["expected_week"] == EXPECTED_WEEK


@pytest.mark.parametrize("manager", IMPLEMENTATIONS, indirect=True)
def test_weekly_health_empty_warns(manager):
    """An empty weekly layer -> WARN (latest_week is NULL -> stale)."""
    report = manager.check_weekly_health(trading_days=HEALTH_DAYS)
    assert report["health_status"] == "WARN"
    st = report["checks"]["staleness"]
    assert st["stale"] is True
    assert st["latest_week"] is None
    assert st["expected_week"] == EXPECTED_WEEK


@pytest.mark.parametrize("manager", IMPLEMENTATIONS, indirect=True)
def test_weekly_health_parity_missing_keys(manager):
    """weekly_close keys absent from weekly_indicators -> WARN."""
    _seed_current(manager, ["AAPL", "MSFT"])
    _seed_weekly(
        manager,
        {"AAPL": [EXPECTED_WEEK], "MSFT": [EXPECTED_WEEK]},
        indicators=False,
    )
    # Backfill indicators for AAPL only -> MSFT key missing.
    _raw_write(
        manager,
        "INSERT INTO weekly_indicators (Date, Ticker) VALUES "
        f"('{EXPECTED_WEEK}', 'AAPL')",
    )
    report = manager.check_weekly_health(trading_days=HEALTH_DAYS)
    assert report["health_status"] == "WARN"
    par = report["checks"]["parity"]
    assert par["weekly_close_rows"] == 2
    assert par["weekly_indicators_rows"] == 1
    assert par["row_count_match"] is False
    assert par["missing_indicator_keys"] == 1
    assert par["missing_keys"] == [
        {"date": EXPECTED_WEEK, "ticker": "MSFT"}
    ]


@pytest.mark.parametrize("manager", IMPLEMENTATIONS, indirect=True)
def test_weekly_health_stale_ticker(manager):
    """A ticker in weekly_close absent from daily_close -> WARN."""
    _seed_current(manager, ["AAPL"])
    _seed_weekly(
        manager,
        {"AAPL": [EXPECTED_WEEK], "GONE": [EXPECTED_WEEK]},
    )
    report = manager.check_weekly_health(trading_days=HEALTH_DAYS)
    assert report["health_status"] == "WARN"
    assert report["checks"]["stale_tickers"]["count"] == 1
    assert report["checks"]["stale_tickers"]["tickers"] == ["GONE"]


@pytest.mark.parametrize("manager", IMPLEMENTATIONS, indirect=True)
def test_weekly_health_integrity_violations(manager):
    """NULL OHLC and Volume < 0 rows on weekly_close -> WARN."""
    _seed_current(manager, ["AAPL"])
    _seed_weekly(manager, {"AAPL": [EXPECTED_WEEK]})
    _raw_write(
        manager,
        "INSERT INTO weekly_close VALUES "
        f"('{PRIOR_WEEK}', 'AAPL', NULL, 105, 95, 102, 1000),"
        f"('{PRIOR_WEEK}', 'MSFT', 100, 105, 95, 102, -5)",
    )
    report = manager.check_weekly_health(trading_days=HEALTH_DAYS)
    assert report["health_status"] == "WARN"
    integ = report["checks"]["integrity"]
    assert integ["null_ohlc_count"] == 1
    assert integ["negative_volume_count"] == 1
    # MSFT's prior-week row is also a missing indicators key / stale
    # week / stale ticker — the WARN reason count doesn't matter, the
    # integrity counts do.
    json.dumps(report)


@pytest.mark.parametrize("manager", IMPLEMENTATIONS, indirect=True)
def test_weekly_health_pending_session_week_boundary(manager):
    """Pending-session grace moves expected_week across a week boundary.

    2026-09-14 is a Monday session.  Pre-close ``now`` -> latest ended
    session is Friday 2026-09-11 -> expected_week is the *prior* week's
    Friday (2026-09-11).  Post-grace -> 2026-09-14 ended -> expected_week
    jumps to this week's Friday (2026-09-18) and the same table is stale.
    """
    monday = "2026-09-14"
    _seed_current(manager, ["AAPL"], days=["2026-09-11"])
    _seed_weekly(manager, {"AAPL": ["2026-09-11"]})
    report = manager.check_weekly_health(
        as_of=monday,
        now=datetime(2026, 9, 14, 21, 0, tzinfo=timezone.utc),
    )
    assert report["health_status"] == "OK"
    assert report["latest_trading_day"] == "2026-09-11"
    assert report["pending_session"] == monday
    assert report["checks"]["staleness"]["expected_week"] == "2026-09-11"
    assert report["checks"]["staleness"]["stale"] is False

    report = manager.check_weekly_health(
        as_of=monday,
        now=datetime(2026, 9, 14, 23, 0, tzinfo=timezone.utc),
    )
    assert report["health_status"] == "WARN"
    assert report["pending_session"] is None
    st = report["checks"]["staleness"]
    assert st["expected_week"] == "2026-09-18"
    assert st["latest_week"] == "2026-09-11"
    assert st["stale"] is True


@pytest.mark.parametrize("manager", IMPLEMENTATIONS, indirect=True)
def test_weekly_health_check_failed_on_missing_table(manager):
    """A missing weekly table -> CHECK_FAILED, never raises."""
    _raw_write(manager, "DROP TABLE weekly_indicators")
    report = manager.check_weekly_health(trading_days=HEALTH_DAYS)
    assert report["health_status"] == "CHECK_FAILED"
    assert "error" in report
    json.dumps(report)


# -- company_metadata ---------------------------------------------------


def _seed_metadata(manager, rows):
    """Seed company_metadata; each row is a dict for upsert_company_metadata."""
    manager.upsert_company_metadata(rows)


@pytest.mark.parametrize("manager", IMPLEMENTATIONS, indirect=True)
def test_metadata_health_ok(manager):
    """All configured tickers present with real fields -> OK."""
    _seed_metadata(
        manager,
        [
            {
                "ticker": "AAPL",
                "company_name": "Apple Inc.",
                "sector": "Technology",
                "industry": "Consumer Electronics",
                "quote_type": "EQUITY",
            },
            {
                "ticker": "MSFT",
                "company_name": "Microsoft",
                "sector": "Technology",
                "industry": "Software",
                "quote_type": "EQUITY",
            },
        ],
    )
    report = manager.check_metadata_health(tickers=["AAPL", "MSFT"])
    assert report["health_status"] == "OK"
    cov = report["checks"]["coverage"]
    assert cov["configured_count"] == 2
    assert cov["missing_count"] == 0
    assert cov["missing_tickers"] == []
    nf = report["checks"]["null_fields"]
    assert nf["row_count"] == 2
    assert nf["null_field_rows"] == 0
    json.dumps(report)


@pytest.mark.parametrize("manager", IMPLEMENTATIONS, indirect=True)
def test_metadata_health_missing_ticker_warns(manager):
    """Configured ticker absent from company_metadata -> WARN."""
    _seed_metadata(
        manager,
        [
            {
                "ticker": "AAPL",
                "company_name": "Apple Inc.",
                "sector": "Technology",
                "industry": "Consumer Electronics",
                "quote_type": "EQUITY",
            }
        ],
    )
    report = manager.check_metadata_health(
        tickers=["AAPL", "MSFT", "NVDA"]
    )
    assert report["health_status"] == "WARN"
    cov = report["checks"]["coverage"]
    assert cov["configured_count"] == 3
    assert cov["missing_count"] == 2
    assert cov["missing_tickers"] == ["MSFT", "NVDA"]


@pytest.mark.parametrize("manager", IMPLEMENTATIONS, indirect=True)
def test_metadata_health_null_rows_informational(manager):
    """All-NULL field rows are legitimate placeholders -> not a WARN."""
    _seed_metadata(
        manager,
        [
            {
                "ticker": "AAPL",
                "company_name": "Apple Inc.",
                "sector": "Technology",
                "industry": "Consumer Electronics",
                "quote_type": "EQUITY",
            },
            {
                "ticker": "ZZZZ",
                "company_name": None,
                "sector": None,
                "industry": None,
                "quote_type": None,
            },
        ],
    )
    report = manager.check_metadata_health(tickers=["AAPL", "ZZZZ"])
    assert report["health_status"] == "OK"
    nf = report["checks"]["null_fields"]
    assert nf["row_count"] == 2
    assert nf["null_field_rows"] == 1


@pytest.mark.parametrize("manager", IMPLEMENTATIONS, indirect=True)
def test_metadata_health_no_tickers_skips_coverage(manager):
    """tickers=None -> coverage skipped; only informational counts."""
    report = manager.check_metadata_health(tickers=None)
    assert report["health_status"] == "OK"
    cov = report["checks"]["coverage"]
    assert cov["configured_count"] == 0
    assert cov["missing_count"] == 0


@pytest.mark.parametrize("manager", IMPLEMENTATIONS, indirect=True)
def test_metadata_health_missing_bounded(manager):
    """missing_tickers is bounded by max_items; counts stay exact."""
    tickers = [f"T{i:02d}" for i in range(60)]
    report = manager.check_metadata_health(tickers=tickers, max_items=5)
    cov = report["checks"]["coverage"]
    assert report["health_status"] == "WARN"
    assert cov["missing_count"] == 60
    assert len(cov["missing_tickers"]) == 5


@pytest.mark.parametrize("manager", IMPLEMENTATIONS, indirect=True)
def test_metadata_health_check_failed_on_missing_table(manager):
    """A missing company_metadata table -> CHECK_FAILED, never raises."""
    _raw_write(manager, "DROP TABLE company_metadata")
    report = manager.check_metadata_health(tickers=["AAPL"])
    assert report["health_status"] == "CHECK_FAILED"
    assert "error" in report
    json.dumps(report)
