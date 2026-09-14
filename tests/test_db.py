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
