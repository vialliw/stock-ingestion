"""Critic tests for DatabaseManager (F2-T3).

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
    """Build a valid daily_quotes DataFrame with explicit, safe dtypes.

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
    assert {"daily_quotes", "splits_processed", "schema_version"} <= names


# ---------------------------------------------------------------------------
# 2. Upsert inserts new rows
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("manager", IMPLEMENTATIONS, indirect=True)
def test_upsert_inserts_new_rows(manager):
    df = make_df("AAPL", ["2026-01-01", "2026-01-02"], [100.0, 101.0], [1000.0, 2000.0])
    n = manager.upsert_dataframe(df)
    assert n == 2
    tbl = manager.execute_query("SELECT COUNT(*) AS c FROM daily_quotes")
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
    tbl = manager.execute_query("SELECT COUNT(*) AS c FROM daily_quotes")
    assert tbl.column("c").to_pylist()[0] == 2

    # Close values must be the updated ones, in date order.
    closes = manager.execute_query(
        "SELECT Close FROM daily_quotes WHERE Ticker = 'AAPL' ORDER BY Date"
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
    tbl = manager.execute_query("SELECT COUNT(*) AS c FROM daily_quotes")
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
        manager.execute_query("DELETE FROM daily_quotes")


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

    tbl = manager.execute_query("SELECT COUNT(*) AS c FROM daily_quotes")
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

    tbl = manager.execute_query("SELECT COUNT(*) AS c FROM daily_quotes")
    assert tbl.column("c").to_pylist()[0] == len(tickers) * 2
