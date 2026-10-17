"""INC-T7 regression tests — weekly rebuild orphan cleanup.

``rebuild_weekly_close`` / ``rebuild_weekly_indicators`` were
upsert-only: deleting ``daily_close`` rows (e.g. via INC-T6
``delete_daily_rows``) left their weekly rollup rows behind — verified
live as the phantom QRVO 2026-10-09 weekly bar that survived a
``weekly_refresh``.  Both rebuilds now anti-join DELETE orphans inside
the same write lock.

All data lives in tmp_path-isolated DuckDB files via the real
``src.db.DatabaseManager`` — no yfinance calls, no real database.
"""

from __future__ import annotations

import datetime as _dt

import pandas as pd
import pytest

from src.db import DatabaseManager


@pytest.fixture
def db(tmp_path):
    mgr = DatabaseManager(str(tmp_path / "orphans.duckdb"))
    try:
        yield mgr
    finally:
        mgr.close()


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


def _weekly_keys(db) -> set:
    tbl = db.execute_query(
        "SELECT Date, Ticker FROM weekly_close ORDER BY Date, Ticker"
    ).to_pydict()
    return set(zip(tbl["Date"], tbl["Ticker"]))


def _indicator_keys(db) -> set:
    tbl = db.execute_query(
        "SELECT Date, Ticker FROM weekly_indicators ORDER BY Date, Ticker"
    ).to_pydict()
    return set(zip(tbl["Date"], tbl["Ticker"]))


def _seed_two_weeks_two_tickers(db) -> None:
    """AAA gets daily rows in two ISO weeks; BBB gets one row in week 1.

    Week-1 nominal Friday = 2024-01-05, week-2 = 2024-01-12.
    """
    db.upsert_dataframe(
        pd.concat(
            [
                make_daily_df(
                    "AAA",
                    [
                        ("2024-01-02", 10, 15, 9, 14, 100),   # wk1 Tue
                        ("2024-01-03", 11, 16, 10, 15, 200),  # wk1 Wed
                        ("2024-01-09", 20, 25, 19, 24, 300),  # wk2 Tue
                        ("2024-01-10", 21, 26, 20, 25, 400),  # wk2 Wed
                    ],
                ),
                make_daily_df(
                    "BBB",
                    [("2024-01-04", 50, 55, 49, 54, 900)],    # wk1 Thu
                ),
            ],
            ignore_index=True,
        )
    )


W1 = _dt.date(2024, 1, 5)
W2 = _dt.date(2024, 1, 12)


def test_orphan_week_removed_from_weekly_close_and_indicators(db):
    """Delete ALL daily rows of one (ticker, week): the weekly_close and
    weekly_indicators rows for that (ticker, week) are removed on the
    next rebuild, while the other week and the other ticker stay."""
    _seed_two_weeks_two_tickers(db)

    assert db.rebuild_weekly_close() == 3
    assert db.rebuild_weekly_indicators() == 3
    assert _weekly_keys(db) == {(W1, "AAA"), (W2, "AAA"), (W1, "BBB")}
    assert _indicator_keys(db) == {(W1, "AAA"), (W2, "AAA"), (W1, "BBB")}

    # INC-T6 eviction path: remove every daily row of (AAA, week 1).
    deleted = db.delete_daily_rows(
        [("AAA", "2024-01-02"), ("AAA", "2024-01-03")]
    )
    assert deleted == 2

    # Rebuild — the orphan week must disappear from BOTH weekly tables.
    assert db.rebuild_weekly_close() == 2
    assert _weekly_keys(db) == {(W2, "AAA"), (W1, "BBB")}
    assert db.rebuild_weekly_indicators() == 2
    assert _indicator_keys(db) == {(W2, "AAA"), (W1, "BBB")}


def test_rebuild_idempotent_after_orphan_cleanup(db):
    """A second rebuild leaves counts AND rows identical — the anti-join
    cleanup does not oscillate or delete produced rows."""
    _seed_two_weeks_two_tickers(db)
    db.rebuild_weekly_close()
    db.rebuild_weekly_indicators()
    db.delete_daily_rows(
        [("AAA", "2024-01-02"), ("AAA", "2024-01-03")]
    )
    db.rebuild_weekly_close()
    db.rebuild_weekly_indicators()

    snap_close = db.execute_query(
        "SELECT * FROM weekly_close ORDER BY Date, Ticker"
    ).to_pydict()
    snap_ind = db.execute_query(
        "SELECT * FROM weekly_indicators ORDER BY Date, Ticker"
    ).to_pydict()

    assert db.rebuild_weekly_close() == 2
    assert db.rebuild_weekly_indicators() == 2
    assert (
        db.execute_query(
            "SELECT * FROM weekly_close ORDER BY Date, Ticker"
        ).to_pydict()
        == snap_close
    )
    assert (
        db.execute_query(
            "SELECT * FROM weekly_indicators ORDER BY Date, Ticker"
        ).to_pydict()
        == snap_ind
    )


def test_deleting_all_daily_rows_clears_both_weekly_tables(db):
    """Emptying daily_close entirely drops every weekly rollup row."""
    _seed_two_weeks_two_tickers(db)
    db.rebuild_weekly_close()
    db.rebuild_weekly_indicators()

    db.delete_daily_rows(
        [
            ("AAA", "2024-01-02"),
            ("AAA", "2024-01-03"),
            ("AAA", "2024-01-09"),
            ("AAA", "2024-01-10"),
            ("BBB", "2024-01-04"),
        ]
    )
    assert db.rebuild_weekly_close() == 0
    assert db.rebuild_weekly_indicators() == 0
    assert _weekly_keys(db) == set()
    assert _indicator_keys(db) == set()


def test_partial_week_deletion_updates_row_not_orphans(db):
    """Deleting SOME (not all) daily rows of a week keeps the weekly row
    but refreshes it from the surviving dailies — the cleanup must only
    remove weeks with zero source rows."""
    _seed_two_weeks_two_tickers(db)
    db.rebuild_weekly_close()

    db.delete_daily_rows([("AAA", "2024-01-02")])
    db.rebuild_weekly_close()

    keys = _weekly_keys(db)
    assert keys == {(W1, "AAA"), (W2, "AAA"), (W1, "BBB")}
    # AAA week-1 row now aggregates only the surviving Wednesday bar.
    row = db.execute_query(
        "SELECT Open, Close, Volume FROM weekly_close "
        "WHERE Ticker = 'AAA' AND Date = '2024-01-05'"
    ).to_pydict()
    assert row["Open"] == [11.0]
    assert row["Close"] == [15.0]
    assert row["Volume"] == [200.0]


def test_orphan_indicators_removed_when_weekly_close_row_deleted(db):
    """Direct surgical delete of a weekly_close row (bypassing the daily
    path) is also cleaned up at the indicators layer on rebuild."""
    _seed_two_weeks_two_tickers(db)
    db.rebuild_weekly_close()
    db.rebuild_weekly_indicators()
    assert (W1, "AAA") in _indicator_keys(db)

    with db._write_lock:
        db.conn.execute(
            "DELETE FROM weekly_close "
            "WHERE Ticker = 'AAA' AND Date = '2024-01-05'"
        )
    # weekly_close row is gone but indicators still carry the orphan
    # until the next rebuild — then it must be removed.
    db.rebuild_weekly_indicators()
    assert _indicator_keys(db) == {(W2, "AAA"), (W1, "BBB")}
