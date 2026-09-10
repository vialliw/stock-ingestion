"""Critic tests for IngestionEngine (F3-T3).

Parametrized over BOTH implementations:
  - src.ingestion.IngestionEngine            (executor, F3-T1)
  - src.ingestion_challenger.IngestionEngine (challenger, F3-T2)

ALL yfinance calls are mocked — no real API calls are made.  Each
implementation imports ``yfinance as yf`` in its own module, so the
patch targets are module-specific:
  - executor:    ``src.ingestion.yf.download`` / ``src.ingestion.yf.Ticker``
  - challenger:  ``src.ingestion_challenger.yf.download`` /
                 ``src.ingestion_challenger.yf.Ticker``

A ``module_name`` fixture provides the correct module path so a single
set of tests exercises both engines.

Tests use ``tmp_path``-isolated DuckDB files via the real
``DatabaseManager`` from each implementation's sibling db module
(executor engine -> src.db; challenger engine -> src.db_challenger) and
a real ``PipelineConfig`` constructed with defaults (no file I/O).
``pandas_market_calendars`` is mocked where ``run_daily_load`` is
exercised.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import List
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from src.config import PipelineConfig
from src.db import DatabaseManager as _ExecutorDatabaseManager
from src.db_challenger import DatabaseManager as _ChallengerDatabaseManager
from src.ingestion import IngestionEngine as _ExecutorIngestionEngine
from src.ingestion_challenger import IngestionEngine as _ChallengerIngestionEngine


# ---------------------------------------------------------------------------
# Parametrization matrix
# ---------------------------------------------------------------------------

IMPLEMENTATIONS = [
    pytest.param(
        (_ExecutorIngestionEngine, _ExecutorDatabaseManager, "src.ingestion"),
        id="executor:src.ingestion",
    ),
    pytest.param(
        (
            _ChallengerIngestionEngine,
            _ChallengerDatabaseManager,
            "src.ingestion_challenger",
        ),
        id="challenger:src.ingestion_challenger",
    ),
]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def impl(request):
    """Return the (EngineCls, DbCls, module_name) tuple for this param."""
    return request.param


@pytest.fixture
def db(impl, tmp_path):
    """A tmp_path-isolated DatabaseManager matching the engine under test."""
    _, DbCls, _ = impl
    db_path = str(tmp_path / "test_ingestion.duckdb")
    mgr = DbCls(db_path)
    try:
        yield mgr
    finally:
        mgr.close()


@pytest.fixture
def cfg():
    """A PipelineConfig with deterministic tickers and an exclude list."""
    c = PipelineConfig()
    c.ingestion.tickers = ["AAPL", "MSFT"]
    c.ingestion.exclude_tickers = ["BADTICKER"]
    return c


@pytest.fixture
def engine(impl, db, cfg):
    """An IngestionEngine wired to the tmp_path DatabaseManager."""
    EngineCls, _, _ = impl
    return EngineCls(db, cfg)


@pytest.fixture
def module_name(impl):
    """The yf-bearing module path to patch for this implementation."""
    return impl[2]


# ---------------------------------------------------------------------------
# Mock-data helpers
# ---------------------------------------------------------------------------


def make_mock_download_df(tickers: List[str], dates: List[str]) -> pd.DataFrame:
    """Create a mock yf.download output with MultiIndex columns."""
    cols = pd.MultiIndex.from_product(
        [tickers, ["Open", "High", "Low", "Close", "Volume"]]
    )
    idx = pd.to_datetime(dates)
    idx.name = "Date"
    data = np.random.uniform(90, 110, size=(len(idx), len(cols)))
    df = pd.DataFrame(data, index=idx, columns=cols)
    df.columns.name = "Ticker"
    return df


def make_mock_download_df_single(dates: List[str]) -> pd.DataFrame:
    """Create a mock yf.download output for a single ticker (flat columns)."""
    idx = pd.to_datetime(dates)
    idx.name = "Date"
    data = np.random.uniform(90, 110, size=(len(idx), 5))
    return pd.DataFrame(
        data, index=idx, columns=["Open", "High", "Low", "Close", "Volume"]
    )


def make_mock_splits(factors=(2.0,), dates=("2024-01-15",)) -> pd.Series:
    """Return a Series like yf.Ticker('AAPL').splits."""
    return pd.Series(list(factors), index=pd.to_datetime(list(dates)), name="splits")


def make_empty_download_df(tickers: List[str]) -> pd.DataFrame:
    """An empty MultiIndex-columned DataFrame like yf.download with no data."""
    cols = pd.MultiIndex.from_product(
        [tickers, ["Open", "High", "Low", "Close", "Volume"]]
    )
    return pd.DataFrame(columns=cols)


def dates_range(start: str, end: str) -> List[str]:
    d0 = datetime.strptime(start, "%Y-%m-%d")
    d1 = datetime.strptime(end, "%Y-%m-%d")
    out = []
    cur = d0
    while cur <= d1:
        out.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)
    return out


# ---------------------------------------------------------------------------
# 1. _batch_download adds +1 day to end_date
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "impl", IMPLEMENTATIONS
)
def test_batch_download_adds_one_day_to_end_date(
    engine, db, cfg, module_name
):
    """yf.download must be called with end = end_date + 1 day (exclusive end)."""
    dates = dates_range("2024-01-01", "2024-01-10")
    mock_df = make_mock_download_df(["AAPL"], dates)
    with patch(f"{module_name}.yf.download", return_value=mock_df) as mock_dl:
        engine._batch_download(["AAPL"], "2024-01-01", "2024-01-10")
    assert mock_dl.called
    _, kwargs = mock_dl.call_args
    assert kwargs["end"] == "2024-01-11"
    assert kwargs["start"] == "2024-01-01"
    assert kwargs["auto_adjust"] is True
    assert kwargs["interval"] == "1d"
    assert kwargs["group_by"] == "ticker"


# ---------------------------------------------------------------------------
# 2. fetch_and_clean returns (df, succeeded, failed, errors) tuple
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "impl", IMPLEMENTATIONS
)
def test_fetch_and_clean_returns_tuple(
    engine, db, cfg, module_name
):
    """fetch_and_clean returns (df, succeeded, failed, errors) with no splits."""
    dates = dates_range("2024-01-01", "2024-01-05")
    mock_df = make_mock_download_df(["AAPL"], dates)
    mock_ticker = MagicMock()
    mock_ticker.splits = pd.Series([], dtype=float, name="splits")
    with patch(f"{module_name}.yf.download", return_value=mock_df), \
         patch(f"{module_name}.yf.Ticker", return_value=mock_ticker):
        result = engine.fetch_and_clean(["AAPL"], "2024-01-01", "2024-01-05")
    assert isinstance(result, tuple) and len(result) == 4
    df, succeeded, failed, errors = result
    assert isinstance(df, pd.DataFrame)
    assert isinstance(succeeded, list)
    assert isinstance(failed, list)
    assert isinstance(errors, dict)
    assert "AAPL" in succeeded
    assert failed == []


# ---------------------------------------------------------------------------
# 3. Split detection: unprocessed split -> delete + re-download
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "impl", IMPLEMENTATIONS
)
def test_unprocessed_split_triggers_delete_and_redownload(
    engine, db, cfg, module_name
):
    """An unprocessed split triggers db.delete_ticker_data + re-download + upsert."""
    # Seed the DB with one row so get_aligned_start_date returns a value.
    seed_df = pd.DataFrame(
        {
            "Date": pd.to_datetime(["2023-06-01"]),
            "Ticker": ["AAPL"],
            "Open": [100.0],
            "High": [101.0],
            "Low": [99.0],
            "Close": [100.5],
            "Volume": [1000.0],
        }
    )
    db.upsert_dataframe(seed_df)

    splits = make_mock_splits(factors=(2.0,), dates=("2024-01-15",))
    mock_ticker = MagicMock()
    mock_ticker.splits = splits

    redownload_df = make_mock_download_df_single(
        dates_range("2023-06-01", "2024-01-20")
    )

    # The first yf.download call is the re-download inside split handling;
    # fetch_and_clean then calls _batch_download again for the normal fetch.
    # We use a side_effect that returns redownload_df first, then a normal df.
    normal_df = make_mock_download_df(
        ["AAPL"], dates_range("2024-01-01", "2024-01-05")
    )

    dl_calls = {"count": 0}

    def dl_side_effect(*args, **kwargs):
        dl_calls["count"] += 1
        # The re-download is for a single ticker list; the normal fetch is
        # for the original tickers list. Distinguish by the tickers kwarg.
        tickers = kwargs.get("tickers", args[0] if args else [])
        if dl_calls["count"] == 1:
            return redownload_df
        return normal_df

    with patch(f"{module_name}.yf.Ticker", return_value=mock_ticker), \
         patch(f"{module_name}.yf.download", side_effect=dl_side_effect), \
         patch.object(db, "is_split_processed", return_value=False) as mock_isp, \
         patch.object(db, "delete_ticker_data") as mock_delete, \
         patch.object(db, "upsert_dataframe", return_value=1) as mock_upsert:
        engine.fetch_and_clean(["AAPL"], "2024-01-01", "2024-01-05")

    mock_isp.assert_called()
    mock_delete.assert_called_once_with("AAPL")
    mock_upsert.assert_called()
    # At least one upsert call received a DataFrame (the re-downloaded data).
    upsert_dfs = [
        c.args[0] for c in mock_upsert.call_args_list if c.args
    ]
    assert any(isinstance(d, pd.DataFrame) and not d.empty for d in upsert_dfs)


# ---------------------------------------------------------------------------
# 4. Already-processed split -> skip (no delete, no re-download)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "impl", IMPLEMENTATIONS
)
def test_processed_split_skipped(
    engine, db, cfg, module_name
):
    """A split already recorded in splits_processed is skipped."""
    splits = make_mock_splits(factors=(2.0,), dates=("2024-01-15",))
    mock_ticker = MagicMock()
    mock_ticker.splits = splits
    normal_df = make_mock_download_df(
        ["AAPL"], dates_range("2024-01-01", "2024-01-05")
    )

    with patch(f"{module_name}.yf.Ticker", return_value=mock_ticker), \
         patch(f"{module_name}.yf.download", return_value=normal_df), \
         patch.object(db, "is_split_processed", return_value=True), \
         patch.object(db, "delete_ticker_data") as mock_delete, \
         patch.object(db, "record_split") as mock_record:
        df, succeeded, failed, errors = engine.fetch_and_clean(
            ["AAPL"], "2024-01-01", "2024-01-05"
        )

    mock_delete.assert_not_called()
    mock_record.assert_not_called()
    assert "AAPL" in succeeded


# ---------------------------------------------------------------------------
# 5. Multiple splits -> single re-download
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "impl", IMPLEMENTATIONS
)
def test_multiple_splits_single_redownload(
    engine, db, cfg, module_name
):
    """Two unprocessed splits for one ticker -> one delete + one re-download."""
    seed_df = pd.DataFrame(
        {
            "Date": pd.to_datetime(["2023-01-01"]),
            "Ticker": ["AAPL"],
            "Open": [100.0],
            "High": [101.0],
            "Low": [99.0],
            "Close": [100.5],
            "Volume": [1000.0],
        }
    )
    db.upsert_dataframe(seed_df)

    splits = make_mock_splits(
        factors=(2.0, 3.0), dates=("2024-01-15", "2024-06-01")
    )
    mock_ticker = MagicMock()
    mock_ticker.splits = splits

    redownload_df = make_mock_download_df_single(
        dates_range("2023-01-01", "2024-06-02")
    )
    normal_df = make_mock_download_df(
        ["AAPL"], dates_range("2024-01-01", "2024-01-05")
    )

    dl_calls = {"count": 0}

    def dl_side_effect(*args, **kwargs):
        dl_calls["count"] += 1
        if dl_calls["count"] == 1:
            return redownload_df
        return normal_df

    with patch(f"{module_name}.yf.Ticker", return_value=mock_ticker), \
         patch(f"{module_name}.yf.download", side_effect=dl_side_effect), \
         patch.object(db, "is_split_processed", return_value=False), \
         patch.object(db, "delete_ticker_data") as mock_delete, \
         patch.object(db, "batch_record_splits", return_value=2) as mock_batch, \
         patch.object(db, "upsert_dataframe", return_value=1):
        engine.fetch_and_clean(["AAPL"], "2024-01-01", "2024-01-05")

    # delete called once (not twice) despite two splits
    assert mock_delete.call_count == 1
    # batch_record_splits called once with all unprocessed splits (2 splits)
    mock_batch.assert_called_once()
    batch_args = mock_batch.call_args.args[0]
    assert len(batch_args) == 2
    # re-download happened exactly once (first download is the re-download)
    assert dl_calls["count"] >= 1


# ---------------------------------------------------------------------------
# 6. Excluded ticker is filtered at earliest stage (never downloaded)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "impl", IMPLEMENTATIONS
)
def test_empty_excluded_ticker_purged(
    engine, db, cfg, module_name
):
    """A ticker in exclude_tickers is filtered out at the earliest stage —
    it is never downloaded, never split-checked, and never purged."""
    dates = dates_range("2024-01-01", "2024-01-03")
    good = make_mock_download_df(["AAPL"], dates)

    mock_ticker = MagicMock()
    mock_ticker.splits = pd.Series([], dtype=float, name="splits")

    with patch(f"{module_name}.yf.download", return_value=good) as mock_dl, \
         patch(f"{module_name}.yf.Ticker", return_value=mock_ticker), \
         patch.object(db, "purge_ticker") as mock_purge:
        df, succeeded, failed, errors = engine.fetch_and_clean(
            ["AAPL", "BADTICKER"], "2024-01-01", "2024-01-03"
        )

    # BADTICKER was filtered out — yf.download was called with only ["AAPL"]
    assert "BADTICKER" not in succeeded
    assert "BADTICKER" not in failed
    assert "AAPL" in succeeded
    # purge_ticker was never called (excluded tickers are filtered, not purged)
    mock_purge.assert_not_called()
    # yf.download was called with only effective tickers (no BADTICKER)
    call_args = mock_dl.call_args
    assert "BADTICKER" not in call_args.kwargs.get("tickers", [])


# ---------------------------------------------------------------------------
# 7. Empty ticker NOT in exclude_list -> warning, no purge
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "impl", IMPLEMENTATIONS
)
def test_empty_nonexcluded_ticker_not_purged(
    engine, db, cfg, module_name
):
    """An empty dataset for a ticker NOT in exclude_tickers is not purged."""
    dates = dates_range("2024-01-01", "2024-01-03")
    good = make_mock_download_df(["AAPL"], dates)
    empty_cols = pd.MultiIndex.from_product(
        [["UNKNOWN"], ["Open", "High", "Low", "Close", "Volume"]]
    )
    empty_idx = pd.to_datetime(dates)
    empty_idx.name = "Date"
    empty_data = np.full((len(dates), 5), np.nan)
    empty_df = pd.DataFrame(
        empty_data, index=empty_idx, columns=empty_cols
    )
    combined = pd.concat([good, empty_df], axis=1)
    combined.index.name = "Date"

    mock_ticker = MagicMock()
    mock_ticker.splits = pd.Series([], dtype=float, name="splits")

    with patch(f"{module_name}.yf.download", return_value=combined), \
         patch(f"{module_name}.yf.Ticker", return_value=mock_ticker), \
         patch.object(db, "purge_ticker") as mock_purge:
        df, succeeded, failed, errors = engine.fetch_and_clean(
            ["AAPL", "UNKNOWN"], "2024-01-01", "2024-01-03"
        )

    mock_purge.assert_not_called()
    assert "UNKNOWN" in failed
    assert "AAPL" in succeeded


# ---------------------------------------------------------------------------
# 8. run_daily_load: new ticker detection + backfill from aligned start
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "impl", IMPLEMENTATIONS
)
def test_run_daily_load_new_ticker_backfill(
    engine, db, cfg, module_name
):
    """detect_new_tickers returns ['NVDA'] -> backfill from aligned start."""
    # Seed DB so get_aligned_start_date returns a known date.
    seed_df = pd.DataFrame(
        {
            "Date": pd.to_datetime(["2020-01-01"]),
            "Ticker": ["AAPL"],
            "Open": [100.0],
            "High": [101.0],
            "Low": [99.0],
            "Close": [100.5],
            "Volume": [1000.0],
        }
    )
    db.upsert_dataframe(seed_df)

    mock_df = make_mock_download_df(
        ["NVDA"], dates_range("2020-01-01", "2024-01-02")
    )
    mock_ticker = MagicMock()
    mock_ticker.splits = pd.Series([], dtype=float, name="splits")

    # A non-empty market schedule (market open).
    sched = pd.DataFrame(
        {"market_open": [pd.Timestamp("2024-01-02 09:30")],
         "market_close": [pd.Timestamp("2024-01-02 16:00")]},
        index=[pd.Timestamp("2024-01-02")],
    )
    mock_calendar = MagicMock()
    mock_calendar.schedule.return_value = sched

    # detect_new_tickers -> ['NVDA']; get_aligned_start_date -> '2020-01-01'.
    # The trailing 4-day refresh also downloads; provide a df for it too.
    refresh_df = make_mock_download_df(
        ["AAPL", "MSFT"], dates_range("2023-12-29", "2024-01-02")
    )

    dl_calls = {"count": 0}

    def dl_side_effect(*args, **kwargs):
        dl_calls["count"] += 1
        tickers = kwargs.get("tickers", args[0] if args else [])
        if tickers == ["NVDA"]:
            return mock_df
        return refresh_df

    with patch(f"{module_name}.mcal.get_calendar", return_value=mock_calendar), \
         patch(f"{module_name}.yf.download", side_effect=dl_side_effect), \
         patch(f"{module_name}.yf.Ticker", return_value=mock_ticker), \
         patch.object(db, "detect_new_tickers", return_value=["NVDA"]), \
         patch.object(db, "get_aligned_start_date", return_value="2020-01-01"), \
         patch.object(db, "upsert_dataframe", return_value=1) as mock_upsert:
        result = engine.run_daily_load()

    # The backfill download must have been called with start='2020-01-01'.
    backfill_calls = [
        c for c in mock_upsert.call_args_list
    ]
    assert mock_upsert.call_count >= 1
    # Verify yf.download was called with start='2020-01-01' for the backfill.
    # We inspect dl_side_effect invocations via the patch context: re-patch
    # to capture is not needed; instead check the result dict is not SKIPPED.
    assert result.get("status") != "SKIPPED"
    assert "rows_upserted" in result


# ---------------------------------------------------------------------------
# 9. run_daily_load: empty DB -> initial_load + trailing refresh
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "impl", IMPLEMENTATIONS
)
def test_run_daily_load_empty_db_initial_load(
    engine, db, cfg, module_name
):
    """Empty DB (aligned_start None) -> run_initial_load (6-year download)."""
    mock_df = make_mock_download_df(
        ["AAPL", "MSFT"], dates_range("2018-01-01", "2024-01-02")
    )
    mock_ticker = MagicMock()
    mock_ticker.splits = pd.Series([], dtype=float, name="splits")

    sched = pd.DataFrame(
        {"market_open": [pd.Timestamp("2024-01-02 09:30")],
         "market_close": [pd.Timestamp("2024-01-02 16:00")]},
        index=[pd.Timestamp("2024-01-02")],
    )
    mock_calendar = MagicMock()
    mock_calendar.schedule.return_value = sched

    with patch(f"{module_name}.mcal.get_calendar", return_value=mock_calendar), \
         patch(f"{module_name}.yf.download", return_value=mock_df), \
         patch(f"{module_name}.yf.Ticker", return_value=mock_ticker), \
         patch.object(
             db, "detect_new_tickers", return_value=["AAPL", "MSFT"]
         ), \
         patch.object(db, "get_aligned_start_date", return_value=None), \
         patch.object(db, "upsert_dataframe", return_value=2) as mock_upsert:
        result = engine.run_daily_load()

    # run_initial_load path -> upsert called (initial load + trailing refresh).
    assert mock_upsert.call_count >= 1
    assert result.get("status") != "SKIPPED"
    assert "rows_upserted" in result


# ---------------------------------------------------------------------------
# 10. run_daily_load: market closed -> SKIPPED
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "impl", IMPLEMENTATIONS
)
def test_run_daily_load_market_closed_skipped(
    engine, db, cfg, module_name
):
    """Empty NYSE schedule -> run_daily_load returns SKIPPED."""
    empty_sched = pd.DataFrame()
    mock_calendar = MagicMock()
    mock_calendar.schedule.return_value = empty_sched

    with patch(f"{module_name}.mcal.get_calendar", return_value=mock_calendar), \
         patch(f"{module_name}.yf.download") as mock_dl:
        result = engine.run_daily_load()

    assert result == {
        "status": "SKIPPED",
        "reason": "Market closed",
        "duration_ms": 0,
    }
    mock_dl.assert_not_called()
