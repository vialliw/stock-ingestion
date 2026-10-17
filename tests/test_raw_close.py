"""Tests for R1A-T1: RawClose column (schema v9) and the single
``auto_adjust=False`` batch-pull adjustment contract.

Covers:
  - fresh DDL includes ``RawClose``; migration v8 -> v9 adds the column
    to an existing ``daily_close`` without losing rows and is
    idempotent;
  - ``upsert_dataframe`` writes ``RawClose``;
  - ``_normalize_batch_df`` produces ``RawClose`` + adjusted OHLC for
    single-ticker flat frames and both multi-ticker MultiIndex level
    orders, including synthetic corporate-action ratio arithmetic;
  - rows that cannot yield an adjusted Close and a finite non-NULL
    ``RawClose`` are dropped (zero/negative market values are retained);
  - all load choke points (on_request, retry, split re-download) emit
    ``RawClose``;
  - legacy ``do_put`` schema is exactly 8 columns — the old 7-column
    payload is rejected.
"""

from __future__ import annotations

import datetime as dt
from unittest.mock import MagicMock, patch

import duckdb
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.flight as flight
import pytest

from src.config import PipelineConfig
from src.db import DatabaseManager, SCHEMA_VERSION
from src.flight_server import MarketDataFlightServer, _DAILY_CLOSE_SCHEMA
from src.ingestion import IngestionEngine


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------

def _download_frame(tickers, rows, layout="ticker_first"):
    """Build a yfinance-style frame.

    ``rows``: list of (date, Open, High, Low, Close, AdjClose, Volume)
    applied to every ticker in ``tickers``. ``layout`` selects the
    MultiIndex level order; single-ticker callers may pass
    ``layout="flat"``.
    """
    idx = pd.DatetimeIndex([r[0] for r in rows], name="Date")
    fields = ["Open", "High", "Low", "Close", "Adj Close", "Volume"]
    data = np.array([r[1:] for r in rows], dtype=float)
    if layout == "flat" or len(tickers) == 1 and layout == "flat":
        return pd.DataFrame(data, index=idx, columns=fields)
    pieces = {}
    for t in tickers:
        pieces[t] = pd.DataFrame(data, index=idx, columns=fields)
    if layout == "ticker_first":
        # group_by='ticker': columns are (ticker, field)
        return pd.concat(pieces, axis=1)
    # (field, ticker): current yfinance default level order
    by_field = {f: pd.concat({t: pieces[t][f] for t in tickers}, axis=1)
                for f in fields}
    return pd.concat(by_field, axis=1)


@pytest.fixture
def cfg():
    c = PipelineConfig()
    c.ingestion.tickers = ["AAPL", "MSFT"]
    return c


@pytest.fixture
def db(tmp_path):
    mgr = DatabaseManager(str(tmp_path / "test.duckdb"))
    try:
        yield mgr
    finally:
        mgr.close()


@pytest.fixture
def engine(db, cfg):
    return IngestionEngine(db, cfg)


# ---------------------------------------------------------------------------
# Schema: fresh DDL + v8 -> v9 migration
# ---------------------------------------------------------------------------

def test_fresh_db_has_rawclose(db):
    cols = {
        r[0]
        for r in db.conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'daily_close'"
        ).fetchall()
    }
    assert "RawClose" in cols
    assert SCHEMA_VERSION == 9
    version = db.conn.execute(
        "SELECT MAX(version) FROM schema_version"
    ).fetchone()[0]
    assert version == 9


def test_migration_v8_to_v9_preserves_rows(tmp_path):
    """A v8 database gains RawClose without row loss; reopen is a no-op."""
    path = str(tmp_path / "v8.duckdb")
    raw = duckdb.connect(path)
    raw.execute(
        "CREATE TABLE schema_version (version INTEGER NOT NULL, "
        "applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    )
    raw.execute(
        "CREATE TABLE daily_close ("
        "Date DATE NOT NULL, Ticker VARCHAR NOT NULL, Open DOUBLE, "
        "High DOUBLE, Low DOUBLE, Close DOUBLE, Volume DOUBLE, "
        "PRIMARY KEY (Date, Ticker))"
    )
    raw.execute(
        "INSERT INTO daily_close VALUES "
        "('2024-01-02','AAPL',1,2,0.5,1.5,100),"
        "('2024-01-03','AAPL',2,3,1.5,2.5,200)"
    )
    raw.execute("INSERT INTO schema_version (version) VALUES (8)")
    raw.close()

    mgr = DatabaseManager(path)
    out = mgr.execute_query(
        "SELECT Ticker, Close, RawClose FROM daily_close ORDER BY Date"
    ).to_pandas()
    assert len(out) == 2  # no row loss
    assert out["RawClose"].isna().all()  # pre-migration rows stay NULL
    assert mgr.conn.execute(
        "SELECT MAX(version) FROM schema_version"
    ).fetchone()[0] == 9
    mgr.close()

    # Idempotent: a second open must not fail or duplicate rows.
    mgr2 = DatabaseManager(path)
    assert mgr2.conn.execute(
        "SELECT COUNT(*) FROM daily_close"
    ).fetchone()[0] == 2
    mgr2.close()


# ---------------------------------------------------------------------------
# Upsert writes RawClose
# ---------------------------------------------------------------------------

def _df(rows):
    return pd.DataFrame(rows)


def test_upsert_writes_rawclose(db):
    db.upsert_dataframe(_df([
        {"Date": "2024-01-02", "Ticker": "AAPL", "Open": 90.0,
         "High": 91.0, "Low": 89.0, "Close": 90.5, "Volume": 10.0,
         "RawClose": 181.0},
    ]))
    row = db.execute_query(
        "SELECT Close, RawClose FROM daily_close WHERE Ticker='AAPL'"
    ).to_pandas().iloc[0]
    assert row["Close"] == 90.5
    assert row["RawClose"] == 181.0

    # MERGE update path also writes RawClose.
    db.upsert_dataframe(_df([
        {"Date": "2024-01-02", "Ticker": "AAPL", "Open": 92.0,
         "High": 93.0, "Low": 91.0, "Close": 92.5, "Volume": 20.0,
         "RawClose": 185.0},
    ]))
    row = db.execute_query(
        "SELECT Close, RawClose FROM daily_close WHERE Ticker='AAPL'"
    ).to_pandas().iloc[0]
    assert row["Close"] == 92.5
    assert row["RawClose"] == 185.0


def test_upsert_without_rawclose_column_stores_null(db):
    """Legacy 7-field inserts remain supported and initially store NULL."""
    db.upsert_dataframe(_df([
        {"Date": "2024-01-02", "Ticker": "MSFT", "Open": 1.0,
         "High": 2.0, "Low": 0.5, "Close": 1.5, "Volume": 5.0},
    ]))
    val = db.execute_query(
        "SELECT RawClose FROM daily_close WHERE Ticker='MSFT'"
    ).to_pandas().iloc[0, 0]
    assert pd.isna(val)


def test_legacy_upsert_preserves_existing_raw_close(db):
    """A matched seven-field update cannot erase durable RawClose data."""
    key = {"Date": "2024-01-02", "Ticker": "AAPL"}
    db.upsert_dataframe(_df([{
        **key, "Open": 90.0, "High": 91.0, "Low": 89.0,
        "Close": 90.5, "Volume": 10.0, "RawClose": 181.0,
    }]))

    # Legacy internal caller: same key, deliberately no RawClose field.
    db.upsert_dataframe(_df([{
        **key, "Open": 92.0, "High": 93.0, "Low": 91.0,
        "Close": 92.5, "Volume": 20.0,
    }]))
    row = db.execute_query(
        "SELECT Close, Volume, RawClose FROM daily_close WHERE Ticker='AAPL'"
    ).to_pandas().iloc[0]
    assert row["Close"] == 92.5
    assert row["Volume"] == 20.0
    assert row["RawClose"] == 181.0

    # The preservation guard is not over-broad: an explicit valid
    # RawClose still updates the durable value on the matched path.
    db.upsert_dataframe(_df([{
        **key, "Open": 94.0, "High": 95.0, "Low": 93.0,
        "Close": 94.5, "Volume": 30.0, "RawClose": 189.0,
    }]))
    row = db.execute_query(
        "SELECT Close, RawClose FROM daily_close WHERE Ticker='AAPL'"
    ).to_pandas().iloc[0]
    assert row["Close"] == 94.5
    assert row["RawClose"] == 189.0


def test_mixed_frame_preserves_missing_raw_close(db):
    """In a mixed batch, rows without RawClose do not overwrite stored ones."""
    # Seed two tickers with populated RawClose values.
    db.upsert_dataframe(_df([
        {"Date": "2024-01-02", "Ticker": "AAPL", "Open": 90.0,
         "High": 91.0, "Low": 89.0, "Close": 90.5, "Volume": 10.0,
         "RawClose": 181.0},
        {"Date": "2024-01-02", "Ticker": "MSFT", "Open": 10.0,
         "High": 11.0, "Low": 9.0, "Close": 10.5, "Volume": 100.0,
         "RawClose": 21.0},
    ]))

    # Mixed batch: AAPL omits RawClose (legacy row), MSFT supplies a
    # valid RawClose update.
    db.upsert_dataframe(_df([
        {"Date": "2024-01-02", "Ticker": "AAPL", "Open": 91.0,
         "High": 92.0, "Low": 90.0, "Close": 91.5, "Volume": 11.0},
        {"Date": "2024-01-02", "Ticker": "MSFT", "Open": 11.0,
         "High": 12.0, "Low": 10.0, "Close": 11.5, "Volume": 110.0,
         "RawClose": 22.0},
    ]))

    aapl, msft = db.execute_query(
        "SELECT Ticker, Close, RawClose FROM daily_close ORDER BY Ticker"
    ).to_pandas().set_index("Ticker").loc[["AAPL", "MSFT"]].itertuples()
    assert aapl.Close == 91.5
    assert aapl.RawClose == 181.0  # preserved despite omitted in source
    assert msft.Close == 11.5
    assert msft.RawClose == 22.0   # explicitly updated


def test_mixed_frame_invalid_raw_close_values_preserve_target(db):
    """NULL and non-finite RawClose values preserve stored ones."""
    key = {"Date": "2024-01-02", "Ticker": "AAPL"}
    db.upsert_dataframe(_df([{
        **key, "Open": 90.0, "High": 91.0, "Low": 89.0,
        "Close": 90.5, "Volume": 10.0, "RawClose": 181.0,
    }]))

    db.upsert_dataframe(_df([
        {**key, "Open": 91.0, "High": 92.0, "Low": 90.0,
         "Close": 91.5, "Volume": 11.0, "RawClose": None},
        {"Date": "2024-01-03", "Ticker": "AAPL", "Open": 92.0,
         "High": 93.0, "Low": 91.0, "Close": 92.5, "Volume": 12.0,
         "RawClose": np.nan},
    ]))

    rows = db.execute_query(
        "SELECT Date, Close, RawClose FROM daily_close WHERE Ticker='AAPL' "
        "ORDER BY Date"
    ).to_pandas()
    # Row with explicit NULL RawClose should preserve existing value.
    assert rows.iloc[0]["Close"] == 91.5
    assert rows.iloc[0]["RawClose"] == 181.0
    # New row with NaN RawClose inserts NULL (legacy insert behavior).
    assert rows.iloc[1]["Close"] == 92.5
    assert pd.isna(rows.iloc[1]["RawClose"])

    # Infinite source values also preserve an existing populated
    # RawClose on a matched update.
    db.upsert_dataframe(_df([
        {**key, "Open": 96.0, "High": 97.0, "Low": 95.0,
         "Close": 96.5, "Volume": 16.0, "RawClose": np.inf},
    ]))
    val = db.execute_query(
        "SELECT Close, RawClose FROM daily_close WHERE Ticker='AAPL' "
        "AND Date='2024-01-02'"
    ).to_pandas().iloc[0]
    assert val["Close"] == 96.5
    assert val["RawClose"] == 181.0


# ---------------------------------------------------------------------------
# Normalization: layouts and adjustment arithmetic
# ---------------------------------------------------------------------------

def test_normalize_single_ticker_flat(engine):
    rows = [("2024-01-02", 100.0, 102.0, 99.0, 101.0, 90.9, 1000.0)]
    df, ok, failed, _ = engine._normalize_batch_df(
        _download_frame(["AAPL"], rows, layout="flat"), ["AAPL"]
    )
    assert ok == ["AAPL"] and not failed
    r = df.iloc[0]
    ratio = 90.9 / 101.0
    assert r["RawClose"] == pytest.approx(101.0)
    assert r["Close"] == pytest.approx(90.9)
    assert r["Open"] == pytest.approx(100.0 * ratio)
    assert r["High"] == pytest.approx(102.0 * ratio)
    assert r["Low"] == pytest.approx(99.0 * ratio)


@pytest.mark.parametrize("layout", ["ticker_first", "field_first"])
def test_normalize_multi_ticker_layouts(engine, layout):
    rows = [("2024-01-02", 10.0, 11.0, 9.0, 10.5, 10.0, 500.0)]
    frame = _download_frame(["AAPL", "MSFT"], rows, layout=layout)
    df, ok, failed, _ = engine._normalize_batch_df(
        frame, ["AAPL", "MSFT"]
    )
    assert sorted(ok) == ["AAPL", "MSFT"] and not failed
    assert set(df["Ticker"]) == {"AAPL", "MSFT"}
    for _, r in df.iterrows():
        assert r["RawClose"] == pytest.approx(10.5)
        assert r["Close"] == pytest.approx(10.0)


def test_normalize_corporate_action_ratio(engine):
    """Synthetic 2:1 split: pre-split raw close 100 adjusted to 50."""
    rows = [
        # pre-split bar: vendor raw 100, adjusted 50 (ratio 0.5)
        ("2024-01-02", 100.0, 110.0, 95.0, 100.0, 50.0, 1000.0),
        # post-split bar: raw == adjusted (ratio 1.0)
        ("2024-01-03", 50.0, 55.0, 48.0, 50.0, 50.0, 2000.0),
    ]
    df, ok, failed, _ = engine._normalize_batch_df(
        _download_frame(["AAPL"], rows, layout="flat"), ["AAPL"]
    )
    assert ok == ["AAPL"] and not failed
    df = df.sort_values("Date").reset_index(drop=True)
    pre, post = df.iloc[0], df.iloc[1]
    assert pre["RawClose"] == pytest.approx(100.0)
    assert pre["Close"] == pytest.approx(50.0)
    assert pre["Open"] == pytest.approx(50.0)   # 100 * 0.5
    assert pre["High"] == pytest.approx(55.0)   # 110 * 0.5
    assert pre["Low"] == pytest.approx(47.5)    # 95 * 0.5
    assert post["RawClose"] == pytest.approx(50.0)
    assert post["Close"] == pytest.approx(50.0)


def test_normalize_frame_without_adj_close_defaults_ratio_one(engine):
    idx = pd.DatetimeIndex(["2024-01-02"], name="Date")
    frame = pd.DataFrame(
        [[10.0, 11.0, 9.0, 10.5, 500.0]], index=idx,
        columns=["Open", "High", "Low", "Close", "Volume"],
    )
    df, ok, failed, _ = engine._normalize_batch_df(frame, ["AAPL"])
    assert ok == ["AAPL"] and not failed
    r = df.iloc[0]
    assert r["RawClose"] == pytest.approx(10.5)
    assert r["Close"] == pytest.approx(10.5)


def test_normalize_drops_invalid_rawclose_rows(engine):
    rows = [
        ("2024-01-02", 10.0, 11.0, 9.0, 10.5, 10.0, 100.0),   # valid
        ("2024-01-05", 10.0, 11.0, 9.0, 10.5, np.nan, 100.0),  # NaN adj
        ("2024-01-08", 10.0, 11.0, 9.0, np.nan, 10.0, 100.0),  # NaN raw
        ("2024-01-09", 10.0, 11.0, 9.0, np.inf, 10.0, 100.0),  # inf raw
    ]
    df, ok, failed, _ = engine._normalize_batch_df(
        _download_frame(["AAPL"], rows, layout="flat"), ["AAPL"]
    )
    assert ok == ["AAPL"] and not failed
    assert len(df) == 1
    assert df.iloc[0]["RawClose"] == pytest.approx(10.5)
    assert df["RawClose"].notna().all()


def test_finite_nonpositive_raw_close_is_retained(engine, db):
    """Zero and negative market values normalize and update durably."""
    rows = [
        # ^IRX-style zero bar: undefined adjustment ratio uses identity.
        ("2024-01-02", 0.0, 0.01, -0.01, 0.0, 0.0, 100.0),
        # WTI-style negative bar: finite Adj/Raw ratio remains valid.
        ("2024-01-03", -20.0, -18.0, -40.0, -37.0, -37.0, 200.0),
    ]
    normalized, ok, failed, _ = engine._normalize_batch_df(
        _download_frame(["CL=F"], rows, layout="flat"), ["CL=F"]
    )
    assert ok == ["CL=F"] and not failed
    assert normalized["RawClose"].tolist() == [0.0, -37.0]
    assert normalized["Close"].tolist() == [0.0, -37.0]
    assert np.isfinite(normalized[["Open", "High", "Low", "Close"]]).all().all()

    # Matched updates with finite zero/negative RawClose replace populated
    # targets rather than being mistaken for missing values.
    seeded = normalized.copy()
    seeded["RawClose"] = [1.0, 1.0]
    db.upsert_dataframe(seeded)
    db.upsert_dataframe(normalized)
    stored = db.execute_query(
        "SELECT Date, RawClose FROM daily_close WHERE Ticker='CL=F' ORDER BY Date"
    ).to_pandas()
    assert stored["RawClose"].tolist() == [0.0, -37.0]


def test_normalize_all_invalid_rows_marks_ticker_failed(engine):
    rows = [("2024-01-02", 10.0, 11.0, 9.0, np.inf, 10.0, 100.0)]
    df, ok, failed, errors = engine._normalize_batch_df(
        _download_frame(["AAPL"], rows, layout="flat"), ["AAPL"]
    )
    assert df.empty and ok == [] and failed == ["AAPL"]
    assert "RawClose" in errors["AAPL"]


# ---------------------------------------------------------------------------
# Choke points: every load path emits RawClose
# ---------------------------------------------------------------------------

def test_on_request_load_stores_rawclose(engine, db):
    rows = [("2024-01-02", 100.0, 110.0, 95.0, 100.0, 50.0, 1000.0)]
    frame = _download_frame(["AAPL"], rows, layout="ticker_first")
    with patch(
        "src.ingestion.yf.download", return_value=frame
    ), patch.object(engine, "_weekly_bridge", return_value={}), patch.object(
        engine, "_fear_greed_bridge", return_value={}
    ):
        result = engine.run_on_request_load(
            ["AAPL"], "2024-01-01", "2024-01-05"
        )
    assert result["rows_upserted"] == 1
    out = db.execute_query(
        "SELECT Close, RawClose FROM daily_close WHERE Ticker='AAPL'"
    ).to_pandas()
    assert out.iloc[0]["RawClose"] == pytest.approx(100.0)
    assert out.iloc[0]["Close"] == pytest.approx(50.0)


def test_retry_path_stores_rawclose(engine, db):
    """Transient empty dataset -> retry download still yields RawClose."""
    good_rows = [("2024-01-02", 10.0, 11.0, 9.0, 10.5, 10.0, 500.0)]
    empty_cols = pd.MultiIndex.from_product(
        [["AAPL"], ["Open", "High", "Low", "Close", "Adj Close", "Volume"]]
    )
    first = pd.DataFrame(columns=empty_cols)
    retry = _download_frame(["AAPL"], good_rows, layout="ticker_first")
    with patch(
        "src.ingestion.yf.download", side_effect=[first, retry]
    ), patch("src.ingestion.time.sleep", return_value=None), patch.object(
        engine, "_confirm_delisted", return_value=False
    ):
        df, ok, failed, errors = engine.fetch_and_clean(
            ["AAPL"], "2024-01-01", "2024-01-05"
        )
    assert ok == ["AAPL"] and failed == []
    assert df.iloc[0]["RawClose"] == pytest.approx(10.5)


def test_split_redownload_path_stores_rawclose(engine, db):
    """The split re-download branch also produces RawClose."""
    # Seed existing data so the ticker is eligible for split checks.
    db.upsert_dataframe(_df([
        {"Date": "2024-01-02", "Ticker": "AAPL", "Open": 1.0,
         "High": 2.0, "Low": 0.5, "Close": 1.5, "Volume": 5.0,
         "RawClose": 1.5},
    ]))
    splits = pd.Series(
        [2.0], index=pd.to_datetime(["2024-01-10"]), name="splits"
    )
    ticker_mock = MagicMock()
    ticker_mock.splits = splits
    redl_rows = [("2024-01-02", 1.0, 2.0, 0.5, 1.5, 1.5, 5.0)]
    redl = _download_frame(["AAPL"], redl_rows, layout="ticker_first")
    with patch(
        "src.ingestion.yf.Ticker", return_value=ticker_mock
    ), patch("src.ingestion.yf.download", return_value=redl):
        engine._check_and_apply_splits(["AAPL"])
    out = db.execute_query(
        "SELECT RawClose FROM daily_close WHERE Ticker='AAPL'"
    ).to_pandas()
    assert out.iloc[0]["RawClose"] == pytest.approx(1.5)
    assert out["RawClose"].notna().all()


# ---------------------------------------------------------------------------
# do_put: exact 8-column schema, old 7-column payload rejected
# ---------------------------------------------------------------------------

def _server(db, cfg):
    class _TestServer(MarketDataFlightServer):
        def __init__(self, db_mgr, cfg):
            self.db = db_mgr
            self.engine = None
            self.cfg = cfg

    return _TestServer(db, cfg)


def test_do_put_schema_is_eight_columns():
    assert _DAILY_CLOSE_SCHEMA.names == [
        "Date", "Ticker", "Open", "High", "Low", "Close", "Volume",
        "RawClose",
    ]
    assert _DAILY_CLOSE_SCHEMA.field("RawClose").type == pa.float64()


def test_do_put_upserts_with_rawclose(db, cfg):
    server = _server(db, cfg)
    table = pa.table(
        {
            "Date": pa.array([dt.date(2024, 1, 2)], type=pa.date32()),
            "Ticker": pa.array(["AAPL"], type=pa.string()),
            "Open": pa.array([90.0], type=pa.float64()),
            "High": pa.array([91.0], type=pa.float64()),
            "Low": pa.array([89.0], type=pa.float64()),
            "Close": pa.array([90.5], type=pa.float64()),
            "Volume": pa.array([100.0], type=pa.float64()),
            "RawClose": pa.array([181.0], type=pa.float64()),
        },
        schema=_DAILY_CLOSE_SCHEMA,
    )
    reader = MagicMock()
    reader.read_all.return_value = table
    server.do_put(None, None, reader, MagicMock())
    row = db.execute_query(
        "SELECT Close, RawClose FROM daily_close WHERE Ticker='AAPL'"
    ).to_pandas().iloc[0]
    assert row["RawClose"] == 181.0


def test_do_put_rejects_legacy_seven_column_payload(db, cfg):
    server = _server(db, cfg)
    old_schema = pa.schema(
        [
            pa.field("Date", pa.date32()),
            pa.field("Ticker", pa.string()),
            pa.field("Open", pa.float64()),
            pa.field("High", pa.float64()),
            pa.field("Low", pa.float64()),
            pa.field("Close", pa.float64()),
            pa.field("Volume", pa.float64()),
        ]
    )
    table = pa.table(
        {
            "Date": pa.array([dt.date(2024, 1, 2)], type=pa.date32()),
            "Ticker": pa.array(["AAPL"], type=pa.string()),
            "Open": pa.array([1.0], type=pa.float64()),
            "High": pa.array([2.0], type=pa.float64()),
            "Low": pa.array([0.5], type=pa.float64()),
            "Close": pa.array([1.5], type=pa.float64()),
            "Volume": pa.array([100.0], type=pa.float64()),
        },
        schema=old_schema,
    )
    reader = MagicMock()
    reader.read_all.return_value = table
    with pytest.raises(flight.FlightServerError):
        server.do_put(None, None, reader, MagicMock())
    assert db.execute_query(
        "SELECT COUNT(*) AS n FROM daily_close"
    ).to_pandas().iloc[0, 0] == 0


def test_batch_download_requests_auto_adjust_false(engine):
    frame = _download_frame(["AAPL"], [
        ("2024-01-02", 1.0, 2.0, 0.5, 1.5, 1.5, 5.0)
    ], layout="ticker_first")
    with patch(
        "src.ingestion.yf.download", return_value=frame
    ) as mock_dl:
        engine._batch_download(["AAPL"], "2024-01-01", "2024-01-05")
    assert mock_dl.call_args.kwargs["auto_adjust"] is False
