"""Critic tests for the company_metadata load (F10-T1).

Parametrized over BOTH implementations:
  - src.ingestion.IngestionEngine            (executor)
  - src.ingestion_challenger.IngestionEngine (challenger)

ALL yfinance calls are mocked — ``yf.Ticker(t).info`` is replaced per
implementation module, and ``time.sleep`` is patched so the pacing
delay is asserted, not endured.  Tests use ``tmp_path``-isolated
DuckDB files via each implementation's real DatabaseManager.

Covered behaviours (acceptance criteria):
  * skip-existing fetches only tickers missing from company_metadata
    (anti-join — NOT daily_close).
  * ``refresh=True`` re-fetches even already-present tickers.
  * A per-ticker fetch EXCEPTION -> tickers_failed + NO DB row
    (deliberate deviation from the legacy NULL-row-on-exception job —
    a throttled ticker must stay missing so it retries next run).
  * Circuit breaker: N consecutive exceptions abort early, leave the
    rest missing, report aborted=True + reason.
  * A SUCCESSFUL but empty/sparse ``.info`` DOES write a NULL-field row
    (legacy behaviour for genuinely unknown symbols).
  * Rows flush in batch_size chunks DURING the loop, so an aborted run
    keeps its already-flushed progress.
  * ``max_tickers`` caps the fetch list.
  * ``delay_s`` paces the calls (asserted via the time.sleep mock).
  * Validation: non-numeric delay_s, non-int/bool max_tickers, etc.
    raise ValueError.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest

from src.config import PipelineConfig
from src.db import DatabaseManager as _ExecutorDatabaseManager
from src.db_challenger import DatabaseManager as _ChallengerDatabaseManager
from src.ingestion import IngestionEngine as _ExecutorIngestionEngine
from src.ingestion_challenger import IngestionEngine as _ChallengerIngestionEngine


# ---------------------------------------------------------------------------
# Parametrization matrix (same shape as tests/test_ingestion.py)
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


@pytest.fixture
def impl(request):
    return request.param


@pytest.fixture
def db(impl, tmp_path):
    _, DbCls, _ = impl
    mgr = DbCls(str(tmp_path / "test_metadata.duckdb"))
    try:
        yield mgr
    finally:
        mgr.close()


@pytest.fixture
def cfg():
    c = PipelineConfig()
    c.ingestion.tickers = ["AAPL", "MSFT", "GOOG"]
    c.ingestion.exclude_tickers = []
    return c


@pytest.fixture
def engine(impl, db, cfg):
    EngineCls, _, _ = impl
    return EngineCls(db, cfg)


@pytest.fixture
def module_name(impl):
    return impl[2]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

GOOD_INFO: Dict[str, Any] = {
    "longName": "Test Corp",
    "sector": "Technology",
    "industry": "Software",
    "quoteType": "EQUITY",
}


def _ticker_factory(responses: Dict[str, Any], calls: List[str]):
    """Return a ``yf.Ticker`` replacement.

    ``responses`` maps ticker -> dict (returned as ``.info``) or an
    Exception instance (raised — simulating a throttled/failed fetch).
    A ticker absent from the map returns ``GOOD_INFO``.
    """

    def _factory(t):
        calls.append(t)
        v = responses.get(t, GOOD_INFO)
        if isinstance(v, Exception):
            raise v
        mock = MagicMock()
        mock.info = v
        return mock

    return _factory


def _metadata_rows(db) -> Dict[str, Dict[str, Any]]:
    """Read company_metadata into a {ticker: row} dict via the real DB."""
    table = db.execute_query(
        "SELECT ticker, company_name, sector, industry, quote_type "
        "FROM company_metadata"
    )
    cols = table.to_pydict()
    return {
        t: {
            "company_name": cols["company_name"][i],
            "sector": cols["sector"][i],
            "industry": cols["industry"][i],
            "quote_type": cols["quote_type"][i],
        }
        for i, t in enumerate(cols["ticker"])
    }


# ---------------------------------------------------------------------------
# Skip-existing / refresh
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("impl", IMPLEMENTATIONS)
def test_skip_existing_fetches_only_missing(engine, db, cfg, module_name):
    """refresh=False must fetch only tickers absent from company_metadata."""
    db.upsert_company_metadata(
        [
            {
                "ticker": "AAPL",
                "company_name": "Apple Inc.",
                "sector": "Technology",
                "industry": "Consumer Electronics",
                "quote_type": "EQUITY",
            }
        ]
    )
    calls: List[str] = []
    with patch(
        f"{module_name}.yf.Ticker",
        side_effect=_ticker_factory({}, calls),
    ), patch(f"{module_name}.time.sleep") as mock_sleep:
        result = engine.run_metadata_load()

    assert sorted(calls) == ["GOOG", "MSFT"]
    assert result["tickers_fetched"] == 2
    assert result["tickers_skipped"] == 1
    assert result["tickers_failed"] == []
    assert result["rows_upserted"] == 2
    assert result["aborted"] is False
    # AAPL's seeded row is untouched.
    rows = _metadata_rows(db)
    assert rows["AAPL"]["company_name"] == "Apple Inc."
    assert rows["MSFT"]["company_name"] == "Test Corp"
    assert rows["GOOG"]["sector"] == "Technology"
    # Pacing: sleep called once per fetch after the first.
    assert mock_sleep.call_count == len(calls) - 1
    mock_sleep.assert_called_with(0.5)


@pytest.mark.parametrize("impl", IMPLEMENTATIONS)
def test_refresh_refetches_existing(engine, db, cfg, module_name):
    """refresh=True must re-fetch tickers already in company_metadata."""
    db.upsert_company_metadata(
        [
            {
                "ticker": "AAPL",
                "company_name": "Old Name",
                "sector": None,
                "industry": None,
                "quote_type": None,
            }
        ]
    )
    calls: List[str] = []
    with patch(
        f"{module_name}.yf.Ticker",
        side_effect=_ticker_factory({"AAPL": GOOD_INFO}, calls),
    ), patch(f"{module_name}.time.sleep"):
        result = engine.run_metadata_load(tickers=["AAPL"], refresh=True)

    assert calls == ["AAPL"]
    assert result["tickers_fetched"] == 1
    assert result["tickers_skipped"] == 0
    assert _metadata_rows(db)["AAPL"]["company_name"] == "Test Corp"


@pytest.mark.parametrize("impl", IMPLEMENTATIONS)
def test_explicit_tickers_normalized(engine, db, cfg, module_name):
    """Explicit ticker lists are strip/upper/dedup-normalized."""
    calls: List[str] = []
    with patch(
        f"{module_name}.yf.Ticker",
        side_effect=_ticker_factory({}, calls),
    ), patch(f"{module_name}.time.sleep"):
        result = engine.run_metadata_load(
            tickers=[" aapl ", "MSFT", "aapl", "", "MSFT"]
        )

    assert sorted(calls) == ["AAPL", "MSFT"]
    assert result["tickers_fetched"] == 2


# ---------------------------------------------------------------------------
# Exceptions -> failed, no row; circuit breaker
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("impl", IMPLEMENTATIONS)
def test_exception_marks_failed_and_writes_no_row(
    engine, db, cfg, module_name
):
    """A fetch exception -> tickers_failed + NO row (stays retriable).

    Deliberate deviation from the legacy job which wrote a NULL-field
    row on exception — that permanently poisoned the skip-existing
    check under rate-limiting.
    """
    calls: List[str] = []
    with patch(
        f"{module_name}.yf.Ticker",
        side_effect=_ticker_factory(
            {"MSFT": RuntimeError("HTTP 429 Too Many Requests")}, calls
        ),
    ), patch(f"{module_name}.time.sleep"):
        result = engine.run_metadata_load()

    assert result["tickers_fetched"] == 2
    assert result["tickers_failed"] == ["MSFT"]
    assert "MSFT" in result["errors"]
    assert result["aborted"] is False
    rows = _metadata_rows(db)
    assert "MSFT" not in rows  # no NULL-row poisoning
    assert "AAPL" in rows and "GOOG" in rows
    # MSFT is still reported missing so a later run retries it.
    assert db.get_missing_metadata_tickers(["MSFT"]) == ["MSFT"]


@pytest.mark.parametrize("impl", IMPLEMENTATIONS)
def test_circuit_breaker_aborts_and_leaves_remaining_missing(
    engine, db, cfg, module_name
):
    """N consecutive exceptions abort early; unprocessed stay missing."""
    calls: List[str] = []
    with patch(
        f"{module_name}.yf.Ticker",
        side_effect=_ticker_factory(
            {t: RuntimeError("HTTP 429") for t in cfg.ingestion.tickers},
            calls,
        ),
    ), patch(f"{module_name}.time.sleep"):
        result = engine.run_metadata_load(max_consecutive_failures=2)

    assert result["aborted"] is True
    assert result["reason"] is not None
    assert "circuit breaker" in result["reason"]
    assert sorted(result["tickers_failed"]) == ["AAPL", "MSFT"]
    # Only 2 fetch calls — GOOG was never attempted.
    assert sorted(calls) == ["AAPL", "MSFT"]
    # Failures write no row, and the unprocessed ticker was never
    # attempted — ALL THREE stay missing for the next run.
    assert sorted(
        db.get_missing_metadata_tickers(cfg.ingestion.tickers)
    ) == ["AAPL", "GOOG", "MSFT"]
    assert _metadata_rows(db) == {}
    assert result["tickers_skipped"] == 1


@pytest.mark.parametrize("impl", IMPLEMENTATIONS)
def test_consecutive_counter_resets_on_success(
    engine, db, cfg, module_name
):
    """Interleaved failures below the threshold must NOT abort."""
    calls: List[str] = []
    with patch(
        f"{module_name}.yf.Ticker",
        side_effect=_ticker_factory(
            {
                "AAPL": RuntimeError("boom"),
                "GOOG": RuntimeError("boom"),
            },
            calls,
        ),
    ), patch(f"{module_name}.time.sleep"):
        # AAPL fails, MSFT succeeds (reset), GOOG fails -> 1, 0, 1.
        result = engine.run_metadata_load(max_consecutive_failures=2)

    assert result["aborted"] is False
    assert result["tickers_fetched"] == 1
    assert sorted(result["tickers_failed"]) == ["AAPL", "GOOG"]
    assert sorted(calls) == ["AAPL", "GOOG", "MSFT"]


# ---------------------------------------------------------------------------
# Successful-but-empty .info -> NULL-field row
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("impl", IMPLEMENTATIONS)
@pytest.mark.parametrize("empty_info", [{}, None])
def test_empty_info_writes_null_row(
    engine, db, cfg, module_name, empty_info
):
    """A successful empty/sparse .info writes a NULL-field row so the
    ticker stops counting as missing (legacy behaviour)."""
    calls: List[str] = []
    with patch(
        f"{module_name}.yf.Ticker",
        side_effect=_ticker_factory({"AAPL": empty_info}, calls),
    ), patch(f"{module_name}.time.sleep"):
        result = engine.run_metadata_load(tickers=["AAPL"])

    assert result["tickers_fetched"] == 1
    assert result["tickers_failed"] == []
    assert result["rows_upserted"] == 1
    row = _metadata_rows(db)["AAPL"]
    assert row["company_name"] is None
    assert row["sector"] is None
    assert row["industry"] is None
    assert row["quote_type"] is None
    # Present now — skip-existing will not refetch it.
    assert db.get_missing_metadata_tickers(["AAPL"]) == []


@pytest.mark.parametrize("impl", IMPLEMENTATIONS)
def test_sparse_info_partial_fields(engine, db, cfg, module_name):
    """Sparse .info maps what exists and NULLs the rest."""
    sparse = {"longName": "Sparse Co", "quoteType": "ETF"}
    with patch(
        f"{module_name}.yf.Ticker",
        side_effect=_ticker_factory({"AAPL": sparse}, []),
    ), patch(f"{module_name}.time.sleep"):
        engine.run_metadata_load(tickers=["AAPL"])

    row = _metadata_rows(db)["AAPL"]
    assert row["company_name"] == "Sparse Co"
    assert row["sector"] is None
    assert row["industry"] is None
    assert row["quote_type"] == "ETF"


# ---------------------------------------------------------------------------
# Incremental batch flush / progress kept on abort
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("impl", IMPLEMENTATIONS)
def test_incremental_batch_flush(engine, db, cfg, module_name):
    """Rows upsert in batch_size chunks during the loop, plus a final
    flush — verified via upsert call boundaries."""
    flush_sizes: List[int] = []
    real_upsert = db.upsert_company_metadata

    def _spy(rows):
        flush_sizes.append(len(rows))
        return real_upsert(rows)

    with patch(
        f"{module_name}.yf.Ticker",
        side_effect=_ticker_factory({}, []),
    ), patch(f"{module_name}.time.sleep"), patch.object(
        db, "upsert_company_metadata", side_effect=_spy
    ):
        result = engine.run_metadata_load(batch_size=2)

    # 3 tickers, batch_size=2 -> flushes of [2, 1].
    assert flush_sizes == [2, 1]
    assert result["rows_upserted"] == 3
    assert len(_metadata_rows(db)) == 3


@pytest.mark.parametrize("impl", IMPLEMENTATIONS)
def test_abort_keeps_flushed_progress(engine, db, cfg, module_name):
    """An aborted run retains rows already flushed before the breaker."""
    cfg.ingestion.tickers = ["AAA", "BBB", "CCC", "DDD"]
    calls: List[str] = []
    with patch(
        f"{module_name}.yf.Ticker",
        side_effect=_ticker_factory(
            {
                "BBB": RuntimeError("429"),
                "CCC": RuntimeError("429"),
            },
            calls,
        ),
    ), patch(f"{module_name}.time.sleep"):
        # batch_size=1 -> AAA flushes immediately; BBB+CCC then trip the
        # breaker (2 consecutive) and DDD is never attempted.
        result = engine.run_metadata_load(
            batch_size=1, max_consecutive_failures=2
        )

    assert result["aborted"] is True
    assert result["tickers_fetched"] == 1
    assert sorted(result["tickers_failed"]) == ["BBB", "CCC"]
    rows = _metadata_rows(db)
    assert "AAA" in rows  # flushed before the abort
    assert "DDD" not in rows
    assert calls == ["AAA", "BBB", "CCC"]


# ---------------------------------------------------------------------------
# max_tickers cap
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("impl", IMPLEMENTATIONS)
def test_max_tickers_caps_fetch(engine, db, cfg, module_name):
    calls: List[str] = []
    with patch(
        f"{module_name}.yf.Ticker",
        side_effect=_ticker_factory({}, calls),
    ), patch(f"{module_name}.time.sleep"):
        result = engine.run_metadata_load(max_tickers=2)

    assert len(calls) == 2
    assert result["tickers_fetched"] == 2
    assert result["tickers_skipped"] == 1  # the capped-off ticker


# ---------------------------------------------------------------------------
# delay pacing
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("impl", IMPLEMENTATIONS)
def test_delay_pacing_uses_delay_s(engine, db, cfg, module_name):
    with patch(
        f"{module_name}.yf.Ticker",
        side_effect=_ticker_factory({}, []),
    ), patch(f"{module_name}.time.sleep") as mock_sleep:
        engine.run_metadata_load(delay_s=1.25)

    # 3 tickers -> sleeps before calls 2 and 3, each with delay_s.
    assert mock_sleep.call_count == 2
    for c in mock_sleep.call_args_list:
        assert c.args[0] == 1.25


# ---------------------------------------------------------------------------
# Argument validation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("impl", IMPLEMENTATIONS)
@pytest.mark.parametrize(
    "kwargs",
    [
        {"delay_s": "fast"},
        {"delay_s": True},
        {"delay_s": -1},
        {"max_tickers": "5"},
        {"max_tickers": True},
        {"max_tickers": 0},
        {"max_tickers": 2.5},
        {"max_consecutive_failures": 0},
        {"max_consecutive_failures": True},
        {"batch_size": 0},
        {"batch_size": True},
        {"refresh": "yes"},
    ],
)
def test_invalid_arguments_raise_valueerror(
    engine, db, cfg, module_name, kwargs
):
    with pytest.raises(ValueError):
        engine.run_metadata_load(**kwargs)


# ---------------------------------------------------------------------------
# Result shape
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("impl", IMPLEMENTATIONS)
def test_result_shape(engine, db, cfg, module_name):
    with patch(
        f"{module_name}.yf.Ticker",
        side_effect=_ticker_factory({}, []),
    ), patch(f"{module_name}.time.sleep"):
        result = engine.run_metadata_load(tickers=["AAPL"])

    for key in (
        "tickers_fetched",
        "tickers_skipped",
        "tickers_failed",
        "rows_upserted",
        "aborted",
        "errors",
        "duration_ms",
    ):
        assert key in result, f"missing key: {key}"
    assert isinstance(result["tickers_failed"], list)
    assert isinstance(result["errors"], dict)
    assert isinstance(result["duration_ms"], int)
