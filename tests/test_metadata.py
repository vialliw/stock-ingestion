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


# ---------------------------------------------------------------------------
# SEC-T3: metadata_load `overrides` (manual company_metadata corrections)
#
# Executor-only: ``src.ingestion_challenger`` does not implement the
# overrides payload (same divergence precedent as the F24 signal_pipeline
# tests in test_flight_server.py), so these tests use the executor engine
# + DB classes directly instead of the parametrized ``impl`` fixtures.
# ---------------------------------------------------------------------------


@pytest.fixture
def exec_db(tmp_path):
    mgr = _ExecutorDatabaseManager(str(tmp_path / "test_overrides.duckdb"))
    try:
        yield mgr
    finally:
        mgr.close()


@pytest.fixture
def exec_engine(exec_db, cfg):
    return _ExecutorIngestionEngine(exec_db, cfg)


def _seed_metadata(db, ticker="AAA"):
    """Seed one fully-populated company_metadata row."""
    db.upsert_company_metadata(
        [
            {
                "ticker": ticker,
                "company_name": "Stored Name",
                "sector": "Stored Sector",
                "industry": "Stored Industry",
                "quote_type": "EQUITY",
            }
        ]
    )


class TestMetadataOverrides:
    """Merge-semantics manual corrections for company_metadata (SEC-T3)."""

    def test_partial_override_preserves_stored_fields(
        self, exec_engine, exec_db
    ):
        """A partial override must NOT NULL out untouched fields."""
        _seed_metadata(exec_db)
        result = exec_engine.run_metadata_load(
            tickers=[],
            overrides=[{"ticker": "AAA", "sector": "Healthcare"}],
        )
        assert result["overrides_applied"] == 1
        row = _metadata_rows(exec_db)["AAA"]
        assert row["sector"] == "Healthcare"  # overwritten
        assert row["company_name"] == "Stored Name"  # preserved
        assert row["industry"] == "Stored Industry"  # preserved
        assert row["quote_type"] == "EQUITY"  # preserved

    def test_full_override_replaces_all_fields(self, exec_engine, exec_db):
        _seed_metadata(exec_db)
        exec_engine.run_metadata_load(
            tickers=[],
            overrides=[
                {
                    "ticker": "AAA",
                    "company_name": "New Co",
                    "sector": "Technology",
                    "industry": "Software",
                    "quote_type": "ETF",
                }
            ],
        )
        row = _metadata_rows(exec_db)["AAA"]
        assert row == {
            "company_name": "New Co",
            "sector": "Technology",
            "industry": "Software",
            "quote_type": "ETF",
        }

    def test_new_ticker_inserts_nulls_for_unprovided_fields(
        self, exec_engine, exec_db
    ):
        result = exec_engine.run_metadata_load(
            tickers=[],
            overrides=[{"ticker": "NEW1", "sector": "Energy"}],
        )
        assert result["overrides_applied"] == 1
        row = _metadata_rows(exec_db)["NEW1"]
        assert row["sector"] == "Energy"
        assert row["company_name"] is None
        assert row["industry"] is None
        assert row["quote_type"] is None
        # Present now — no longer "missing" for skip-existing.
        assert exec_db.get_missing_metadata_tickers(["NEW1"]) == []

    def test_null_field_value_treated_as_absent(
        self, exec_engine, exec_db
    ):
        """Explicit null -> merge semantics keep the stored value."""
        _seed_metadata(exec_db)
        exec_engine.run_metadata_load(
            tickers=[],
            overrides=[
                {"ticker": "AAA", "sector": None, "industry": "New Ind"}
            ],
        )
        row = _metadata_rows(exec_db)["AAA"]
        assert row["sector"] == "Stored Sector"  # null did NOT overwrite
        assert row["industry"] == "New Ind"

    def test_ticker_normalized_and_last_wins_dedup(
        self, exec_engine, exec_db
    ):
        """Lowercase/whitespace tickers normalize; duplicate entries
        collapse last-wins."""
        result = exec_engine.run_metadata_load(
            tickers=[],
            overrides=[
                {"ticker": " aapl ", "sector": "First"},
                {"ticker": "AAPL", "sector": "Second"},
            ],
        )
        assert result["overrides_applied"] == 1
        assert _metadata_rows(exec_db)["AAPL"]["sector"] == "Second"

    def test_overrides_only_call_performs_no_fetch(
        self, exec_engine, exec_db
    ):
        """tickers=[] + overrides -> zero yfinance calls."""
        calls: List[str] = []
        with patch(
            "src.ingestion.yf.Ticker",
            side_effect=_ticker_factory({}, calls),
        ):
            result = exec_engine.run_metadata_load(
                tickers=[],
                overrides=[{"ticker": "AAA", "sector": "Energy"}],
            )
        assert calls == []
        assert result["tickers_fetched"] == 0
        assert result["overrides_applied"] == 1

    def test_overrides_win_over_same_call_fetch(
        self, exec_engine, exec_db
    ):
        """Ordering: yfinance fetch runs FIRST, overrides applied after —
        so a correction wins over a same-call refresh."""
        _seed_metadata(exec_db)
        with patch(
            "src.ingestion.yf.Ticker",
            side_effect=_ticker_factory({"AAA": GOOD_INFO}, []),
        ), patch("src.ingestion.time.sleep"):
            result = exec_engine.run_metadata_load(
                tickers=["AAA"],
                refresh=True,
                overrides=[{"ticker": "AAA", "sector": "Corrected"}],
            )
        assert result["tickers_fetched"] == 1
        row = _metadata_rows(exec_db)["AAA"]
        assert row["sector"] == "Corrected"  # override won over fetch
        # Fetch populated the other fields; merge preserved them.
        assert row["company_name"] == "Test Corp"
        assert row["industry"] == "Software"
        assert row["quote_type"] == "EQUITY"

    def test_override_report_fields(self, exec_engine, exec_db):
        """Per-override outcome is reported: applied fields + created."""
        _seed_metadata(exec_db)
        result = exec_engine.run_metadata_load(
            tickers=[],
            overrides=[
                {"ticker": "AAA", "sector": "X", "industry": "Y"},
                {"ticker": "NEW2", "quote_type": "EQUITY"},
            ],
        )
        assert result["overrides_applied"] == 2
        report = {r["ticker"]: r for r in result["overrides_report"]}
        assert report["AAA"]["applied_fields"] == ["sector", "industry"]
        assert report["AAA"]["created"] is False
        assert report["NEW2"]["applied_fields"] == ["quote_type"]
        assert report["NEW2"]["created"] is True

    @pytest.mark.parametrize(
        "bad",
        [
            "not-a-list",
            {"ticker": "AAA"},
            [{"ticker": "AAA"}, "not-a-dict"],
            [{"sector": "X"}],  # missing ticker
            [{"ticker": "   "}],  # empty ticker
            [{"ticker": 123}],  # non-string ticker
            [{"ticker": "AAA", "sector": 5}],  # non-string field
            [{"ticker": "AAA", "quote_type": True}],
            [{"ticker": "AAA", "unknown_field": "x"}],  # unknown key
        ],
    )
    def test_malformed_overrides_rejected_before_any_write(
        self, exec_engine, exec_db, bad
    ):
        """Malformed payloads raise ValueError AND write nothing — the
        seeded row must be byte-identical afterwards."""
        _seed_metadata(exec_db)
        with pytest.raises(ValueError):
            exec_engine.run_metadata_load(tickers=[], overrides=bad)
        assert _metadata_rows(exec_db)["AAA"] == {
            "company_name": "Stored Name",
            "sector": "Stored Sector",
            "industry": "Stored Industry",
            "quote_type": "EQUITY",
        }

    def test_overrides_applied_independently_of_skip_existing(
        self, exec_engine, exec_db
    ):
        """An override lands even when its ticker is skipped by the
        skip-existing fetch path (refresh=False)."""
        _seed_metadata(exec_db)
        calls: List[str] = []
        with patch(
            "src.ingestion.yf.Ticker",
            side_effect=_ticker_factory({}, calls),
        ), patch("src.ingestion.time.sleep"):
            result = exec_engine.run_metadata_load(
                tickers=["AAA"],
                refresh=False,  # AAA is present -> skipped by fetch
                overrides=[{"ticker": "AAA", "industry": "Fixed"}],
            )
        assert calls == []  # never fetched
        assert result["tickers_skipped"] == 1
        assert result["overrides_applied"] == 1
        row = _metadata_rows(exec_db)["AAA"]
        assert row["industry"] == "Fixed"
        assert row["company_name"] == "Stored Name"
