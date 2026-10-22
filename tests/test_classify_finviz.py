"""Tests for scripts/classify_tickers_finviz.py (SEC-T1).

All HTTP is mocked via a FakeSession — no live Finviz requests.  Covers
the v=111 overview parser (synthetic HTML, missing columns, empty and
error pages), the returned-vs-requested cross-check with the
batch_size=10 retry pass, the &r= pagination guard, exponential-backoff
retries, the 5-consecutive-failure abort, --dry-run, and CSV outputs.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pandas as pd
import pytest

# Add scripts dir to path (same pattern as tests/test_incident_watch.py).
SCRIPTS_DIR = Path(__file__).parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

import classify_tickers_finviz as cf  # noqa: E402
import requests  # noqa: E402


# ------------------------------------------------------------------
# Fakes / fixtures
# ------------------------------------------------------------------

class FakeResponse:
    def __init__(self, status_code: int = 200, text: str = ""):
        self.status_code = status_code
        self.text = text
        self.headers = {}


class FakeSession:
    """Queue-driven fake for requests.Session.get.

    ``responses`` are consumed in order; ``handler(url)`` takes over
    when given.  Every call URL is recorded in ``self.calls``.
    """

    def __init__(self, responses=None, handler=None):
        self.responses = list(responses or [])
        self.handler = handler
        self.calls = []

    def get(self, url, headers=None, timeout=None):
        self.calls.append(url)
        if self.handler is not None:
            return self.handler(url)
        if not self.responses:
            raise AssertionError(f"FakeSession: unexpected GET {url}")
        return self.responses.pop(0)


def make_page(rows):
    """Build synthetic v=111 overview HTML for rows of
    (ticker, company, sector, industry, country)."""
    trs = "".join(
        f"<tr><td>{i}</td><td>{t}</td><td>{c}</td><td>{s}</td>"
        f"<td>{ind}</td><td>{cty}</td><td>1B</td><td>10.5</td>"
        f"<td>100.00</td><td>1.0%</td><td>1000</td></tr>"
        for i, (t, c, s, ind, cty) in enumerate(rows, start=1)
    )
    return (
        "<html><body><table class='screener'>"
        "<tr><th>No.</th><th>Ticker</th><th>Company</th>"
        "<th>Sector</th><th>Industry</th><th>Country</th>"
        "<th>Market Cap</th><th>P/E</th><th>Price</th>"
        "<th>Change</th><th>Volume</th></tr>"
        f"{trs}</table></body></html>"
    )


def no_sleep(_seconds):
    return None


# ------------------------------------------------------------------
# Parser
# ------------------------------------------------------------------

class TestParseOverviewPage:
    def test_extracts_all_fields(self):
        html = make_page(
            [
                ("AAPL", "Apple Inc", "Technology",
                 "Consumer Electronics", "USA"),
                ("SAP", "SAP SE ADR", "Technology",
                 "Software - Application", "Germany"),
            ]
        )
        recs = cf.parse_overview_page(html)
        assert set(recs) == {"AAPL", "SAP"}
        assert recs["AAPL"] == {
            "company": "Apple Inc",
            "sector": "Technology",
            "industry": "Consumer Electronics",
            "country": "USA",
        }
        assert recs["SAP"]["country"] == "Germany"
        assert recs["SAP"]["industry"] == "Software - Application"

    def test_missing_optional_columns_yield_empty_strings(self):
        html = (
            "<html><body><table>"
            "<tr><th>No.</th><th>Ticker</th><th>Company</th></tr>"
            "<tr><td>1</td><td>XYZ</td><td>Xyz Corp</td></tr>"
            "</table></body></html>"
        )
        recs = cf.parse_overview_page(html)
        assert recs == {
            "XYZ": {
                "company": "Xyz Corp",
                "sector": "",
                "industry": "",
                "country": "",
            }
        }

    def test_error_page_returns_empty(self):
        html = "<html><body><h1>403 Forbidden</h1></body></html>"
        assert cf.parse_overview_page(html) == {}

    def test_empty_and_blank_html_return_empty(self):
        assert cf.parse_overview_page("") == {}
        assert cf.parse_overview_page("   \n  ") == {}

    def test_table_without_ticker_column_returns_empty(self):
        html = (
            "<html><body><table>"
            "<tr><th>One</th><th>Two</th></tr>"
            "<tr><td>a</td><td>b</td></tr>"
            "</table></body></html>"
        )
        assert cf.parse_overview_page(html) == {}

    def test_picks_overview_table_among_layout_tables(self):
        html = (
            "<html><body>"
            "<table><tr><th>Filter</th><th>Value</th></tr>"
            "<tr><td>x</td><td>y</td></tr></table>"
            + make_page([("MSFT", "Microsoft", "Technology",
                          "Software - Infrastructure", "USA")])
            + "</body></html>"
        )
        recs = cf.parse_overview_page(html)
        assert recs["MSFT"]["sector"] == "Technology"


# ------------------------------------------------------------------
# Retry / backoff
# ------------------------------------------------------------------

class TestRequestWithRetries:
    def test_retries_on_429_then_succeeds_with_exponential_backoff(self):
        session = FakeSession(
            responses=[
                FakeResponse(429, "rate limited"),
                FakeResponse(429, "rate limited"),
                FakeResponse(200, make_page(
                    [("AAPL", "Apple", "Technology", "CE", "USA")]
                )),
            ]
        )
        delays = []
        resp = cf._request_with_retries(
            session, "http://x", sleep=delays.append, backoff_base=1.0
        )
        assert resp.status_code == 200
        assert len(session.calls) == 3
        assert delays == [1.0, 2.0]  # exponential: base * 2**attempt

    def test_retryable_5xx_and_403(self):
        session = FakeSession(
            responses=[
                FakeResponse(503, "down"),
                FakeResponse(403, "blocked"),
                FakeResponse(200, "ok"),
            ]
        )
        resp = cf._request_with_retries(
            session, "http://x", sleep=no_sleep, backoff_base=0.0
        )
        assert resp.status_code == 200
        assert len(session.calls) == 3

    def test_exhausts_retries_raises_fetch_error(self):
        session = FakeSession(
            responses=[FakeResponse(500, "err")] * 4
        )
        with pytest.raises(cf.FetchError, match="500"):
            cf._request_with_retries(
                session, "http://x", sleep=no_sleep, max_retries=3
            )
        assert len(session.calls) == 4  # 1 initial + 3 retries

    def test_transport_exception_is_retried(self):
        calls = {"n": 0}

        def handler(url):
            calls["n"] += 1
            if calls["n"] == 1:
                raise requests.ConnectionError("reset by peer")
            return FakeResponse(200, "ok")

        session = FakeSession(handler=handler)
        resp = cf._request_with_retries(
            session, "http://x", sleep=no_sleep, max_retries=3
        )
        assert resp.status_code == 200
        assert calls["n"] == 2

    def test_non_retryable_404_returns_immediately(self):
        session = FakeSession(responses=[FakeResponse(404, "nope")])
        resp = cf._request_with_retries(
            session, "http://x", sleep=no_sleep, max_retries=3
        )
        assert resp.status_code == 404
        assert len(session.calls) == 1


# ------------------------------------------------------------------
# Pagination
# ------------------------------------------------------------------

class TestPagination:
    def test_full_page_follows_r_offsets(self):
        full_rows = [
            (f"T{i:02d}", f"Co{i}", "Tech", "Soft", "USA")
            for i in range(cf.ROW_CAP)
        ]
        page2_rows = [
            ("EXTRA1", "E1", "Finance", "Banks", "USA"),
            ("EXTRA2", "E2", "Energy", "Oil", "USA"),
        ]

        def handler(url):
            if "&r=21" in url:
                return FakeResponse(200, make_page(page2_rows))
            return FakeResponse(200, make_page(full_rows))

        session = FakeSession(handler=handler)
        recs = cf.fetch_batch_rows(
            session, "https://finviz.com/screener.ashx?v=111&t=X",
            sleep=no_sleep,
        )
        assert len(recs) == cf.ROW_CAP + 2
        assert recs["EXTRA1"]["industry"] == "Banks"
        assert session.calls[1].endswith("&r=21")
        assert len(session.calls) == 2  # stopped: page 2 below the cap

    def test_below_cap_page_does_not_paginate(self):
        session = FakeSession(
            responses=[
                FakeResponse(200, make_page(
                    [("A", "Ac", "Tech", "Soft", "USA")]
                ))
            ]
        )
        recs = cf.fetch_batch_rows(
            session, "http://x", sleep=no_sleep
        )
        assert set(recs) == {"A"}
        assert len(session.calls) == 1


# ------------------------------------------------------------------
# classify_tickers — cross-check, retry pass, abort
# ------------------------------------------------------------------

class TestClassifyTickers:
    def test_cross_check_retries_unclassified_at_size_10(self):
        """Finviz silently drops POISON from the 3-ticker batch; the
        retry pass at batch_size=10 fetches it alone and classifies it."""
        rows_by_param = {
            "GOOD1,POISON,GOOD2": [
                ("GOOD1", "G1", "Tech", "Soft", "USA"),
                ("GOOD2", "G2", "Finance", "Banks", "USA"),
            ],
            "POISON": [
                ("POISON", "P Inc", "Energy", "Oil", "USA"),
            ],
        }

        def handler(url):
            t_param = url.split("t=", 1)[1].split("&", 1)[0]
            return FakeResponse(200, make_page(rows_by_param[t_param]))

        session = FakeSession(handler=handler)
        result = cf.classify_tickers(
            ["GOOD1", "POISON", "GOOD2"],
            session=session,
            batch_size=3,
            retry_batch_size=10,
            sleep=no_sleep,
        )
        assert result["unclassified"] == []
        assert set(result["records"]) == {"GOOD1", "POISON", "GOOD2"}
        assert result["records"]["POISON"]["sector"] == "Energy"
        # Two batches total: the 3-ticker pass + 1-ticker retry pass.
        assert result["batches_total"] == 2
        assert "t=POISON" in session.calls[-1]
        assert "GOOD1" not in session.calls[-1]

    def test_permanently_unclassified_ticker_is_named(self):
        def handler(url):
            return FakeResponse(200, make_page(
                [("REAL", "R", "Tech", "Soft", "USA")]
            ))

        session = FakeSession(handler=handler)
        result = cf.classify_tickers(
            ["REAL", "GHOST"],
            session=session,
            batch_size=5,
            sleep=no_sleep,
        )
        assert result["unclassified"] == ["GHOST"]
        assert set(result["records"]) == {"REAL"}
        # Initial batch + one retry pass for GHOST alone.
        assert session.calls[-1].endswith("t=GHOST")

    def test_abort_after_consecutive_failures(self):
        def handler(url):
            return FakeResponse(500, "down")

        session = FakeSession(handler=handler)
        tickers = [f"T{i}" for i in range(12)]
        result = cf.classify_tickers(
            tickers,
            session=session,
            batch_size=2,        # 6 batches
            max_retries=0,
            max_consecutive_failures=5,
            sleep=no_sleep,
        )
        assert result["aborted"] is True
        assert result["batches_attempted"] == 5
        assert result["batches_total"] == 6
        assert len(session.calls) == 5
        # All 12 requested tickers surface as unclassified — not dropped.
        assert len(result["unclassified"]) == 12

    def test_failure_counter_resets_on_success(self):
        """One failing batch between successes does not abort."""
        state = {"fail_next": True}

        def handler(url):
            if state["fail_next"]:
                state["fail_next"] = False
                return FakeResponse(500, "err")
            t_param = url.split("t=", 1)[1].split("&", 1)[0]
            rows = [(t, f"C{t}", "Tech", "Soft", "USA")
                    for t in t_param.split(",")]
            return FakeResponse(200, make_page(rows))

        session = FakeSession(handler=handler)
        result = cf.classify_tickers(
            ["A", "B", "C", "D"],
            session=session,
            batch_size=2,
            max_retries=0,
            sleep=no_sleep,
        )
        assert result["aborted"] is False
        assert result["unclassified"] == ["A", "B"] or set(
            result["records"]
        ) == {"A", "B", "C", "D"}

    def test_pacing_sleeps_between_batches(self):
        def handler(url):
            return FakeResponse(200, "<html><body></body></html>")

        session = FakeSession(handler=handler)
        delays = []
        cf.classify_tickers(
            ["A", "B", "C", "D"],
            session=session,
            batch_size=2,  # 2 batches -> 1 inter-batch sleep
            delay=1.2,
            sleep=delays.append,
        )
        assert len(delays) == 1
        assert 1.2 <= delays[0] <= 1.2 + 1.2 * 0.25  # delay + jitter


# ------------------------------------------------------------------
# Output reports
# ------------------------------------------------------------------

class TestWriteReports:
    def test_detail_and_summary_csvs(self, tmp_path):
        records = {
            "AAPL": {"company": "Apple", "sector": "Technology",
                     "industry": "Consumer Electronics", "country": "USA"},
            "MSFT": {"company": "Microsoft", "sector": "Technology",
                     "industry": "Software - Infrastructure",
                     "country": "USA"},
            "JPM": {"company": "JPMorgan", "sector": "Financial",
                    "industry": "Banks - Diversified", "country": "USA"},
        }
        detail_path, summary_path = cf.write_reports(
            ["AAPL", "MSFT", "JPM", "GHOST"],
            records,
            str(tmp_path),
            "20260101",
        )
        detail = pd.read_csv(detail_path)
        assert list(detail.columns) == [
            "ticker", "company", "sector", "industry", "country"
        ]
        assert len(detail) == 4  # unclassified kept, not dropped
        aapl = detail.set_index("ticker").loc["AAPL"]
        assert aapl["sector"] == "Technology"
        ghost = detail.set_index("ticker").loc["GHOST"]
        assert pd.isna(ghost["sector"]) or ghost["sector"] == ""

        summary = pd.read_csv(summary_path).fillna("")
        sec = summary[summary["level"] == "sector"].set_index("sector")
        assert sec.loc["Technology", "ticker_count"] == 2
        assert sec.loc["Financial", "ticker_count"] == 1
        assert sec.loc["(unclassified)", "ticker_count"] == 1
        ind = summary[summary["level"] == "industry"]
        row = ind[
            (ind["sector"] == "Technology")
            & (ind["industry"] == "Software - Infrastructure")
        ]
        assert row["ticker_count"].iloc[0] == 1

        assert os.path.basename(detail_path) == (
            "finviz_sector_industry_20260101.csv"
        )
        assert os.path.basename(summary_path) == (
            "finviz_sector_summary_20260101.csv"
        )


# ------------------------------------------------------------------
# --dry-run / resolve_tickers
# ------------------------------------------------------------------

class TestDryRunAndResolve:
    def test_dry_run_prints_urls_no_http(self, monkeypatch, capsys):
        monkeypatch.setattr(
            cf.requests,
            "Session",
            lambda *a, **k: (_ for _ in ()).throw(
                AssertionError("HTTP session created during dry-run")
            ),
        )
        rc = cf.main(["--dry-run", "--tickers", "AAPL,MSFT"])
        assert rc == 0
        out = capsys.readouterr().out
        assert "screener.ashx?v=111&t=AAPL,MSFT" in out
        assert "DRY-RUN" in out

    def test_tickers_arg_subset(self):
        cfg = cf.PipelineConfig()
        tickers, excluded, source = cf.resolve_tickers(
            cfg, "aapl, msft ,,BRK-B"
        )
        assert tickers == ["AAPL", "MSFT", "BRK-B"]
        assert excluded == 0
        assert source == "--tickers"

    def test_flight_unreachable_falls_back_to_config(self, monkeypatch):
        def boom(location, sql):
            raise RuntimeError("connection refused")

        monkeypatch.setattr(cf, "_flight_do_get", boom)
        cfg = cf.PipelineConfig()
        object.__setattr__(cfg.ingestion, "tickers", ["AAA", "BBB"])
        tickers, excluded, source = cf.resolve_tickers(cfg, None)
        assert tickers == ["AAA", "BBB"]
        assert excluded == 0
        assert source == "config-fallback"

    def test_flight_path_counts_excluded_non_equity(self, monkeypatch):
        def fake_get(location, sql):
            if "quote_type" in sql:
                return pd.DataFrame({"ticker": ["AAPL", "MSFT"]})
            return pd.DataFrame({"n": [5]})

        monkeypatch.setattr(cf, "_flight_do_get", fake_get)
        cfg = cf.PipelineConfig()
        tickers, excluded, source = cf.resolve_tickers(cfg, None)
        assert tickers == ["AAPL", "MSFT"]
        assert excluded == 3
        assert source == "flight"
