"""Tests for scripts/classify_tickers_finviz.py (SEC-T2).

All HTTP is mocked via a FakeSession — no live Finviz requests.  Covers
the redesigned-screener parser (SSR ``ssr-island-1`` JSON extraction,
missing/malformed island, missing row keys), totalCount-driven ``&r=``
pagination, the set-based returned-vs-requested cross-check with the
batch_size=10 retry pass, exponential-backoff retries, the
5-consecutive-failure abort, --dry-run (canonical /screener/stocks
URLs), and CSV outputs.
"""

from __future__ import annotations

import html
import json
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


def make_ssr_rows(rows):
    """Build the island ``rows`` list for
    (ticker, company, sector, industry, country) tuples."""
    return [
        {
            "text": {
                "ticker": t,
                "company": c,
                "sector": s,
                "industry": ind,
                "securityType": "Stock",
                "country": cty,
                "www": "",
                "type": "",
            }
        }
        for (t, c, s, ind, cty) in rows
    ]


def make_page(rows, *, total_count=None, first_row=1, page_size=20,
              escape=True):
    """Build synthetic redesigned-screener HTML: an ssr-island-1 script
    tag embedding the props JSON (HTML-escaped, as served)."""
    data = {
        "rows": make_ssr_rows(rows),
        "pageSize": page_size,
        "totalCount": len(rows) if total_count is None else total_count,
        "firstRow": first_row,
    }
    payload = json.dumps({"data": data})
    if escape:
        payload = html.escape(payload, quote=True)
    return (
        "<html><body><div id='app'></div>"
        '<script type="application/json" '
        'data-ssr-props="ssr-island-1">'
        f"{payload}</script></body></html>"
    )


def no_sleep(_seconds):
    return None


# ------------------------------------------------------------------
# Parser — SSR island extraction
# ------------------------------------------------------------------

class TestParseOverviewPage:
    def test_extracts_all_fields(self):
        page = make_page(
            [
                ("AAPL", "Apple Inc", "Technology",
                 "Consumer Electronics", "USA"),
                ("SAP", "SAP SE ADR", "Technology",
                 "Software - Application", "Germany"),
            ]
        )
        recs = cf.parse_overview_page(page)
        assert set(recs) == {"AAPL", "SAP"}
        assert recs["AAPL"] == {
            "company": "Apple Inc",
            "sector": "Technology",
            "industry": "Consumer Electronics",
            "country": "USA",
        }
        assert recs["SAP"]["country"] == "Germany"
        assert recs["SAP"]["industry"] == "Software - Application"

    def test_unescaped_json_island_also_parses(self):
        """The parser unescapes entities, so raw (unescaped) JSON works
        too — be tolerant of either serving form."""
        page = make_page(
            [("MSFT", "Microsoft", "Technology",
              "Software - Infrastructure", "USA")],
            escape=False,
        )
        recs = cf.parse_overview_page(page)
        assert recs["MSFT"]["sector"] == "Technology"

    def test_html_entities_in_values_are_unescaped(self):
        page = make_page(
            [("PG", "Procter &amp; Gamble", "Consumer Defensive",
              "Household Products", "USA")],
            escape=False,
        )
        recs = cf.parse_overview_page(page)
        assert recs["PG"]["company"] == "Procter & Gamble"

    def test_missing_optional_fields_yield_empty_strings(self):
        island = {
            "data": {
                "rows": [{"text": {"ticker": "XYZ", "company": "Xyz Corp"}}],
                "pageSize": 20,
                "totalCount": 1,
                "firstRow": 1,
            }
        }
        page = (
            '<script type="application/json" '
            'data-ssr-props="ssr-island-1">'
            f"{json.dumps(island)}</script>"
        )
        recs = cf.parse_overview_page(page)
        assert recs == {
            "XYZ": {
                "company": "Xyz Corp",
                "sector": "",
                "industry": "",
                "country": "",
            }
        }

    def test_row_without_text_or_ticker_is_skipped(self):
        island = {
            "data": {
                "rows": [
                    {"text": {"ticker": "OK", "sector": "Tech"}},
                    {"notext": True},
                    {"text": {"company": "No ticker"}},
                    {"text": {"ticker": "   "}},
                ],
                "totalCount": 4,
            }
        }
        page = (
            'data-ssr-props="ssr-island-1">'
            f"{json.dumps(island)}</script>"
        )
        recs = cf.parse_overview_page(page)
        assert set(recs) == {"OK"}

    def test_error_page_without_island_returns_empty(self):
        page = "<html><body><h1>403 Forbidden</h1></body></html>"
        assert cf.parse_overview_page(page) == {}

    def test_empty_and_blank_html_return_empty(self):
        assert cf.parse_overview_page("") == {}
        assert cf.parse_overview_page("   \n  ") == {}

    def test_malformed_island_json_returns_empty(self):
        page = (
            '<script type="application/json" '
            'data-ssr-props="ssr-island-1">'
            '{"data": {"rows": [TRUNCATED'
            "</script></body></html>"
        )
        assert cf.parse_overview_page(page) == {}

    def test_island_without_data_or_rows_returns_empty(self):
        for payload in (
            {},
            {"data": {}},
            {"data": {"rows": "not-a-list"}},
            {"data": None},
        ):
            page = (
                'data-ssr-props="ssr-island-1">'
                f"{json.dumps(payload)}</script>"
            )
            assert cf.parse_overview_page(page) == {}

    def test_renamed_island_is_ignored(self):
        """A differently-named island must not be parsed."""
        payload = html.escape(json.dumps({"data": {"rows": [
            {"text": {"ticker": "WRONG"}}
        ]}}), quote=True)
        page = (
            '<script type="application/json" '
            f'data-ssr-props="ssr-island-2">{payload}</script>'
        )
        assert cf.parse_overview_page(page) == {}

    def test_other_scripts_do_not_confuse_extraction(self):
        """Only the ssr-island-1 script is read; neighbours ignored."""
        other = (
            '<script type="application/json" '
            'data-ssr-props="ssr-island-2">'
            f"{json.dumps({'data': {'rows': []}})}</script>"
        )
        page = other + make_page(
            [("MSFT", "Microsoft", "Technology",
              "Software - Infrastructure", "USA")]
        )
        recs = cf.parse_overview_page(page)
        assert set(recs) == {"MSFT"}


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
# Pagination — driven by totalCount / firstRow / pageSize
# ------------------------------------------------------------------

class TestPagination:
    def test_totalcount_over_cap_follows_r_offsets(self):
        """24 valid tickers: page 1 serves 20 rows (totalCount=24), so
        &r=21 is followed for the remaining 4."""
        page1_rows = [
            (f"T{i:02d}", f"Co{i}", "Tech", "Soft", "USA")
            for i in range(20)
        ]
        page2_rows = [
            (f"T{i:02d}", f"Co{i}", "Finance", "Banks", "USA")
            for i in range(20, 24)
        ]

        def handler(url):
            if "&r=21" in url:
                return FakeResponse(200, make_page(
                    page2_rows, total_count=24, first_row=21
                ))
            return FakeResponse(200, make_page(
                page1_rows, total_count=24, first_row=1
            ))

        session = FakeSession(handler=handler)
        recs = cf.fetch_batch_rows(
            session, "https://finviz.com/screener/stocks?v=111&t=X",
            sleep=no_sleep,
        )
        assert len(recs) == 24
        assert recs["T20"]["industry"] == "Banks"
        assert session.calls[1].endswith("&r=21")
        assert len(session.calls) == 2  # consumed 20+4 >= totalCount 24

    def test_exact_cap_batch_does_not_paginate(self):
        """totalCount == pageSize == rows served -> no page 2, even
        though the page is exactly at the 20-row cap."""
        rows = [
            (f"T{i:02d}", f"Co{i}", "Tech", "Soft", "USA")
            for i in range(20)
        ]
        session = FakeSession(
            responses=[FakeResponse(200, make_page(rows, total_count=20))]
        )
        recs = cf.fetch_batch_rows(
            session, "http://x", sleep=no_sleep
        )
        assert len(recs) == 20
        assert len(session.calls) == 1  # totalCount drove the stop

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

    def test_missing_island_stops_pagination(self):
        """Page 1 parses but reports more rows; page 2 loses the island
        -> stop cleanly with what page 1 gave."""
        rows = [
            (f"T{i:02d}", f"Co{i}", "Tech", "Soft", "USA")
            for i in range(20)
        ]

        def handler(url):
            if "&r=21" in url:
                return FakeResponse(200, "<html><body>oops</body></html>")
            return FakeResponse(200, make_page(rows, total_count=40))

        session = FakeSession(handler=handler)
        recs = cf.fetch_batch_rows(session, "http://x", sleep=no_sleep)
        assert len(recs) == 20
        assert len(session.calls) == 2

    def test_missing_totalcount_defaults_to_single_page(self):
        """No totalCount in the island -> parsed rows kept, no page 2."""
        payload = html.escape(json.dumps({"data": {
            "rows": make_ssr_rows([("A", "Ac", "T", "S", "USA")]),
            "pageSize": 20,
            "firstRow": 1,
        }}), quote=True)
        page = (
            'data-ssr-props="ssr-island-1">'
            f"{payload}</script>"
        )
        session = FakeSession(responses=[FakeResponse(200, page)])
        recs = cf.fetch_batch_rows(session, "http://x", sleep=no_sleep)
        assert set(recs) == {"A"}
        assert len(session.calls) == 1


# ------------------------------------------------------------------
# classify_tickers — canonical URL, cross-check, retry pass, abort
# ------------------------------------------------------------------

class TestClassifyTickers:
    def test_requests_use_canonical_screener_stocks_url(self):
        def handler(url):
            assert "screener/stocks" in url
            assert "screener.ashx" not in url
            return FakeResponse(200, make_page(
                [("A", "Ac", "Tech", "Soft", "USA")]
            ))

        session = FakeSession(handler=handler)
        result = cf.classify_tickers(
            ["A"], session=session, batch_size=20, sleep=no_sleep
        )
        assert set(result["records"]) == {"A"}

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

    def test_finviz_sort_order_is_set_based_not_request_order(self):
        """Finviz returns alphabetical order, not request order —
        cross-check must not flag reordered tickers as unclassified."""
        def handler(url):
            # Request order ZETA,ALPHA -> Finviz returns ALPHA,ZETA.
            return FakeResponse(200, make_page(
                [
                    ("ALPHA", "A Co", "Tech", "Soft", "USA"),
                    ("ZETA", "Z Co", "Energy", "Oil", "USA"),
                ]
            ))

        session = FakeSession(handler=handler)
        result = cf.classify_tickers(
            ["ZETA", "ALPHA"],
            session=session,
            batch_size=20,
            sleep=no_sleep,
        )
        assert result["unclassified"] == []
        assert set(result["records"]) == {"ZETA", "ALPHA"}
        # No retry pass needed: only the initial batch was fetched.
        assert len(session.calls) == 1

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
        assert set(result["records"]) == {"A", "B", "C", "D"}

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
        assert "screener/stocks?v=111&t=AAPL,MSFT" in out
        assert "screener.ashx" not in out
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
