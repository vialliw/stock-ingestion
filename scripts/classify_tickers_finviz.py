"""Classify all non-ETF tickers into the Finviz sector/industry taxonomy.

SEC-T1: scrapes the Finviz screener *overview* view (``v=111``) in
batches built by ``scripts/split_tickers_for_finviz.py`` and writes two
CSV reports to ``reports.report_dir`` (``C:\\Intel\\parquet``):

* ``finviz_sector_industry_YYYYMMDD.csv`` — per-ticker detail rows
  ``ticker,company,sector,industry,country``.  Requested tickers that
  Finviz never classifies are kept in the file with empty
  classification fields AND named in the stdout digest — nothing is
  silently dropped.
* ``finviz_sector_summary_YYYYMMDD.csv`` — rollup counts: one
  ``level='sector'`` row per sector plus one ``level='industry'`` row
  per sector -> industry pair.

Ticker universe (default): ``SELECT ticker FROM company_metadata WHERE
quote_type = 'EQUITY' ORDER BY ticker`` via the Arrow Flight service's
``do_get`` JSON ``{"sql": ...}`` ticket — the same contract the other
read scripts use.  When the server is unreachable the configured
``ingestion.tickers`` list is used as-is; ``--tickers A,B,C`` forces an
explicit subset.

HTTP behaviour: browser User-Agent, ~1.2 s + jitter between batches,
30 s timeout, exponential-backoff retry on HTTP 429/403/5xx (max 3
retries), clean abort after 5 consecutive batch failures.  Finviz
silently drops unknown tickers, so every batch's returned rows are
cross-checked against the requested list and the leftovers are retried
once at ``batch_size=10``.  The ``v=111`` overview paginates at 20
rows/page — when a page comes back exactly at the row cap the
``&r=21,41,...`` pages are followed defensively.

Usage:
    python scripts/classify_tickers_finviz.py                      # all equities
    python scripts/classify_tickers_finviz.py --tickers AAPL,MSFT  # subset
    python scripts/classify_tickers_finviz.py --dry-run            # URLs only, no HTTP
    python scripts/classify_tickers_finviz.py --delay 2.0          # slower pacing
"""

from __future__ import annotations

import argparse
import io
import json
import os
import random
import sys
import time
from datetime import datetime
from typing import Callable, Dict, List, Optional

import pandas as pd
import requests

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_CONFIG_PATH = os.path.join(_REPO_ROOT, "src", "config.yaml")
sys.path.insert(0, _REPO_ROOT)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.config import PipelineConfig  # noqa: E402
from split_tickers_for_finviz import split_tickers_for_finviz  # noqa: E402

EQUITY_TICKERS_SQL = (
    "SELECT ticker FROM company_metadata "
    "WHERE quote_type = 'EQUITY' ORDER BY ticker"
)
METADATA_COUNT_SQL = "SELECT COUNT(*) AS n FROM company_metadata"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

# v=111 overview shows at most 20 rows per page; a page that comes back
# exactly at the cap may have more rows behind &r=21,41,...
ROW_CAP = 20

REQUIRED_COLUMN = "Ticker"
DETAIL_COLUMNS = ["company", "sector", "industry", "country"]
# Finviz header -> output field name.
_OVERVIEW_FIELD_MAP = {
    "Ticker": "ticker",
    "Company": "company",
    "Sector": "sector",
    "Industry": "industry",
    "Country": "country",
}

DEFAULT_BATCH_SIZE = 20
RETRY_BATCH_SIZE = 10
DEFAULT_DELAY_S = 1.2
DEFAULT_TIMEOUT_S = 30.0
DEFAULT_MAX_RETRIES = 3
DEFAULT_BACKOFF_BASE_S = 1.0
MAX_CONSECUTIVE_FAILURES = 5
FLIGHT_CONNECT_TIMEOUT_S = 5.0
FLIGHT_GET_TIMEOUT_S = 30.0
RETRYABLE_STATUSES = {429, 403}


class FetchError(RuntimeError):
    """Raised when a Finviz GET fails after exhausting all retries."""


def _is_retryable_status(status: int) -> bool:
    """429/403/5xx are retryable; other 4xx are permanent."""
    return status in RETRYABLE_STATUSES or status >= 500


def _request_with_retries(
    session: requests.Session,
    url: str,
    *,
    timeout: float = DEFAULT_TIMEOUT_S,
    max_retries: int = DEFAULT_MAX_RETRIES,
    backoff_base: float = DEFAULT_BACKOFF_BASE_S,
    sleep: Callable[[float], None] = time.sleep,
) -> requests.Response:
    """GET ``url`` with exponential-backoff retries.

    One initial attempt plus up to ``max_retries`` retries; waits
    ``backoff_base * 2**attempt`` seconds before retry ``attempt+1``.
    Retries on HTTP 429/403/5xx and on transport-level
    ``requests.RequestException`` (timeout, connection reset).  Raises
    ``FetchError`` when every attempt fails.
    """
    last_error: Optional[BaseException] = None
    last_status: Optional[int] = None
    for attempt in range(max_retries + 1):  # range() exempt; bounded retry counter
        try:
            resp = session.get(
                url, headers={"User-Agent": USER_AGENT}, timeout=timeout
            )
        except requests.RequestException as exc:
            last_error = exc
        else:
            if not _is_retryable_status(resp.status_code):
                return resp
            last_error = None
            last_status = resp.status_code
        if attempt < max_retries:
            sleep(backoff_base * (2 ** attempt))
    if last_error is not None:
        raise FetchError(f"GET {url} failed after retries: {last_error}")
    raise FetchError(
        f"GET {url} returned HTTP {last_status} after "
        f"{max_retries + 1} attempts"
    )


def parse_overview_page(html: str) -> Dict[str, dict]:
    """Parse one Finviz ``v=111`` overview page.

    Returns ``{ticker: {company, sector, industry, country}}`` for every
    data row of the overview table.  Pages with no usable table — error
    pages, empty bodies, or a table missing the ``Ticker`` column —
    return ``{}`` so the caller's cross-check names the requested
    tickers as unclassified instead of crashing.  Missing *optional*
    columns (Company/Sector/Industry/Country) yield empty strings.
    """
    if not html or not html.strip():
        return {}
    try:
        tables = pd.read_html(io.StringIO(html))
    except (ValueError, ImportError):
        # No <table> at all (Finviz error/rate-limit page), or a missing
        # HTML parser backend.
        return {}
    best = None
    best_overlap = -1
    # justified: inspect each parsed table's header to find the overview
    # table; page table count is tiny (~2-4) and selection needs a
    # per-table column intersection.
    for table in tables:
        cols = [str(c).strip() for c in table.columns]
        if REQUIRED_COLUMN not in cols:
            continue
        overlap = sum(1 for c in cols if c in _OVERVIEW_FIELD_MAP)
        if overlap > best_overlap:
            best = (table, cols)
            best_overlap = overlap
    if best is None:
        return {}
    table, cols = best
    table = table.copy()
    table.columns = cols
    records: Dict[str, dict] = {}
    rows = table.to_dict("records")
    # justified: per-row dict shaping of the parsed HTML table; row
    # count is bounded by ROW_CAP (20) per page and needs per-row Ticker
    # normalization + first-wins dedup.
    for row in rows:
        raw = row.get(REQUIRED_COLUMN)
        if raw is None or (isinstance(raw, float) and pd.isna(raw)):
            continue
        ticker = str(raw).strip().upper()
        if not ticker or ticker == "NAN":
            continue
        if ticker in records:
            continue
        rec = {}
        for finviz_col, field in _OVERVIEW_FIELD_MAP.items():  # .items() exempt; fixed 5-entry column map
            if field == "ticker":
                continue
            val = row.get(finviz_col)
            rec[field] = (
                "" if val is None or pd.isna(val) else str(val).strip()
            )
        records[ticker] = rec
    return records


def fetch_batch_rows(
    session: requests.Session,
    url: str,
    *,
    timeout: float = DEFAULT_TIMEOUT_S,
    max_retries: int = DEFAULT_MAX_RETRIES,
    backoff_base: float = DEFAULT_BACKOFF_BASE_S,
    sleep: Callable[[float], None] = time.sleep,
) -> Dict[str, dict]:
    """Fetch all overview rows behind a batch URL, following ``&r=``
    pagination when a page returns exactly ``ROW_CAP`` rows."""
    records: Dict[str, dict] = {}
    page_url = url
    offset = 1
    while True:
        resp = _request_with_retries(
            session,
            page_url,
            timeout=timeout,
            max_retries=max_retries,
            backoff_base=backoff_base,
            sleep=sleep,
        )
        page_rows = parse_overview_page(resp.text)
        for ticker, rec in page_rows.items():  # .items() exempt; merging a <=20-row page dict, first wins.
            records.setdefault(ticker, rec)
        if len(page_rows) < ROW_CAP:
            break
        offset += ROW_CAP
        page_url = f"{url}&r={offset}"
    return records


def classify_tickers(
    tickers: List[str],
    *,
    session: Optional[requests.Session] = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
    retry_batch_size: int = RETRY_BATCH_SIZE,
    delay: float = DEFAULT_DELAY_S,
    jitter: float = 0.25,
    timeout: float = DEFAULT_TIMEOUT_S,
    max_retries: int = DEFAULT_MAX_RETRIES,
    backoff_base: float = DEFAULT_BACKOFF_BASE_S,
    max_consecutive_failures: int = MAX_CONSECUTIVE_FAILURES,
    sleep: Callable[[float], None] = time.sleep,
    rng: Optional[random.Random] = None,
) -> dict:
    """Fetch Finviz classification for ``tickers``.

    Returns ``{records, unclassified, batches_total, batches_attempted,
    aborted}``.  ``records`` maps classified tickers to their field
    dict; ``unclassified`` lists every requested ticker Finviz never
    returned — after one retry pass at ``retry_batch_size``.
    """
    if session is None:
        session = requests.Session()
    if rng is None:
        rng = random.Random()

    def _pace() -> None:
        sleep(delay + rng.uniform(0.0, max(0.0, delay * jitter)))

    def _run_pass(todo: List[str], size: int) -> tuple:
        batches = split_tickers_for_finviz(todo, batch_size=size)
        found: Dict[str, dict] = {}
        consecutive_failures = 0
        attempted = 0
        aborted = False
        # justified: sequential I/O-bound HTTP requests — batches must be
        # paced (delay between requests) and the circuit breaker needs
        # per-iteration consecutive-failure tracking; cannot vectorize.
        for i, batch in enumerate(batches):
            if i > 0:
                _pace()
            attempted += 1
            try:
                rows = fetch_batch_rows(
                    session,
                    batch["url"],
                    timeout=timeout,
                    max_retries=max_retries,
                    backoff_base=backoff_base,
                    sleep=sleep,
                )
            except FetchError as exc:
                consecutive_failures += 1
                print(
                    f"  batch {batch['batch_number']} FAILED: {exc} "
                    f"({consecutive_failures} consecutive)",
                    file=sys.stderr,
                )
                if consecutive_failures >= max_consecutive_failures:
                    print(
                        f"Aborting: {consecutive_failures} consecutive "
                        f"batch failures.",
                        file=sys.stderr,
                    )
                    aborted = True
                    break
                continue
            consecutive_failures = 0
            for ticker, rec in rows.items():  # .items() exempt; merging a small (<=batch_size) dict, first wins.
                found.setdefault(ticker.upper(), rec)
        return found, len(batches), attempted, aborted

    records, batches_total, attempted, aborted = _run_pass(
        list(tickers), batch_size
    )
    requested = {t.strip().upper() for t in tickers if t.strip()}
    unclassified = sorted(requested - set(records))

    if unclassified and not aborted:
        print(
            f"Retrying {len(unclassified)} unclassified tickers at "
            f"batch_size={retry_batch_size}: "
            f"{', '.join(unclassified)}"
        )
        retry_found, retry_batches, retry_attempted, retry_aborted = (
            _run_pass(unclassified, retry_batch_size)
        )
        batches_total += retry_batches
        attempted += retry_attempted
        aborted = aborted or retry_aborted
        for ticker, rec in retry_found.items():  # .items() exempt; merging the small retry dict, first wins.
            records.setdefault(ticker, rec)
        unclassified = sorted(requested - set(records))

    return {
        "records": records,
        "unclassified": unclassified,
        "batches_total": batches_total,
        "batches_attempted": attempted,
        "aborted": aborted,
    }


def _flight_do_get(location: str, sql: str) -> pd.DataFrame:
    """Run ``sql`` through the Flight service's ``do_get``.

    Sends a JSON ``{"sql": ...}`` ticket — the same contract the
    server's ``do_get`` implements — after a short
    ``wait_for_available`` probe.  Raises on any failure so the caller
    can fall back to the configured ticker list.
    """
    import pyarrow.flight as flight

    client = flight.FlightClient(location)
    client.wait_for_available(timeout=FLIGHT_CONNECT_TIMEOUT_S)
    ticket = flight.Ticket(json.dumps({"sql": sql}).encode("utf-8"))
    options = flight.FlightCallOptions(timeout=FLIGHT_GET_TIMEOUT_S)
    reader = client.do_get(ticket, options=options)
    return reader.read_all().to_pandas()


def resolve_tickers(
    cfg: PipelineConfig, tickers_arg: Optional[str]
) -> tuple:
    """Resolve the classification universe.

    Returns ``(tickers, excluded_non_equity, source)``.  ``--tickers``
    wins; otherwise the Flight server's ``company_metadata`` EQUITY rows
    (with ``excluded_non_equity`` = table total - equity count); on any
    Flight failure, the configured ``ingestion.tickers`` list (excluded
    count 0 — configured tickers are treated as the equity universe).
    """
    if tickers_arg:
        tickers = [
            t.strip().upper() for t in tickers_arg.split(",") if t.strip()
        ]
        return tickers, 0, "--tickers"

    location = f"grpc://{cfg.server.flight_host}:{cfg.server.flight_port}"
    try:
        equity_df = _flight_do_get(location, EQUITY_TICKERS_SQL)
        total_df = _flight_do_get(location, METADATA_COUNT_SQL)
        tickers = [str(t).strip().upper() for t in equity_df["ticker"]]
        total = int(total_df.iloc[0]["n"])
        return tickers, max(0, total - len(tickers)), "flight"
    except Exception as exc:
        print(
            f"Flight server at {location} unreachable ({exc}); falling "
            f"back to configured ingestion.tickers.",
            file=sys.stderr,
        )
        return list(cfg.ingestion.tickers), 0, "config-fallback"


def write_reports(
    requested: List[str],
    records: Dict[str, dict],
    output_dir: str,
    datestamp: str,
) -> tuple:
    """Write the detail and summary CSVs.  Returns their paths.

    Every requested ticker gets a detail row — unclassified tickers keep
    empty classification fields so nothing is silently dropped.
    """
    os.makedirs(output_dir, exist_ok=True)
    detail_rows = [
        {
            "ticker": t,
            "company": records.get(t, {}).get("company", ""),
            "sector": records.get(t, {}).get("sector", ""),
            "industry": records.get(t, {}).get("industry", ""),
            "country": records.get(t, {}).get("country", ""),
        }
        for t in requested
    ]
    detail_df = pd.DataFrame(
        detail_rows, columns=["ticker", *DETAIL_COLUMNS]
    )
    detail_path = os.path.join(
        output_dir, f"finviz_sector_industry_{datestamp}.csv"
    )
    detail_df.to_csv(detail_path, index=False)

    classified = detail_df[detail_df["sector"] != ""]
    sector_rows = (
        classified.groupby("sector")
        .size()
        .reset_index(name="ticker_count")
        .assign(level="sector", industry="")
    )
    industry_rows = (
        classified.groupby(["sector", "industry"])
        .size()
        .reset_index(name="ticker_count")
        .assign(level="industry")
    )
    summary_df = pd.concat([sector_rows, industry_rows], ignore_index=True)[
        ["level", "sector", "industry", "ticker_count"]
    ]
    unclassified_count = int((detail_df["sector"] == "").sum())
    if unclassified_count:
        summary_df = pd.concat(
            [
                summary_df,
                pd.DataFrame(
                    [
                        {
                            "level": "sector",
                            "sector": "(unclassified)",
                            "industry": "",
                            "ticker_count": unclassified_count,
                        }
                    ]
                ),
            ],
            ignore_index=True,
        )
    summary_df = summary_df.sort_values(
        ["sector", "industry", "level"], kind="stable"
    ).reset_index(drop=True)
    summary_path = os.path.join(
        output_dir, f"finviz_sector_summary_{datestamp}.csv"
    )
    summary_df.to_csv(summary_path, index=False)
    return detail_path, summary_path


def _parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Classify non-ETF tickers into the Finviz sector/industry "
            "taxonomy via batched screener.ashx?v=111 scrapes; writes "
            "detail + summary CSVs to reports.report_dir."
        )
    )
    parser.add_argument(
        "--tickers",
        default=None,
        metavar="A,B,...",
        help=(
            "Explicit comma-separated subset.  Default: all "
            "quote_type='EQUITY' tickers from company_metadata via "
            "Flight (configured tickers as fallback)."
        ),
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        metavar="N",
        help=f"Tickers per Finviz screener URL (default {DEFAULT_BATCH_SIZE}).",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=DEFAULT_DELAY_S,
        metavar="S",
        help=f"Seconds between batch GETs (+jitter; default {DEFAULT_DELAY_S}).",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_TIMEOUT_S,
        metavar="S",
        help=f"Per-request timeout seconds (default {DEFAULT_TIMEOUT_S}).",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=DEFAULT_MAX_RETRIES,
        metavar="N",
        help=f"Max retries per request on 429/403/5xx (default {DEFAULT_MAX_RETRIES}).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the batch URLs without any HTTP requests.",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        metavar="DIR",
        help="Override reports.report_dir for the CSV outputs.",
    )
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = _parse_args(argv)
    cfg = PipelineConfig.from_yaml(_CONFIG_PATH)

    tickers, excluded_non_equity, source = resolve_tickers(
        cfg, args.tickers
    )
    if not tickers:
        print("No tickers to classify.", file=sys.stderr)
        return 1

    if args.dry_run:
        # split_tickers_for_finviz prints every batch's URL; no HTTP.
        split_tickers_for_finviz(tickers, batch_size=args.batch_size)
        print(
            f"DRY-RUN: {len(tickers)} tickers (source={source}); "
            "no requests made."
        )
        return 0

    result = classify_tickers(
        tickers,
        batch_size=args.batch_size,
        delay=args.delay,
        timeout=args.timeout,
        max_retries=args.retries,
    )
    records = result["records"]
    unclassified = result["unclassified"]

    output_dir = args.output_dir or cfg.reports.report_dir
    datestamp = datetime.now().strftime("%Y%m%d")
    detail_path, summary_path = write_reports(
        [t.strip().upper() for t in tickers if t.strip()],
        records,
        output_dir,
        datestamp,
    )

    print("=" * 50)
    print("Finviz classification digest")
    print(f"  ticker source:         {source}")
    print(f"  requested:             {len(tickers)}")
    print(f"  excluded non-equity:   {excluded_non_equity}")
    print(
        f"  batches attempted/run: {result['batches_attempted']}"
        f"/{result['batches_total']}"
    )
    print(f"  classified:            {len(records)}")
    print(f"  unclassified:          {len(unclassified)}")
    if unclassified:
        print(f"  unclassified tickers:  {', '.join(unclassified)}")
    print(f"  aborted early:         {result['aborted']}")
    print(f"  detail CSV:            {detail_path}")
    print(f"  summary CSV:           {summary_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
