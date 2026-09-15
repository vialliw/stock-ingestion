"""On-demand daily_close data-health report via the Flight server.

Sends the ``data_health`` Flight action to the running server and
prints a formatted report covering five checks: staleness vs the
latest NYSE trading day, coverage vs the configured ticker list,
OHLC integrity, duplicate (Ticker, Date) keys, and per-ticker
missing trading sessions inside a trailing window.

Usage::

    python scripts\\data_health_report.py [--gap-days N] [--json]

Exit codes: 0 = OK, 1 = WARN / CHECK_FAILED / server unreachable.
The check is report-only — it never writes to the database.
"""
from __future__ import annotations

import argparse
import json
import os
import sys

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)
os.chdir(_REPO_ROOT)

_CONFIG_PATH = os.path.join(_REPO_ROOT, "src", "config.yaml")

import pyarrow.flight as flight  # noqa: E402

from src.config import PipelineConfig  # noqa: E402


def _fetch_health(config: PipelineConfig, gap_days: int) -> dict:
    """Call the ``data_health`` action and return the health dict."""
    client = flight.FlightClient(
        f"grpc://{config.server.flight_host}:{config.server.flight_port}"
    )
    client.wait_for_available(timeout=10)
    action = flight.Action(
        "data_health", json.dumps({"gap_days": gap_days}).encode("utf-8")
    )
    results = list(
        client.do_action(action, options=flight.FlightCallOptions(timeout=120))
    )
    payload = json.loads(results[0].body.to_pybytes().decode("utf-8"))
    return payload["health"]


def _fmt_tickers(items, key="ticker", extra=None) -> str:
    """Render a bounded ticker list, e.g. ``PTLE (last 2026-09-11)``."""
    if not items:
        return "none"
    parts = []
    for it in items:
        if isinstance(it, dict):
            label = str(it.get(key, "?"))
            if extra and it.get(extra) is not None:
                label += f" ({extra}={it[extra]})"
            parts.append(label)
        else:
            parts.append(str(it))
    return ", ".join(parts)


def _print_report(h: dict) -> None:
    """Human-readable rendering of the data_health report."""
    status = h.get("health_status", "?")
    mode = h.get("mode", "?")
    print("=" * 62)
    print(f" daily_close DATA HEALTH — {status}  (mode: {mode})")
    print(f" as_of: {h.get('as_of')}   latest_trading_day: "
          f"{h.get('latest_trading_day')}")
    print("=" * 62)
    if status == "CHECK_FAILED":
        print(f" The check itself failed: {h.get('error', 'unknown error')}")
        return

    checks = h.get("checks", {})

    st = checks.get("staleness", {})
    print(f"\n[staleness]  {st.get('stale_count', 0)} ticker(s) behind "
          f"the latest trading day")
    for t in st.get("stale_tickers", []):
        print(f"    {t.get('ticker')}: last data {t.get('last_date')}")

    cov = checks.get("coverage", {})
    print(f"\n[coverage]   {cov.get('missing_count', 0)} of "
          f"{cov.get('configured_count', '?')} configured tickers absent")
    for t in cov.get("missing_tickers", []):
        print(f"    {t}")

    integ = checks.get("integrity", {})
    rc = integ.get("reason_counts", {})
    print(f"\n[integrity]  {integ.get('violation_count', 0)} bad row(s): "
          f"null/non-positive OHLC={rc.get('null_or_nonpositive_ohlc', 0)}, "
          f"High<Low={rc.get('high_lt_low', 0)}, "
          f"negative Volume={rc.get('negative_volume', 0)}")
    for v in integ.get("violations", []):
        print(f"    {v}")

    dup = checks.get("duplicates", {})
    print(f"\n[duplicates] {dup.get('duplicate_count', 0)} duplicate "
          f"(Ticker, Date) key(s)")
    for d in dup.get("duplicates", []):
        print(f"    {d}")

    gaps = checks.get("gaps")
    if gaps is not None:
        print(f"\n[gaps]       {gaps.get('tickers_with_gaps', 0)} ticker(s) "
              f"with missing sessions, {gaps.get('total_missing_days', 0)} "
              f"day(s) total — window {gaps.get('window_start')} .. "
              f"{gaps.get('window_end')} "
              f"({gaps.get('expected_sessions')} sessions)")
        for t in gaps.get("per_ticker", []):
            days = ", ".join(t.get("missing_days", []))
            print(f"    {t.get('ticker')}: "
                  f"{t.get('missing_count')} missing — {days}")
    print()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="On-demand daily_close data-health report "
                    "via the running Flight server."
    )
    parser.add_argument(
        "--gap-days", type=int, default=30, metavar="N",
        help="trailing sessions scanned for per-ticker gaps (default 30)",
    )
    parser.add_argument(
        "--json", action="store_true",
        help="print the raw JSON report instead of the formatted view",
    )
    args = parser.parse_args()

    config = PipelineConfig.from_yaml(_CONFIG_PATH)
    try:
        health = _fetch_health(config, args.gap_days)
    except Exception as e:
        print(f"ERROR: could not reach the Flight server at "
              f"grpc://{config.server.flight_host}:"
              f"{config.server.flight_port} — is the "
              f"StockIngestion-FlightServer task running?\n{e}")
        return 1

    if args.json:
        print(json.dumps(health, indent=1))
    else:
        _print_report(health)
    return 0 if health.get("health_status") == "OK" else 1


if __name__ == "__main__":
    sys.exit(main())
