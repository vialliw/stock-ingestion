"""On-demand composite data-health report via the Flight server.

Sends the ``data_health`` Flight action to the running server and
prints a formatted report covering the daily_close checks (staleness
vs the latest NYSE trading day, coverage vs the configured ticker
list, OHLC integrity, duplicate (Ticker, Date) keys, and per-ticker
missing trading sessions inside a trailing window) plus the F17-T1
sections: fear_greed_history (presence/staleness/integrity/series),
the weekly layer (staleness/parity/stale tickers/integrity) and
company_metadata (coverage/NULL-field rows).

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


def _print_fear_greed(fg) -> None:
    """Print the ``fear_greed_history`` section (F17-T1)."""
    if not isinstance(fg, dict):
        return
    status = fg.get("health_status", "?")
    print(f"\n[fear_greed] health_status={status}   "
          f"rows={fg.get('row_count', '?')}")
    if status == "DISABLED":
        print(f"    {fg.get('reason', 'disabled')}")
        return
    if status == "CHECK_FAILED":
        print(f"    check failed: {fg.get('error', 'unknown error')}")
        return
    checks = fg.get("checks", {})
    st = checks.get("staleness", {})
    print(f"    staleness: latest_date={st.get('latest_date')} "
          f"expected={st.get('expected_date')} "
          f"stale={st.get('stale')} "
          f"days_behind={st.get('days_behind')}")
    integ = checks.get("integrity", {})
    print(f"    integrity: null_fg_score="
          f"{integ.get('null_fg_score_count', 0)} "
          f"out_of_range={integ.get('out_of_range_count', 0)} "
          f"invalid_rating={integ.get('invalid_rating_count', 0)}")
    for s in integ.get("out_of_range_samples", []):
        print(f"        out_of_range {s}")
    for s in integ.get("invalid_rating_samples", []):
        print(f"        invalid_rating {s}")
    ser = checks.get("series", {})
    print(f"    series: {_fmt_tickers(ser.get('missing_series', []))} "
          f"missing of {ser.get('tracked', '?')} tracked")


def _print_weekly(wk) -> None:
    """Print the weekly_close/weekly_indicators section (F17-T1)."""
    if not isinstance(wk, dict):
        return
    status = wk.get("health_status", "?")
    print(f"\n[weekly]     health_status={status}")
    if status == "CHECK_FAILED":
        print(f"    check failed: {wk.get('error', 'unknown error')}")
        return
    checks = wk.get("checks", {})
    st = checks.get("staleness", {})
    print(f"    staleness: latest_week={st.get('latest_week')} "
          f"expected_week={st.get('expected_week')} "
          f"stale={st.get('stale')}")
    par = checks.get("parity", {})
    print(f"    parity: weekly_close={par.get('weekly_close_rows', '?')} "
          f"weekly_indicators={par.get('weekly_indicators_rows', '?')} "
          f"missing_indicator_keys="
          f"{par.get('missing_indicator_keys', 0)}")
    for k in par.get("missing_keys", []):
        print(f"        missing {k}")
    stale = checks.get("stale_tickers", {})
    print(f"    stale tickers (in weekly, absent daily): "
          f"{_fmt_tickers(stale.get('tickers', []))} "
          f"({stale.get('count', 0)} total)")
    integ = checks.get("integrity", {})
    print(f"    integrity: null_ohlc={integ.get('null_ohlc_count', 0)} "
          f"negative_volume={integ.get('negative_volume_count', 0)}")


def _print_metadata(md) -> None:
    """Print the ``company_metadata`` section (F17-T1)."""
    if not isinstance(md, dict):
        return
    status = md.get("health_status", "?")
    print(f"\n[metadata]   health_status={status}")
    if status == "CHECK_FAILED":
        print(f"    check failed: {md.get('error', 'unknown error')}")
        return
    checks = md.get("checks", {})
    cov = checks.get("coverage", {})
    print(f"    coverage: {cov.get('missing_count', 0)} of "
          f"{cov.get('configured_count', '?')} configured tickers absent")
    for t in cov.get("missing_tickers", []):
        print(f"        {t}")
    nf = checks.get("null_fields", {})
    print(f"    null-field rows (informational): "
          f"{nf.get('null_field_rows', 0)} of "
          f"{nf.get('row_count', '?')}")


def _print_report(h: dict) -> None:
    """Human-readable rendering of the composite data_health report."""
    status = h.get("health_status", "?")
    # The daily section mirrors the daily report with its own status
    # (F17-T1); fall back to the top level for pre-F17 servers.
    daily = h.get("daily_close")
    if not isinstance(daily, dict):
        daily = h
    dstatus = daily.get("health_status", status)
    mode = daily.get("mode", h.get("mode", "?"))
    print("=" * 62)
    print(f" DATA HEALTH — {status}  (daily_close mode: {mode})")
    print(f" as_of: {daily.get('as_of')}   latest_trading_day: "
          f"{daily.get('latest_trading_day')}")
    pending = daily.get("pending_session")
    if pending:
        print(f" pending session: {pending} — still open/within grace, "
              f"not yet expected")
    print("=" * 62)
    if dstatus == "CHECK_FAILED":
        print(f" The daily_close check itself failed: "
              f"{daily.get('error', 'unknown error')}")
    else:
        _print_daily_checks(daily)
    _print_fear_greed(h.get("fear_greed"))
    _print_weekly(h.get("weekly"))
    _print_metadata(h.get("company_metadata"))
    print()


def _print_daily_checks(h: dict) -> None:
    """Print the daily_close check detail lines."""
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


def main() -> int:
    parser = argparse.ArgumentParser(
        description="On-demand composite data-health report "
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
