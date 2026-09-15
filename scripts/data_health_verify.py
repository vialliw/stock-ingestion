"""Verify flagged data_health gaps against yfinance — halt vs gap.

Runs the ``data_health`` Flight action for the gap report, then probes
each flagged ticker with a single ``yf.download`` covering its missing
range.  Each missing session is classified:

* ``download_gap`` — Yahoo returned a row for that date; our table
  missed it (repairable via ``on_request_load``).
* ``halt_or_suspension`` — Yahoo returned nothing for that date; the
  ticker legitimately did not trade (halt, suspension, or delisted).
* ``probe_failed`` — the verification request itself errored.

Report-only: never writes to the database.  Sequential single-threaded
probes, bounded by ``--max-probes``.

Usage::

    python scripts\\data_health_verify.py [--gap-days N]
                                        [--max-probes N] [--json]

Exit codes: 0 = health OK / nothing to verify, 1 = gaps found or the
Flight server unreachable.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import timedelta

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)
os.chdir(_REPO_ROOT)

_CONFIG_PATH = os.path.join(_REPO_ROOT, "src", "config.yaml")

import pandas as pd  # noqa: E402
import pyarrow.flight as flight  # noqa: E402
import yfinance as yf  # noqa: E402

from src.config import PipelineConfig  # noqa: E402


def _fetch_health(config: PipelineConfig, gap_days: int) -> dict:
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
    return json.loads(results[0].body.to_pybytes().decode("utf-8"))["health"]


def _collect_targets(health: dict) -> dict:
    """Merge flagged tickers into {ticker: {probe_start, missing_days}}.

    Sources: gap-check missing days, stale tickers (missing from
    last_date+1 through latest_trading_day), coverage-missing tickers.
    """
    latest = health.get("latest_trading_day")
    targets = {}
    gaps = health.get("checks", {}).get("gaps") or {}
    for entry in gaps.get("per_ticker", []):
        days = list(entry.get("missing_days", []))
        if days:
            targets[entry["ticker"]] = {
                "missing_days": days,
                "probe_start": min(days),
            }
    for entry in health.get("checks", {}).get("staleness", {}).get(
        "stale_tickers", []
    ):
        if latest is None:
            continue
        t = entry["ticker"]
        start = (
            pd.Timestamp(entry["last_date"]) + pd.Timedelta(days=1)
        ).strftime("%Y-%m-%d")
        tgt = targets.setdefault(t, {"missing_days": [], "probe_start": start})
        tgt["probe_start"] = min(tgt["probe_start"], start)
        tgt["stale_since"] = start
    for t in health.get("checks", {}).get("coverage", {}).get(
        "missing_tickers", []
    ):
        targets.setdefault(
            t, {"missing_days": [], "probe_start": None, "no_history": True}
        )
    return targets, latest


def _probe(ticker: str, start: str, end: str) -> pd.DataFrame:
    """Single-threaded yf.download over [start, end); gentle pacing."""
    return yf.download(
        tickers=ticker,
        start=start,
        end=end,
        interval="1d",
        auto_adjust=True,
        progress=False,
        threads=False,
    )


def _returned_dates(df: pd.DataFrame) -> set:
    idx = df.index
    if idx.tz is not None:
        idx = idx.tz_convert("America/New_York")
    return {d.strftime("%Y-%m-%d") for d in idx}


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Verify flagged data_health gaps against yfinance."
    )
    parser.add_argument("--gap-days", type=int, default=30,
                        help="gap window passed to data_health (default 30)")
    parser.add_argument("--max-probes", type=int, default=50,
                        help="max tickers probed (default 50)")
    parser.add_argument("--json", action="store_true",
                        help="print raw JSON verdicts")
    args = parser.parse_args()

    config = PipelineConfig.from_yaml(_CONFIG_PATH)
    try:
        health = _fetch_health(config, args.gap_days)
    except Exception as e:
        print(f"ERROR: Flight server unreachable at "
              f"grpc://{config.server.flight_host}:"
              f"{config.server.flight_port}\n{e}")
        return 1

    print(f"data_health: {health.get('health_status')} "
          f"(as_of {health.get('as_of')}, "
          f"latest_trading_day {health.get('latest_trading_day')})")
    if health.get("health_status") == "OK":
        print("Nothing to verify — no flagged gaps.")
        return 0
    if health.get("health_status") == "CHECK_FAILED":
        print(f"Health check itself failed: {health.get('error')}")
        return 1

    targets, latest = _collect_targets(health)
    if not targets:
        print("WARN came from non-gap checks (integrity/duplicates) "
              "— nothing to probe.")
        return 1

    probe_tickers = sorted(targets)[: args.max_probes]
    skipped = len(targets) - len(probe_tickers)
    end_exclusive = (
        pd.Timestamp(latest) + timedelta(days=1)
    ).strftime("%Y-%m-%d")
    print(f"Probing {len(probe_tickers)} ticker(s) against yfinance "
          f"(sequential, single-threaded)" +
          (f" — {skipped} more skipped by --max-probes" if skipped else ""))
    print()

    verdicts = {}
    for t in probe_tickers:
        tgt = targets[t]
        start = tgt["probe_start"]
        if start is None:  # coverage-missing: no history at all
            start = (
                pd.Timestamp(latest) - pd.Timedelta(days=90)
            ).strftime("%Y-%m-%d")
        try:
            df = _probe(t, start, end_exclusive)
        except Exception as e:
            verdicts[t] = {"probe_failed": str(e)}
            print(f"  {t}: probe_failed ({e})")
            continue
        returned = _returned_dates(df) if df is not None else set()
        days = tgt["missing_days"]
        if tgt.get("stale_since") or tgt.get("no_history"):
            # Stale/coverage: classify by whether Yahoo has ANY data in
            # the probed range rather than per-day.
            verdicts[t] = {
                "classification": "download_gap" if returned
                else "halt_or_suspension",
                "yahoo_rows_in_range": len(returned),
                "stale_since": tgt.get("stale_since"),
            }
        else:
            verdicts[t] = {
                "classification": "per_day",
                "days": {
                    d: ("download_gap" if d in returned
                        else "halt_or_suspension")
                    for d in days
                },
            }
        n_gap = sum(
            1 for v in (verdicts[t].get("days") or {}).values()
            if v == "download_gap"
        ) or (len(returned) if verdicts[t].get(
            "classification") == "download_gap" else 0)
        n_halt = sum(
            1 for v in (verdicts[t].get("days") or {}).values()
            if v == "halt_or_suspension"
        ) or (0 if verdicts[t].get(
            "classification") == "download_gap" else 1)
        print(f"  {t}: {verdicts[t]['classification']} "
              f"({n_gap} recoverable day(s), {n_halt} halt/suspension)")

    if args.json:
        print(json.dumps(verdicts, indent=1))

    gap_ct = sum(
        1 for v in verdicts.values()
        if v.get("classification") == "download_gap"
        or "download_gap" in (v.get("days") or {}).values()
    )
    halt_ct = sum(
        1 for v in verdicts.values()
        if v.get("classification") == "halt_or_suspension"
        or "halt_or_suspension" in (v.get("days") or {}).values()
    )
    fail_ct = sum(1 for v in verdicts.values() if "probe_failed" in v)
    print()
    print(f"SUMMARY: {gap_ct} ticker(s) with download gaps "
          f"(repairable via on_request_load), {halt_ct} halt/suspension, "
          f"{fail_ct} probe failure(s)")
    return 1


if __name__ == "__main__":
    sys.exit(main())
