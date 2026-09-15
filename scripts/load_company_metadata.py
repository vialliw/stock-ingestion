"""On-demand company_metadata backfill via the Flight server.

Sends the ``metadata_load`` Flight action to the running server —
the ONLY write path, since only the server may open the DuckDB file
(single-writer model).  This script never touches the DB directly.

Behaviour on the server side (rate-limit safe port of the legacy
``load_company_metadata_to_duckdb3b.py`` job):

* Default (no flags): fetch ``yf.Ticker(t).info`` only for configured
  tickers missing from ``company_metadata`` (skip-existing), upsert
  ``longName/sector/industry/quoteType`` per ticker.
* ``--refresh``: re-fetch every given/configured ticker.
* ``--tickers A,B``: restrict to an explicit list (still skip-existing
  unless ``--refresh``).
* ``--limit N``: cap the number of tickers fetched (smoke tests).
* ``--delay S``: seconds slept between ``.info`` calls (default 0.5).

The server paces requests and trips a circuit breaker after 15
consecutive fetch failures — a throttled run reports ``ABORTED`` and
leaves the unfetched tickers missing for a later retry.  Successful
but empty ``.info`` responses write NULL-field rows (matching the
legacy job); fetch *exceptions* write no row so they stay retriable.

Usage:
    python scripts/load_company_metadata.py                # missing only
    python scripts/load_company_metadata.py --refresh      # re-fetch all
    python scripts/load_company_metadata.py --tickers AAPL,MSFT --refresh
    python scripts/load_company_metadata.py --limit 5 --delay 1.0
"""

from __future__ import annotations

import argparse
import json
import os
import sys

import pyarrow.flight as flight

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_CONFIG_PATH = os.path.join(_REPO_ROOT, "src", "config.yaml")
sys.path.insert(0, _REPO_ROOT)

from src.config import PipelineConfig  # noqa: E402


def _parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill company_metadata via the Flight server's "
            "'metadata_load' action (the only write path — the server "
            "owns the DuckDB file).  By default only tickers missing "
            "metadata are fetched; use --refresh to re-fetch."
        )
    )
    parser.add_argument(
        "--tickers",
        default=None,
        metavar="A,B,...",
        help=(
            "Comma-separated ticker list.  Default: all configured "
            "tickers (skip-existing unless --refresh)."
        ),
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Re-fetch metadata even for tickers already present.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Fetch at most N tickers (for smoke tests).",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=None,
        metavar="S",
        help="Seconds between .info calls (server default: 0.5).",
    )
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = _parse_args(argv)
    cfg = PipelineConfig.from_yaml(_CONFIG_PATH)

    payload = {"refresh": args.refresh}
    if args.tickers:
        payload["tickers"] = [
            t.strip().upper() for t in args.tickers.split(",") if t.strip()
        ]
    if args.limit is not None:
        payload["max_tickers"] = args.limit
    if args.delay is not None:
        payload["delay_s"] = args.delay

    loc = flight.Location.for_grpc_tcp(
        cfg.server.flight_host, cfg.server.flight_port
    )
    client = flight.FlightClient(loc)
    action = flight.Action("metadata_load", json.dumps(payload).encode("utf-8"))
    try:
        raw = next(client.do_action(action)).body.to_pybytes()
    except Exception as exc:
        print(
            f"Error contacting the Flight server at "
            f"{cfg.server.flight_host}:{cfg.server.flight_port}: {exc}\n"
            f"Is the server running?  Start it with: "
            f"python scripts/run_flight_server.py",
            file=sys.stderr,
        )
        return 1

    report = json.loads(raw.decode("utf-8"))

    status = report.get("status")
    fetched = report.get("tickers_fetched")
    skipped = report.get("tickers_skipped")
    failed = report.get("tickers_failed") or []
    upserted = report.get("rows_upserted")
    aborted = report.get("aborted")
    duration_ms = report.get("duration_ms")
    reason = report.get("reason")
    errors = report.get("errors") or {}

    print(f"company_metadata load — status: {status}")
    print(f"  fetched:        {fetched}")
    print(f"  skipped:        {skipped}")
    print(f"  failed:         {len(failed)}")
    print(f"  rows upserted:  {upserted}")
    print(f"  aborted:        {aborted}")
    print(f"  duration:       {duration_ms} ms")
    if reason:
        print(f"  reason:         {reason}")
    if failed:
        print("  failed tickers:")
        for t in failed:
            print(f"    - {t}")
    if errors:
        print("  errors:")
        for t, e in errors.items():
            print(f"    - {t}: {e}")
    if aborted:
        print(
            "\nCircuit breaker tripped — run again later to retry the "
            "tickers that are still missing."
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
