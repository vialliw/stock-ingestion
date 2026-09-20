"""On-demand CNN Fear & Greed refresh via the Flight server.

Sends the ``fear_greed_refresh`` Flight action to the running server —
the ONLY write path, since only the server may open the DuckDB file
(single-writer model).  This script never touches the DB directly.

The server fetches CNN's fear-and-greed graphdata payload (composite
score + 9 sub-indicators) and upserts one row per date into
``fear_greed_history``.  An optional positional argument overrides the
configured ``fear_greed.start_date`` backfill horizon (YYYY-MM-DD).

Usage:
    python scripts/refresh_fear_greed.py [YYYY-MM-DD]

Exit codes: 0 = SUCCESS, 1 = FAILED / server unreachable.
"""

from __future__ import annotations

import json
import os
import sys

import pyarrow.flight as flight

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_CONFIG_PATH = os.path.join(_REPO_ROOT, "src", "config.yaml")
sys.path.insert(0, _REPO_ROOT)

from src.config import PipelineConfig  # noqa: E402


def main(argv=None) -> int:
    cfg = PipelineConfig.from_yaml(_CONFIG_PATH)

    argv = list(sys.argv[1:] if argv is None else argv)
    start_date = argv[0] if argv else None
    body = (
        json.dumps({"start_date": start_date}).encode("utf-8")
        if start_date
        else b"{}"
    )

    loc = flight.Location.for_grpc_tcp(
        cfg.server.flight_host, cfg.server.flight_port
    )
    client = flight.FlightClient(loc)
    action = flight.Action("fear_greed_refresh", body)
    try:
        client.wait_for_available(timeout=10)
        raw = next(
            client.do_action(
                action, options=flight.FlightCallOptions(timeout=300)
            )
        ).body.to_pybytes()
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
    print(f"fear & greed refresh — status: {status}")
    print(f"  rows upserted:   {report.get('rows_upserted')}")
    print(f"  latest date:     {report.get('latest_date')}")
    print(f"  fg_score:        {report.get('fg_score')}")
    print(f"  fg_rating:       {report.get('fg_rating')}")
    print(f"  missing series:  {report.get('missing_series')}")
    print(f"  duration:        {report.get('duration_ms')} ms")
    if report.get("alert_triggered"):
        print(
            f"  ALERT: fg_score {report.get('fg_score')} <= "
            f"{cfg.fear_greed.alert_threshold} "
            f"(extreme-fear sub-indicators: {report.get('alert_detail')})"
        )
    if report.get("error"):
        print(f"  error:           {report['error']}")
    return 0 if status == "SUCCESS" else 1


if __name__ == "__main__":
    sys.exit(main())
