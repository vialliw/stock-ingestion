"""On-demand weekly-layer refresh via the Flight server.

Sends the ``weekly_refresh`` Flight action to the running server —
the ONLY write path, since only the server may open the DuckDB file
(single-writer model).  This script never touches the DB directly.

The server rebuilds ``weekly_close`` from ``daily_close`` (nominal
Friday week keys via ``date_trunc('week', Date)::DATE + 4``) and then
``weekly_indicators`` from ``weekly_close`` (stochastic incl.
``Slow_D_80_20_Crossover``, Bollinger, PPO, Force Index, log-EMAs) —
a verbatim port of the legacy ``aggregate_to_weekly`` /
``update_weekly_indicators_01`` jobs in
``libs/vw_pipe_data_duckdb.py``.

Usage:
    python scripts/refresh_weekly.py

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

    loc = flight.Location.for_grpc_tcp(
        cfg.server.flight_host, cfg.server.flight_port
    )
    client = flight.FlightClient(loc)
    action = flight.Action("weekly_refresh", b"{}")
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
    close_rows = report.get("weekly_close_rows")
    indicator_rows = report.get("weekly_indicators_rows")
    duration_ms = report.get("duration_ms")

    print(f"weekly refresh — status: {status}")
    print(f"  weekly_close rows:      {close_rows}")
    print(f"  weekly_indicators rows: {indicator_rows}")
    print(f"  duration:               {duration_ms} ms")
    if report.get("error"):
        print(f"  error:                  {report['error']}")
    return 0 if status == "SUCCESS" else 1


if __name__ == "__main__":
    sys.exit(main())
