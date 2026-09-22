"""On-demand signal-pipeline run via the Flight server (F24-T1).

Sends the ``signal_pipeline`` Flight action to the running server —
the ONLY write path, since only the server may open the DuckDB file
(single-writer model).  This script never touches the DB directly.

The server runs the six-step pipeline ported from
``quant_strategy_building/signal_input/orchestrator.py``:

1. ingest ``*_active_signal_combined.xlsx`` files from
   ``signals.folder_path`` into ``signal_records``
2. dedupe older runs (latest ``Signal_File`` per
   ``Trading_Date/Model/Ticker``)
3. repair NULL ``Close`` values from ``daily_close``
4. prune unwanted signals (thresholds active; the configured
   cluster/model exclusion lists are empty)
5. compute ``signal_performance`` forward-return outcomes
6. silent repair sweep + NULL verification, then
   ``cluster_metadata`` refresh

No payload overrides are sent — the ``signals:`` config block
defaults apply (``backdate_days: 3``, ``max_holding_days: 8``,
``overwrite_existing: true``, ``min_reward_risk: 1.39``,
``min_target_pct: 5.5``, empty exclusion lists, no below-open filter).

Usage:
    python scripts/orchestrator.py

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
    action = flight.Action("signal_pipeline", b"{}")
    try:
        client.wait_for_available(timeout=10)
        raw = next(
            client.do_action(
                action, options=flight.FlightCallOptions(timeout=600)
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
    duration_ms = report.get("duration_ms")
    print(f"signal pipeline — status: {status}")
    steps = report.get("steps") or {}
    for name, step in steps.items():
        if isinstance(step, dict):
            summary = ", ".join(
                f"{k}={v}"
                for k, v in step.items()
                if k not in ("files", "pruned_records", "unmatched")
            )
            print(f"  {name}: {summary}")
            for detail_key in ("files", "unmatched", "pruned_records"):
                detail = step.get(detail_key)
                if detail:
                    print(f"    {detail_key}: {detail}")
        else:
            print(f"  {name}: {step}")
    print(f"  duration: {duration_ms} ms")
    if report.get("error"):
        print(f"  error:    {report['error']}")
    return 0 if status == "SUCCESS" else 1


if __name__ == "__main__":
    sys.exit(main())
