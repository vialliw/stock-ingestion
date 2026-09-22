"""On-demand signal-analysis run via the Flight server (F25-T1).

Sends the ``signal_analysis`` Flight action to the running server —
the ONLY write path, since only the server may open the DuckDB file
(single-writer model).  This script never touches the DB directly.

The server runs the five-phase pipeline ported from
``quant_strategy_building/signal_input2/`` for every configured
``signal_analysis.sets`` entry (each gets its own ``signals_<name>``
schema of ``aly_``-prefixed tables/views):

1. ingest ``*_active_signal_combined.xlsx`` files (all sheets) into
   ``aly_signal_records_raw`` (DROP+rebuild per run)
2. dedupe older runs (latest ``Signal_File`` per
   ``Trading_Date/Model/Ticker``)
3. repair NULL/NaN ``Close`` values from ``daily_close``
4. compile the four ``aly_v_signals_*`` views
5. compute ``aly_signal_performance`` (15 cols, Execution_Status) +
   per-view scorecard + statistical edge validation
6. phase-5 matrix analytics -> reports under
   ``reports.report_dir/signal_analysis/<name>/``

No payload overrides are sent — the ``signal_analysis:`` config block
defaults apply (``backdate_days: 90``, ``max_holding_days: 8``,
``mc_iterations: 50``, ``benchmark: SPY``).

Usage:
    python scripts/run_signal_analysis.py

Exit codes: 0 = SUCCESS, 1 = FAILED/PARTIAL / server unreachable.
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
    action = flight.Action("signal_analysis", b"{}")
    try:
        client.wait_for_available(timeout=10)
        raw = next(
            client.do_action(
                action, options=flight.FlightCallOptions(timeout=3600)
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
    print(f"signal analysis — status: {status}")
    sets = report.get("sets") or {}
    for name, set_result in sets.items():
        if not isinstance(set_result, dict):
            print(f"  {name}: {set_result}")
            continue
        print(
            f"  {name}: status={set_result.get('status')} "
            f"schema={set_result.get('schema')} "
            f"duration={set_result.get('duration_ms')} ms"
        )
        steps = set_result.get("steps") or {}
        for step_name, step in steps.items():
            if isinstance(step, dict):
                summary = ", ".join(
                    f"{k}={v}"
                    for k, v in step.items()
                    if k
                    not in (
                        "files",
                        "unmatched",
                        "scorecard",
                        "edge_validation",
                    )
                )
                print(f"    {step_name}: {summary}")
                scorecard = step.get("scorecard")
                if scorecard:
                    for row in scorecard:
                        print(f"      {row}")
                edge = step.get("edge_validation")
                if isinstance(edge, dict) and edge:
                    print(f"      edge_validation: {edge}")
            else:
                print(f"    {step_name}: {step}")
    print(f"  duration: {duration_ms} ms")
    if report.get("error"):
        print(f"  error:    {report['error']}")
    return 0 if status == "SUCCESS" else 1


if __name__ == "__main__":
    sys.exit(main())
