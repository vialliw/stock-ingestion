"""Thin client for the ``dml_admin`` action + generalized ``do_put``
(F26-T1 — the ``pivot`` namespace contract).

Vendorable: copy this file into a downstream project (it only needs
``pyarrow`` + a reachable Flight server) or import it in-repo.  The
library functions take a ``flight.FlightClient``; ``main()`` is a small
CLI for the common ops.  ALL writes go through the Flight server — the
server holds the DuckDB file lock; there is no other write path.

Wire contract (server side: ``src/flight_server.py`` +
``src/ingestion.py``):

* ``do_action("dml_admin", <json>)`` — ``{"op": <enum>, ...}`` returns a
  JSON dict ``{"status": "SUCCESS"|"FAILED"|"REJECTED", ...}``.
* ``do_put(<command descriptor>, <arrow>)`` — the descriptor is a JSON
  command ``{"table": "pivot.t", "mode":
  "create_or_replace|append|replace", ...}``.

IMPORTANT: ``pivot`` is a DuckDB reserved word — every SQL string you
send (``create_as``/``create_view``/``insert_select`` sources and your
own ``do_get`` reads) must quote the schema: ``"pivot"."table"``.

Usage (library):

    import pyarrow.flight as flight
    from dml_client import DmlClient

    dml = DmlClient(flight.FlightClient("grpc://127.0.0.1:8815"))
    dml.register_namespace("pivot")
    dml.create_table("pivot.week_calendar", [
        {"name": "week_id", "type": "INTEGER", "pk": True},
        {"name": "week_date", "type": "DATE", "not_null": True},
    ])
    dml.insert_select("pivot.week_calendar", "append",
                      'SELECT 1 AS week_id, DATE \\'2026-01-02\\' AS week_date')
    dml.put_frame("pivot.data_quality_report", "append", arrow_table)

Usage (CLI):

    python scripts/dml_client.py register-namespace pivot
    python scripts/dml_client.py list-tables [--namespace pivot]
    python scripts/dml_client.py drop-table pivot.old_table
    python scripts/dml_client.py sweep [--ttl-days 90]
    python scripts/dml_client.py admin '{"op": "pin", "table": "pivot.t"}'

Exit codes: 0 = SUCCESS, 1 = FAILED/REJECTED/server unreachable.
"""

from __future__ import annotations

import argparse
import json
import os
import sys

import pyarrow as pa
import pyarrow.flight as flight

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_CONFIG_PATH = os.path.join(_REPO_ROOT, "src", "config.yaml")
sys.path.insert(0, _REPO_ROOT)

from src.config import PipelineConfig  # noqa: E402

_ACTION = "dml_admin"


class DmlClient:
    """Typed wrappers over the ``dml_admin`` action + ``do_put``."""

    def __init__(self, client: flight.FlightClient, client_tag=None,
                 timeout: float = 300.0):
        self.client = client
        self.client_tag = client_tag
        self.timeout = timeout

    # ----- core -------------------------------------------------

    def admin(self, payload: dict) -> dict:
        """Send one ``dml_admin`` payload; returns the result dict."""
        body = dict(payload)
        if self.client_tag is not None and "client_tag" not in body:
            body["client_tag"] = self.client_tag
        action = flight.Action(
            _ACTION, json.dumps(body).encode("utf-8")
        )
        raw = next(
            self.client.do_action(
                action,
                options=flight.FlightCallOptions(timeout=self.timeout),
            )
        ).body.to_pybytes()
        return json.loads(raw.decode("utf-8"))

    def put_frame(self, table: str, mode: str, frame,
                  namespace=None, client_tag=None,
                  timeout: float = None) -> None:
        """Stream ``frame`` (pa.Table / pandas df) to ``table`` under
        ``mode`` — create_or_replace|append|replace.

        Raises ``flight.FlightError`` on REJECTED/FAILED writes.
        """
        descriptor_body = {"table": table, "mode": mode}
        if namespace is not None:
            descriptor_body["namespace"] = namespace
        tag = client_tag if client_tag is not None else self.client_tag
        if tag is not None:
            descriptor_body["client_tag"] = tag
        descriptor = flight.FlightDescriptor.for_command(
            json.dumps(descriptor_body).encode("utf-8")
        )
        if isinstance(frame, pa.Table):
            arrow_table = frame
        elif hasattr(frame, "to_arrow"):  # pandas DataFrame
            arrow_table = pa.Table.from_pandas(
                frame, preserve_index=False
            )
        else:
            arrow_table = pa.table(frame)
        options = flight.FlightCallOptions(
            timeout=timeout if timeout is not None else self.timeout
        )
        writer, _ = self.client.do_put(
            descriptor, arrow_table.schema, options=options
        )
        try:
            writer.write_table(arrow_table)
            writer.done_writing()
        finally:
            try:
                writer.close()
            except Exception:
                pass

    # ----- op wrappers -------------------------------------------

    def register_namespace(self, namespace: str) -> dict:
        return self.admin({"op": "register_namespace",
                           "namespace": namespace})

    def create_table(self, table: str, columns: list,
                     namespace=None, pinned=None) -> dict:
        payload = {"op": "create_table", "table": table,
                   "columns": columns}
        if namespace is not None:
            payload["namespace"] = namespace
        if pinned is not None:
            payload["pinned"] = pinned
        return self.admin(payload)

    def create_as(self, table: str, sql: str,
                  namespace=None, pinned=None) -> dict:
        payload = {"op": "create_as", "table": table, "sql": sql}
        if namespace is not None:
            payload["namespace"] = namespace
        if pinned is not None:
            payload["pinned"] = pinned
        return self.admin(payload)

    def create_view(self, table: str, sql: str,
                    namespace=None, pinned=None) -> dict:
        payload = {"op": "create_view", "table": table, "sql": sql}
        if namespace is not None:
            payload["namespace"] = namespace
        if pinned is not None:
            payload["pinned"] = pinned
        return self.admin(payload)

    def insert_select(self, table: str, mode: str, sql: str,
                      namespace=None) -> dict:
        payload = {"op": "insert_select", "table": table,
                   "mode": mode, "sql": sql}
        if namespace is not None:
            payload["namespace"] = namespace
        return self.admin(payload)

    def drop_table(self, table: str, namespace=None) -> dict:
        payload = {"op": "drop_table", "table": table}
        if namespace is not None:
            payload["namespace"] = namespace
        return self.admin(payload)

    def list_tables(self, namespace=None) -> dict:
        payload = {"op": "list_tables"}
        if namespace is not None:
            payload["namespace"] = namespace
        return self.admin(payload)

    def pin(self, table: str, namespace=None) -> dict:
        payload = {"op": "pin", "table": table}
        if namespace is not None:
            payload["namespace"] = namespace
        return self.admin(payload)

    def unpin(self, table: str, namespace=None) -> dict:
        payload = {"op": "unpin", "table": table}
        if namespace is not None:
            payload["namespace"] = namespace
        return self.admin(payload)

    def sweep(self, ttl_days=None) -> dict:
        payload = {"op": "dml_sweep"}
        if ttl_days is not None:
            payload["ttl_days"] = ttl_days
        return self.admin(payload)


def _make_client(cfg) -> DmlClient:
    loc = flight.Location.for_grpc_tcp(
        cfg.server.flight_host, cfg.server.flight_port
    )
    client = flight.FlightClient(loc)
    client.wait_for_available(timeout=10)
    return DmlClient(client, client_tag="dml_client.py")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Client-DML helper for the Flight service "
                    "(pivot namespace)"
    )
    parser.add_argument(
        "--client-tag", default="dml_client.py",
        help="advisory identity recorded on audit rows",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("register-namespace")
    p.add_argument("namespace")

    p = sub.add_parser("list-tables")
    p.add_argument("--namespace", default=None)

    p = sub.add_parser("drop-table")
    p.add_argument("table")

    p = sub.add_parser("pin")
    p.add_argument("table")
    p = sub.add_parser("unpin")
    p.add_argument("table")

    p = sub.add_parser("sweep")
    p.add_argument("--ttl-days", type=int, default=None)

    p = sub.add_parser(
        "admin", help="raw op: admin '{\"op\": \"create_view\", ...}'"
    )
    p.add_argument("payload", help="JSON object with an 'op' key")

    args = parser.parse_args(argv)
    cfg = PipelineConfig.from_yaml(_CONFIG_PATH)

    try:
        dml = _make_client(cfg)
        dml.client_tag = args.client_tag
        if args.cmd == "register-namespace":
            result = dml.register_namespace(args.namespace)
        elif args.cmd == "list-tables":
            result = dml.list_tables(args.namespace)
        elif args.cmd == "drop-table":
            result = dml.drop_table(args.table)
        elif args.cmd == "pin":
            result = dml.pin(args.table)
        elif args.cmd == "unpin":
            result = dml.unpin(args.table)
        elif args.cmd == "sweep":
            result = dml.sweep(args.ttl_days)
        elif args.cmd == "admin":
            payload = json.loads(args.payload)
            if not isinstance(payload, dict):
                raise ValueError("payload must be a JSON object")
            result = dml.admin(payload)
        else:  # pragma: no cover - argparse enforces the enum
            parser.error(f"unknown command {args.cmd}")
    except Exception as exc:
        print(
            f"Error contacting the Flight server at "
            f"{cfg.server.flight_host}:{cfg.server.flight_port}: {exc}\n"
            f"Is the server running?  Start it with: "
            f"python scripts/run_flight_server.py",
            file=sys.stderr,
        )
        return 1

    print(json.dumps(result, indent=2))
    return 0 if result.get("status") == "SUCCESS" else 1


if __name__ == "__main__":
    sys.exit(main())
