"""Tests for client-issued DML on the Flight service (F26-T1).

Covers the ``pivot``-namespace contract end-to-end with a REAL
``DatabaseManager`` (temp-file DuckDB) and a REAL ``IngestionEngine`` —
no mocks on the execution path:

* SCHEMA_VERSION 8 creates ``table_registry`` + ``client_dml_audit``;
  every op is audited incl. REJECTED.
* ``dml_admin`` closed-enum ops: register_namespace (allowlist),
  create_table (structured spec, real PK), create_as
  (snapshot->Arrow->CTAS under lock), create_view, insert_select
  (append|replace; replace is one atomic txn), drop_table (registered
  only), list_tables, pin/unpin, dml_sweep (TTL drops only
  pinned=false past ttl_days).
* Confinement: names regex-validated + server-composed; ``main.*`` and
  unregistered namespaces/tables -> REJECTED + audit row; post-write
  ``duckdb_tables()`` catalog assertion.
* Source-SQL validation: SELECT/WITH prefix + keyword denylist +
  ``extract_statements`` single-SELECT check.
* Generalized ``do_put`` (engine level): create_or_replace|append|
  replace; PK enforced (dup keys fail loudly); schema mismatch ->
  REJECTED.
"""

from __future__ import annotations

import types
from datetime import datetime, timedelta

import pyarrow as pa
import pytest

from src import client_dml
from src.db import DatabaseManager, SCHEMA_VERSION
from src.ingestion import IngestionEngine


# =====================================================================
# Fixtures / helpers
# =====================================================================

def make_cfg(**dml_overrides):
    """Minimal cfg double exposing the client_dml/server blocks."""
    dml = types.SimpleNamespace(
        allowed_namespaces=["pivot"],
        dml_timeout_seconds=None,
        max_namespace_mb=None,
        ttl_days=90,
    )
    for key, value in dml_overrides.items():
        setattr(dml, key, value)
    return types.SimpleNamespace(
        client_dml=dml,
        server=types.SimpleNamespace(query_timeout_seconds=30),
        ingestion=types.SimpleNamespace(exclude_tickers=[]),
    )


@pytest.fixture
def env(tmp_path):
    """(db, engine) on a fresh temp-file DuckDB."""
    db = DatabaseManager(str(tmp_path / "test.duckdb"))
    engine = IngestionEngine(db, make_cfg())
    yield db, engine
    db.close()


def audit_df(env):
    db, _ = env
    return db.execute_query(
        "SELECT op, target, status, error, rows_written, "
        "source_sql_sha256, bytes_in FROM client_dml_audit "
        "ORDER BY audit_id"
    ).to_pandas()


def registered(env):
    db, _ = env
    return db.list_registered_tables()


WEEK_CALENDAR_COLS = [
    {"name": "week_id", "type": "INTEGER", "pk": True},
    {"name": "week_date", "type": "DATE", "not_null": True},
    {"name": "is_partial", "type": "BOOLEAN"},
]


def setup_namespace(env):
    _, eng = env
    return eng.run_dml_admin(
        {"op": "register_namespace", "namespace": "pivot"}
    )


def setup_week_calendar(env):
    _, eng = env
    setup_namespace(env)
    return eng.run_dml_admin(
        {
            "op": "create_table",
            "table": "pivot.week_calendar",
            "columns": WEEK_CALENDAR_COLS,
        }
    )


# =====================================================================
# SCHEMA_VERSION 8 migration
# =====================================================================

class TestMigrationV8:
    def test_schema_version_is_8(self, env):
        db, _ = env
        assert SCHEMA_VERSION == 8
        row = db.execute_query(
            "SELECT MAX(version) FROM schema_version"
        ).to_pandas()
        assert int(row.iloc[0, 0]) == 8

    def test_registry_and_audit_tables_exist(self, env):
        db, _ = env
        tables = set(
            db.execute_query(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main'"
            ).to_pandas()["table_name"]
        )
        assert "table_registry" in tables
        assert "client_dml_audit" in tables

    def test_registry_columns(self, env):
        db, _ = env
        cols = db.execute_query(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'table_registry' "
            "ORDER BY ordinal_position"
        ).to_pandas()["column_name"].tolist()
        assert cols == [
            "namespace", "table_name", "full_name", "owner", "has_pk",
            "pinned", "created_at", "last_write", "rows_est",
        ]


# =====================================================================
# client_dml.py module-level validation
# =====================================================================

class TestNameValidation:
    @pytest.mark.parametrize("good", ["pivot", "research", "a", "ns_2"])
    def test_valid_names_accepted(self, good):
        assert client_dml.validate_name(good) == good

    @pytest.mark.parametrize(
        "bad",
        ["Main", "PIVOT", "pivot.x", "1abc", "", "a-b", "a b", "main'",
         None, 123, ["pivot"]],
    )
    def test_invalid_names_rejected(self, bad):
        with pytest.raises(client_dml.DmlRejected):
            client_dml.validate_name(bad)


class TestParseTableRef:
    def test_dotted_form(self):
        assert client_dml.parse_table_ref(
            {"table": "pivot.t"}, ["pivot"]
        ) == ("pivot", "t")

    def test_namespace_field_form(self):
        assert client_dml.parse_table_ref(
            {"table": "t", "namespace": "pivot"}, ["pivot"]
        ) == ("pivot", "t")

    def test_both_forms_must_agree(self):
        with pytest.raises(client_dml.DmlRejected):
            client_dml.parse_table_ref(
                {"table": "pivot.t", "namespace": "other"}, ["pivot"]
            )

    @pytest.mark.parametrize(
        "payload",
        [
            {"table": "main.daily_close"},
            {"table": "other.t"},
            {"table": "t"},  # no namespace anywhere
            {"table": "a.b.c"},
            {"table": "Pivot.t"},
            {"table": ""},
            {"table": 123},
            {},
        ],
    )
    def test_rejected_refs(self, payload):
        with pytest.raises(client_dml.DmlRejected):
            client_dml.parse_table_ref(payload, ["pivot"])


class TestSourceSqlValidation:
    @pytest.mark.parametrize(
        "sql",
        [
            "SELECT 1",
            "  select * from daily_close  ",
            "WITH x AS (SELECT 1 AS a) SELECT * FROM x",
            "SELECT * FROM \"pivot\".\"t\" WHERE a > 0;",
        ],
    )
    def test_valid_sources_accepted(self, sql):
        assert client_dml.validate_source_sql(sql)

    @pytest.mark.parametrize(
        "sql",
        [
            # denylist — file/exfil + mutation + catalog/session verbs
            "SELECT * FROM read_parquet('x.parquet')",
            "SELECT * FROM read_csv('x.csv')",
            "SELECT * FROM parquet_scan('x')",
            "SELECT * FROM sqlite_scan('x', 't')",
            "SELECT * FROM (COPY (SELECT 1) TO 'x')",
            "SELECT * FROM (ATTACH 'x.db' AS x)",
            "PRAGMA database_size",          # PRAGMA types as SELECT too
            "SELECT 1; SELECT 2",            # multi-statement
            "WITH x AS (DELETE FROM t) SELECT 1",  # WITH+DELETE = DELETE
            "INSERT INTO t SELECT 1",
            "UPDATE t SET a = 1",
            "DELETE FROM t",
            "DROP TABLE t",
            "CREATE TABLE t (a INT)",
            "SET schema = 'x'",
            "CALL checkpoint()",
            "SELECT * FROM t MERGE INTO u",  # MERGE keyword
            "not sql at all",                # parse error
            "",
            None,
            123,
        ],
    )
    def test_denied_sources_rejected(self, sql):
        with pytest.raises(client_dml.DmlRejected):
            client_dml.validate_source_sql(sql)

    def test_multi_statement_rejected(self):
        with pytest.raises(client_dml.DmlRejected):
            client_dml.validate_source_sql(
                "SELECT 1 AS a; SELECT 2 AS b"
            )

    def test_strips_trailing_semicolon(self):
        assert client_dml.validate_source_sql("SELECT 1;") == "SELECT 1"


class TestComposeCreateTableDdl:
    def test_composes_ddl_with_pk(self):
        ddl, has_pk = client_dml.compose_create_table_ddl(
            "pivot", "week_calendar", WEEK_CALENDAR_COLS
        )
        assert has_pk is True
        assert ddl == (
            'CREATE TABLE "pivot"."week_calendar" ('
            '"week_id" INTEGER NOT NULL, '
            '"week_date" DATE NOT NULL, '
            '"is_partial" BOOLEAN, '
            'PRIMARY KEY ("week_id"))'
        )

    def test_no_pk(self):
        ddl, has_pk = client_dml.compose_create_table_ddl(
            "pivot", "t", [{"name": "a", "type": "INTEGER"}]
        )
        assert has_pk is False
        assert "PRIMARY KEY" not in ddl

    def test_mixed_case_column_names_allowed(self):
        """The canonical Bollinger DDL uses Ticker/Date/SplitDate."""
        ddl, _ = client_dml.compose_create_table_ddl(
            "pivot",
            "weekly_bars",
            [
                {"name": "week_id", "type": "INTEGER", "pk": True},
                {"name": "Ticker", "type": "VARCHAR", "pk": True},
                {"name": "Date", "type": "DATE", "not_null": True},
            ],
        )
        assert '"Ticker" VARCHAR NOT NULL' in ddl
        assert 'PRIMARY KEY ("week_id", "Ticker")' in ddl

    @pytest.mark.parametrize(
        "columns",
        [
            [{"name": "a", "type": "TEXT"}],            # not whitelisted
            [{"name": "a", "type": "INT; DROP TABLE"}],  # injection
            [{"name": "a-b", "type": "INTEGER"}],        # bad name
            [{"name": "a", "type": "INTEGER"},
             {"name": "A", "type": "INTEGER"}],          # dup (case-ins)
            [{"name": "a"}],                             # missing type
            [{"type": "INTEGER"}],                       # missing name
            "not a list",
            [],
            [{"name": "a", "type": "INTEGER", "pk": "yes"}],  # bool flag
        ],
    )
    def test_bad_specs_rejected(self, columns):
        with pytest.raises(client_dml.DmlRejected):
            client_dml.compose_create_table_ddl("pivot", "t", columns)


class TestResolveSettings:
    def test_defaults_on_namespace_cfg(self):
        cfg = types.SimpleNamespace(
            client_dml=types.SimpleNamespace(
                allowed_namespaces=["pivot"],
                dml_timeout_seconds=None,
                max_namespace_mb=None,
                ttl_days=90,
            ),
            server=types.SimpleNamespace(query_timeout_seconds=45),
        )
        s = client_dml.resolve_settings(cfg)
        assert s["allowed_namespaces"] == ["pivot"]
        assert s["dml_timeout_seconds"] == 45.0  # inherits query timeout
        assert s["max_namespace_mb"] is None
        assert s["ttl_days"] == 90

    def test_missing_block_falls_back_safe(self):
        cfg = types.SimpleNamespace()
        s = client_dml.resolve_settings(cfg)
        assert s["allowed_namespaces"] == []   # nothing writable
        assert s["dml_timeout_seconds"] == 120.0
        assert s["ttl_days"] == 90


class TestCatalogHelpers:
    def test_catalog_object_kind(self, env):
        db, _ = env
        con = db.conn
        con.execute('CREATE SCHEMA "pivot"')
        con.execute('CREATE TABLE "pivot"."t" (a INTEGER)')
        con.execute('CREATE VIEW "pivot"."v" AS SELECT 1 AS x')
        assert client_dml.catalog_object_kind(con, "pivot", "t") == "table"
        assert client_dml.catalog_object_kind(con, "pivot", "v") == "view"
        assert client_dml.catalog_object_kind(con, "pivot", "zz") is None

    def test_catalog_column_names(self, env):
        db, _ = env
        db.conn.execute('CREATE SCHEMA "pivot"')
        db.conn.execute('CREATE TABLE "pivot"."t" (a INTEGER, "B" VARCHAR)')
        assert client_dml.catalog_column_names(
            db.conn, "pivot", "t"
        ) == ["a", "B"]


# =====================================================================
# dml_admin ops — real engine + real DB
# =====================================================================

class TestRegisterNamespace:
    def test_success(self, env):
        result = setup_namespace(env)
        assert result["status"] == "SUCCESS"
        assert result["namespace"] == "pivot"
        db, _ = env
        schemas = set(
            db.execute_query(
                "SELECT schema_name FROM information_schema.schemata"
            ).to_pandas()["schema_name"]
        )
        assert "pivot" in schemas

    def test_non_allowlisted_rejected_and_audited(self, env):
        _, eng = env
        result = eng.run_dml_admin(
            {"op": "register_namespace", "namespace": "other"}
        )
        assert result["status"] == "REJECTED"
        rows = audit_df(env)
        assert list(rows["status"]) == ["REJECTED"]
        assert rows.iloc[0]["op"] == "register_namespace"
        assert rows.iloc[0]["target"] == "other"

    def test_bad_name_rejected(self, env):
        _, eng = env
        result = eng.run_dml_admin(
            {"op": "register_namespace", "namespace": "Main"}
        )
        assert result["status"] == "REJECTED"


class TestCreateTable:
    def test_creates_real_pk_table(self, env):
        result = setup_week_calendar(env)
        assert result["status"] == "SUCCESS"
        assert result["has_pk"] is True
        reg = registered(env)
        assert len(reg) == 1
        assert reg[0]["full_name"] == "pivot.week_calendar"
        assert reg[0]["has_pk"] is True
        assert reg[0]["pinned"] is True  # pivot default-pinned

    def test_pk_enforced_loudly(self, env):
        """A duplicate-key insert_select must FAIL, not silently pass."""
        _, eng = env
        setup_week_calendar(env)
        one = eng.run_dml_admin(
            {
                "op": "insert_select",
                "table": "pivot.week_calendar",
                "mode": "append",
                "sql": "SELECT 1 AS week_id, DATE '2026-01-02' AS "
                       "week_date, false AS is_partial",
            }
        )
        assert one["status"] == "SUCCESS"
        dup = eng.run_dml_admin(
            {
                "op": "insert_select",
                "table": "pivot.week_calendar",
                "mode": "append",
                "sql": "SELECT 1 AS week_id, DATE '2026-01-02' AS "
                       "week_date, false AS is_partial",
            }
        )
        assert dup["status"] == "FAILED"
        assert "primary key" in dup["error"].lower() or "constraint" in (
            dup["error"].lower()
        )
        # The FAILED op is audited too.
        rows = audit_df(env)
        assert "FAILED" in set(rows["status"])

    def test_existing_object_rejected(self, env):
        _, eng = env
        setup_week_calendar(env)
        result = eng.run_dml_admin(
            {
                "op": "create_table",
                "table": "pivot.week_calendar",
                "columns": [{"name": "a", "type": "INTEGER"}],
            }
        )
        assert result["status"] == "REJECTED"
        assert "already exists" in result["error"]

    def test_invalid_spec_rejected_and_audited(self, env):
        _, eng = env
        setup_namespace(env)
        result = eng.run_dml_admin(
            {
                "op": "create_table",
                "table": "pivot.t",
                "columns": [{"name": "a", "type": "TEXT"}],
            }
        )
        assert result["status"] == "REJECTED"
        assert audit_df(env).iloc[-1]["status"] == "REJECTED"

    def test_main_namespace_confined(self, env):
        """Writes into main.* can never be spelled — REJECTED+audited."""
        _, eng = env
        result = eng.run_dml_admin(
            {
                "op": "create_table",
                "table": "main.daily_close",
                "columns": [{"name": "a", "type": "INTEGER"}],
            }
        )
        assert result["status"] == "REJECTED"
        rows = audit_df(env)
        assert rows.iloc[-1]["status"] == "REJECTED"
        assert rows.iloc[-1]["target"] == "main.daily_close"


class TestCreateAs:
    def test_snapshot_arrow_path(self, env):
        _, eng = env
        setup_week_calendar(env)
        eng.run_dml_admin(
            {
                "op": "insert_select",
                "table": "pivot.week_calendar",
                "mode": "append",
                "sql": "SELECT 1 AS week_id, DATE '2026-01-02' AS "
                       "week_date, false AS is_partial",
            }
        )
        result = eng.run_dml_admin(
            {
                "op": "create_as",
                "table": "pivot.wc_copy",
                "sql": 'SELECT * FROM "pivot"."week_calendar"',
            }
        )
        assert result["status"] == "SUCCESS"
        assert result["rows_written"] == 1
        assert result["materialized_via"] == "arrow_snapshot"
        db, _ = env
        assert int(
            db.execute_query(
                'SELECT COUNT(*) FROM "pivot"."wc_copy"'
            ).to_pandas().iloc[0, 0]
        ) == 1
        reg = db.get_registered_table("pivot", "wc_copy")
        assert reg["has_pk"] is False

    def test_source_sql_sha256_audited(self, env):
        _, eng = env
        setup_namespace(env)
        sql = "SELECT 1 AS a"
        eng.run_dml_admin(
            {"op": "create_as", "table": "pivot.t", "sql": sql}
        )
        rows = audit_df(env)
        last = rows.iloc[-1]
        assert last["source_sql_sha256"] == client_dml.sql_sha256(sql)

    @pytest.mark.parametrize(
        "sql",
        [
            "SELECT * FROM read_parquet('x')",
            "SELECT 1; SELECT 2",
            "DELETE FROM \"pivot\".\"t\"",
            "PRAGMA database_size",
            "WITH x AS (INSERT INTO t VALUES (1)) SELECT 1",
        ],
    )
    def test_denied_sources_rejected(self, env, sql):
        _, eng = env
        setup_namespace(env)
        result = eng.run_dml_admin(
            {"op": "create_as", "table": "pivot.t", "sql": sql}
        )
        assert result["status"] == "REJECTED"
        assert audit_df(env).iloc[-1]["status"] == "REJECTED"

    def test_create_as_on_pk_table_rejected(self, env):
        """create_as would silently drop the PK — refused."""
        _, eng = env
        setup_week_calendar(env)
        result = eng.run_dml_admin(
            {
                "op": "create_as",
                "table": "pivot.week_calendar",
                "sql": "SELECT 1 AS week_id",
            }
        )
        assert result["status"] == "REJECTED"


class TestCreateView:
    def test_creates_view_and_registers(self, env):
        _, eng = env
        setup_week_calendar(env)
        result = eng.run_dml_admin(
            {
                "op": "create_view",
                "table": "pivot.v_wc",
                "sql": 'SELECT week_id, week_date FROM '
                      '"pivot"."week_calendar"',
            }
        )
        assert result["status"] == "SUCCESS"
        db, _ = env
        assert client_dml.catalog_object_kind(
            db.conn, "pivot", "v_wc"
        ) == "view"
        assert db.get_registered_table("pivot", "v_wc") is not None

    def test_invalid_source_rejected(self, env):
        _, eng = env
        setup_namespace(env)
        result = eng.run_dml_admin(
            {
                "op": "create_view",
                "table": "pivot.v",
                "sql": "SELECT * FROM read_csv('x')",
            }
        )
        assert result["status"] == "REJECTED"

    def test_broken_source_sql_fails(self, env):
        """Parses + passes denylist but fails to bind -> FAILED."""
        _, eng = env
        setup_namespace(env)
        result = eng.run_dml_admin(
            {
                "op": "create_view",
                "table": "pivot.v",
                "sql": "SELECT nope FROM missing_table",
            }
        )
        assert result["status"] == "FAILED"
        assert audit_df(env).iloc[-1]["status"] == "FAILED"


class TestInsertSelect:
    def test_append(self, env):
        _, eng = env
        setup_week_calendar(env)
        r1 = eng.run_dml_admin(
            {
                "op": "insert_select",
                "table": "pivot.week_calendar",
                "mode": "append",
                "sql": "SELECT 1 AS week_id, DATE '2026-01-02' AS "
                       "week_date, false AS is_partial",
            }
        )
        assert r1["status"] == "SUCCESS"
        assert r1["rows_written"] == 1

    def test_replace_is_atomic(self, env):
        """mode=replace = ONE txn DELETE+INSERT — a poisoned source
        leaves the OLD rows, not an empty table."""
        _, eng = env
        setup_week_calendar(env)
        eng.run_dml_admin(
            {
                "op": "insert_select",
                "table": "pivot.week_calendar",
                "mode": "append",
                "sql": "SELECT 1 AS week_id, DATE '2026-01-02' AS "
                       "week_date, false AS is_partial",
            }
        )
        # Poisoned: second row dup-keys INSIDE the single INSERT.
        poisoned = eng.run_dml_admin(
            {
                "op": "insert_select",
                "table": "pivot.week_calendar",
                "mode": "replace",
                "sql": "SELECT 2 AS week_id, DATE '2026-01-03' AS "
                       "week_date, true AS is_partial "
                       "UNION ALL SELECT 2, DATE '2026-01-04', false",
            }
        )
        assert poisoned["status"] == "FAILED"
        db, _ = env
        rows = db.execute_query(
            'SELECT week_id FROM "pivot"."week_calendar"'
        ).to_pandas()
        assert rows["week_id"].tolist() == [1]  # old state preserved

        # A clean replace really replaces.
        clean = eng.run_dml_admin(
            {
                "op": "insert_select",
                "table": "pivot.week_calendar",
                "mode": "replace",
                "sql": "SELECT 7 AS week_id, DATE '2026-02-02' AS "
                       "week_date, false AS is_partial "
                       "UNION ALL SELECT 8, DATE '2026-02-09', false",
            }
        )
        assert clean["status"] == "SUCCESS"
        assert clean["rows_written"] == 2
        rows = db.execute_query(
            'SELECT week_id FROM "pivot"."week_calendar" ORDER BY 1'
        ).to_pandas()
        assert rows["week_id"].tolist() == [7, 8]

    def test_unregistered_table_rejected(self, env):
        _, eng = env
        setup_namespace(env)
        result = eng.run_dml_admin(
            {
                "op": "insert_select",
                "table": "pivot.ghost",
                "mode": "append",
                "sql": "SELECT 1 AS a",
            }
        )
        assert result["status"] == "REJECTED"

    def test_bad_mode_rejected(self, env):
        _, eng = env
        setup_week_calendar(env)
        result = eng.run_dml_admin(
            {
                "op": "insert_select",
                "table": "pivot.week_calendar",
                "mode": "upsert",
                "sql": "SELECT 1 AS week_id",
            }
        )
        assert result["status"] == "REJECTED"


class TestDropTable:
    def test_drops_table_and_unregisters(self, env):
        _, eng = env
        setup_week_calendar(env)
        result = eng.run_dml_admin(
            {"op": "drop_table", "table": "pivot.week_calendar"}
        )
        assert result["status"] == "SUCCESS"
        assert result["dropped"] == "table"
        db, _ = env
        assert db.get_registered_table("pivot", "week_calendar") is None
        assert client_dml.catalog_object_kind(
            db.conn, "pivot", "week_calendar"
        ) is None

    def test_drops_view_with_drop_view(self, env):
        _, eng = env
        setup_week_calendar(env)
        eng.run_dml_admin(
            {
                "op": "create_view",
                "table": "pivot.v_wc",
                "sql": 'SELECT week_id FROM "pivot"."week_calendar"',
            }
        )
        result = eng.run_dml_admin(
            {"op": "drop_table", "table": "pivot.v_wc"}
        )
        assert result["status"] == "SUCCESS"
        assert result["dropped"] == "view"

    def test_unregistered_rejected(self, env):
        _, eng = env
        setup_namespace(env)
        result = eng.run_dml_admin(
            {"op": "drop_table", "table": "pivot.ghost"}
        )
        assert result["status"] == "REJECTED"


class TestListTables:
    def test_lists_registered_objects(self, env):
        _, eng = env
        setup_week_calendar(env)
        eng.run_dml_admin(
            {
                "op": "create_as",
                "table": "pivot.copy",
                "sql": "SELECT 1 AS a",
            }
        )
        result = eng.run_dml_admin({"op": "list_tables"})
        assert result["status"] == "SUCCESS"
        names = {t["full_name"] for t in result["tables"]}
        assert names == {"pivot.week_calendar", "pivot.copy"}

    def test_namespace_filter_and_reject(self, env):
        _, eng = env
        setup_namespace(env)
        result = eng.run_dml_admin(
            {"op": "list_tables", "namespace": "pivot"}
        )
        assert result["status"] == "SUCCESS"
        assert result["tables"] == []
        bad = eng.run_dml_admin(
            {"op": "list_tables", "namespace": "main"}
        )
        assert bad["status"] == "REJECTED"


class TestPinUnpinAndSweep:
    def test_pin_unpin_flips_registry_flag(self, env):
        _, eng = env
        setup_week_calendar(env)
        r = eng.run_dml_admin(
            {"op": "unpin", "table": "pivot.week_calendar"}
        )
        assert r["status"] == "SUCCESS"
        db, _ = env
        assert db.get_registered_table("pivot", "week_calendar")[
            "pinned"
        ] is False
        eng.run_dml_admin({"op": "pin", "table": "pivot.week_calendar"})
        assert db.get_registered_table("pivot", "week_calendar")[
            "pinned"
        ] is True

    def test_unpin_unregistered_rejected(self, env):
        _, eng = env
        setup_namespace(env)
        result = eng.run_dml_admin(
            {"op": "unpin", "table": "pivot.ghost"}
        )
        assert result["status"] == "REJECTED"

    def test_sweep_drops_only_expired_unpinned(self, env):
        """TTL sweep: pinned=false + older than ttl -> dropped;
        pinned and fresh objects survive."""
        db, eng = env
        setup_week_calendar(env)                       # pinned (default)
        eng.run_dml_admin(
            {
                "op": "create_as",
                "table": "pivot.stale",
                "sql": "SELECT 1 AS a",
            }
        )
        eng.run_dml_admin(
            {"op": "unpin", "table": "pivot.stale"}
        )
        # Backdate 'stale' past the TTL; week_calendar is pinned so its
        # age doesn't matter.
        db.conn.execute(
            "UPDATE table_registry SET last_write = ? "
            "WHERE table_name = 'stale'",
            [datetime.now() - timedelta(days=120)],
        )
        result = eng.run_dml_admin({"op": "dml_sweep"})
        assert result["status"] == "SUCCESS"
        assert result["dropped"] == ["pivot.stale"]
        remaining = {t["table_name"] for t in registered(env)}
        assert remaining == {"week_calendar"}
        assert client_dml.catalog_object_kind(
            db.conn, "pivot", "stale"
        ) is None

    def test_sweep_respects_fresh_unpinned(self, env):
        _, eng = env
        setup_namespace(env)
        eng.run_dml_admin(
            {"op": "create_as", "table": "pivot.fresh",
             "sql": "SELECT 1 AS a"}
        )
        eng.run_dml_admin({"op": "unpin", "table": "pivot.fresh"})
        result = eng.run_dml_admin({"op": "dml_sweep", "ttl_days": 90})
        assert result["status"] == "SUCCESS"
        assert result["dropped"] == []
        assert {t["table_name"] for t in registered(env)} == {"fresh"}


class TestAdminMisc:
    def test_unknown_op_rejected_and_audited(self, env):
        _, eng = env
        result = eng.run_dml_admin({"op": "execute_dml", "sql": "x"})
        assert result["status"] == "REJECTED"
        rows = audit_df(env)
        assert rows.iloc[-1]["status"] == "REJECTED"
        assert rows.iloc[-1]["op"] == "execute_dml"

    def test_non_dict_payload_raises(self, env):
        _, eng = env
        with pytest.raises(ValueError):
            eng.run_dml_admin([1, 2])

    def test_every_op_writes_an_audit_row(self, env):
        """SUCCESS + FAILED + REJECTED all land in client_dml_audit."""
        _, eng = env
        setup_namespace(env)                                  # SUCCESS
        eng.run_dml_admin({"op": "register_namespace",
                           "namespace": "other"})              # REJECTED
        eng.run_dml_admin(                                     # FAILED
            {"op": "create_view", "table": "pivot.v",
             "sql": "SELECT nope FROM missing_table"}
        )
        rows = audit_df(env)
        assert set(rows["status"]) == {"SUCCESS", "REJECTED", "FAILED"}

    def test_client_tag_audited(self, env):
        _, eng = env
        eng.run_dml_admin(
            {"op": "register_namespace", "namespace": "pivot",
             "client_tag": "bollinger-p1"}
        )
        rows = env[0].execute_query(
            "SELECT client_tag FROM client_dml_audit"
        ).to_pandas()
        assert rows.iloc[-1]["client_tag"] == "bollinger-p1"


# =====================================================================
# Generalized do_put — engine level (wire path tested in
# tests/test_flight_server.py)
# =====================================================================

class TestRunDmlPut:
    def _frame(self, a=(1, 2), b=("x", "y")):
        return pa.table({"a": list(a), "b": list(b)})

    def test_create_or_replace_registers(self, env):
        db, eng = env
        setup_namespace(env)
        result = eng.run_dml_put(
            {"table": "pivot.t", "mode": "create_or_replace"},
            self._frame(),
        )
        assert result["status"] == "SUCCESS"
        assert result["rows_written"] == 2
        assert db.get_registered_table("pivot", "t") is not None
        rows = db.execute_query('SELECT * FROM "pivot"."t"').to_pandas()
        assert len(rows) == 2

    def test_append_into_registered_table(self, env):
        db, eng = env
        setup_week_calendar(env)
        frame = pa.table(
            {
                "week_id": [1, 2],
                "week_date": pa.array(
                    [datetime(2026, 1, 2).date(),
                     datetime(2026, 1, 9).date()],
                    type=pa.date32(),
                ),
                "is_partial": [False, False],
            }
        )
        result = eng.run_dml_put(
            {"table": "pivot.week_calendar", "mode": "append"}, frame
        )
        assert result["status"] == "SUCCESS"
        assert result["rows_written"] == 2
        # Second append of the same keys -> PK violation, loud.
        with pytest.raises(Exception) as excinfo:
            eng.run_dml_put(
                {"table": "pivot.week_calendar", "mode": "append"}, frame
            )
        assert "primary key" in str(excinfo.value).lower() or (
            "constraint" in str(excinfo.value).lower()
        )
        rows = audit_df(env)
        assert rows.iloc[-1]["status"] == "FAILED"
        assert rows.iloc[-1]["op"] == "do_put:append"

    def test_replace_swaps_contents_atomically(self, env):
        db, eng = env
        setup_namespace(env)
        eng.run_dml_put(
            {"table": "pivot.t", "mode": "create_or_replace"},
            self._frame(),
        )
        eng.run_dml_put(
            {"table": "pivot.t", "mode": "replace"},
            self._frame(a=(7,), b=("z",)),
        )
        rows = db.execute_query(
            'SELECT a FROM "pivot"."t"'
        ).to_pandas()["a"].tolist()
        assert rows == [7]

    def test_append_unregistered_rejected(self, env):
        _, eng = env
        setup_namespace(env)
        with pytest.raises(client_dml.DmlRejected):
            eng.run_dml_put(
                {"table": "pivot.ghost", "mode": "append"},
                self._frame(),
            )
        rows = audit_df(env)
        assert rows.iloc[-1]["status"] == "REJECTED"
        assert rows.iloc[-1]["op"] == "do_put:append"

    def test_create_or_replace_on_pk_table_rejected(self, env):
        _, eng = env
        setup_week_calendar(env)
        with pytest.raises(client_dml.DmlRejected):
            eng.run_dml_put(
                {"table": "pivot.week_calendar",
                 "mode": "create_or_replace"},
                self._frame(),
            )

    def test_schema_mismatch_rejected(self, env):
        _, eng = env
        setup_week_calendar(env)
        bad = pa.table({"week_id": [1], "bogus_col": ["x"]})
        with pytest.raises(client_dml.DmlRejected) as excinfo:
            eng.run_dml_put(
                {"table": "pivot.week_calendar", "mode": "append"}, bad
            )
        assert "schema mismatch" in str(excinfo.value)

    def test_bad_mode_and_bad_namespace_rejected(self, env):
        _, eng = env
        setup_namespace(env)
        with pytest.raises(client_dml.DmlRejected):
            eng.run_dml_put(
                {"table": "pivot.t", "mode": "upsert"}, self._frame()
            )
        with pytest.raises(client_dml.DmlRejected):
            eng.run_dml_put(
                {"table": "main.daily_close", "mode": "append"},
                self._frame(),
            )
        rows = audit_df(env)
        put_rows = rows[rows["op"].str.startswith("do_put")]
        assert set(put_rows["status"]) == {"REJECTED"}
        assert "main.daily_close" in set(put_rows["target"])

    def test_bytes_in_audited(self, env):
        _, eng = env
        setup_namespace(env)
        eng.run_dml_put(
            {"table": "pivot.t", "mode": "create_or_replace"},
            self._frame(),
        )
        rows = audit_df(env)
        assert rows.iloc[-1]["bytes_in"] > 0


# =====================================================================
# Quota (advisory, off by default)
# =====================================================================

class TestQuota:
    def test_quota_zero_blocks_writes(self, tmp_path):
        db = DatabaseManager(str(tmp_path / "q.duckdb"))
        cfg = make_cfg(max_namespace_mb=0.000001)
        eng = IngestionEngine(db, cfg)
        try:
            eng.run_dml_admin(
                {"op": "register_namespace", "namespace": "pivot"}
            )
            # Force a nonzero estimate so the quota trips.
            db.conn.execute(
                "CREATE TABLE \"pivot\".\"big\" AS "
                "SELECT * FROM range(100000) t(a)"
            )
            result = eng.run_dml_admin(
                {"op": "create_as", "table": "pivot.t",
                 "sql": "SELECT 1 AS a"}
            )
            assert result["status"] == "REJECTED"
            assert "max_namespace_mb" in result["error"]
        finally:
            db.close()


# =====================================================================
# Config block
# =====================================================================

class TestConfigBlock:
    def test_pipeline_config_has_client_dml_defaults(self):
        from src.config import PipelineConfig

        cfg = PipelineConfig()
        assert cfg.client_dml.allowed_namespaces == ["pivot"]
        assert cfg.client_dml.dml_timeout_seconds is None
        assert cfg.client_dml.max_namespace_mb is None
        assert cfg.client_dml.ttl_days == 90

    def test_real_yaml_loads_client_dml(self):
        import os
        from src.config import PipelineConfig

        path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "src", "config.yaml",
        )
        cfg = PipelineConfig.from_yaml(path)
        assert cfg.client_dml.allowed_namespaces == ["pivot"]
        assert cfg.client_dml.ttl_days == 90

    @pytest.mark.parametrize(
        "bad",
        [["Main"], ["a-b"], ["pivot", "PIVOT"], "pivot"],
    )
    def test_bad_namespaces_rejected(self, bad):
        from src.config import ClientDmlConfig
        import pydantic

        with pytest.raises(pydantic.ValidationError):
            ClientDmlConfig(allowed_namespaces=bad)
