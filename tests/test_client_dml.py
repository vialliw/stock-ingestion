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
* Source-SQL validation: SELECT/WITH prefix + keyword/function
  denylist + replacement-scan literal denylist +
  ``extract_statements`` single-SELECT check (F26-T2: every
  challenger-verified exfil vector rejected + audited on all three
  source-SQL ops; the create_as snapshot read is bounded by
  ``dml_timeout_seconds`` via worker + interrupt.  F26-T3: the
  generator/side-effect family — ``dbgen``/``dsdgen``/``tpch_*``/
  ``tpcds_*``/``enable_*``/``disable_*``/``verify_*``,
  ``add_parquet_key``, ``import_database``/``export_database``,
  ``describe``/``desc``/``summarize``/``explain``/``show*`` statement
  forms, bundled autoloadable-extension families — rejected + audited
  on all three ops; comment-separated ``FROM 'x'`` literals denied.
  F26-T4: extension autoload is disabled server-wide at
  ``DatabaseManager`` open (``autoinstall``/``autoload_known_extensions
  =false``, GLOBAL — do_get's identical hole closed too), the
  autocomplete/fts/inet/excel entrypoints and built-in
  ``truncate_duckdb_logs``/``write_log``/``error``/``stats`` are
  denied, the ``(DESC`` arm spans comments, and an un-denied
  autoloadable (``::INET``) is proven FAILED+audited at bind).
* Generalized ``do_put`` (engine level): create_or_replace|append|
  replace; PK enforced (dup keys fail loudly); schema mismatch ->
  REJECTED.
"""

from __future__ import annotations

import time
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
            # --- F26-T2: every challenger-verified exfil vector -----
            # file/remote-read functions (all verified live to
            # materialize arbitrary server files pre-fix)
            "SELECT * FROM read_text('C:/tmp/x.txt')",
            "SELECT * FROM read_blob('/etc/passwd')",
            "SELECT * FROM glob('C:/data/*')",
            "SELECT * FROM read_json('x.json')",
            "SELECT * FROM read_json_objects('x.json')",
            "SELECT * FROM read_ndjson('x.ndjson')",
            "SELECT * FROM read_duckdb('other.duckdb')",
            "SELECT * FROM csv_sniffer('x.csv')",
            "SELECT * FROM sniff_csv('x.csv')",
            "SELECT * FROM parquet_metadata('x.parquet')",
            "SELECT * FROM parquet_schema('x.parquet')",
            "SELECT * FROM parquet_file_metadata('x')",
            "SELECT * FROM parquet_kv_metadata('x')",
            "SELECT * FROM sqlite_query('x.db','SELECT 1')",
            "SELECT * FROM sqlite_attach('x.db')",
            "SELECT * FROM delta_scan('d/')",
            "SELECT * FROM iceberg_scan('i/')",
            "SELECT * FROM st_read('x.shp')",
            "SELECT * FROM postgres_scan('h','u','t')",
            "SELECT * FROM query('SELECT 1')",
            "SELECT * FROM query_table('t')",
            # replacement-scan shorthands (verified live: file
            # contents materialized; httpfs autoload + HTTP egress)
            "SELECT * FROM 'C:/data/x.csv'",
            'SELECT * FROM "C:/data/x.csv"',
            "SELECT * FROM 'x.csv'",
            "SELECT * FROM 'a/b/c'",
            "SELECT * FROM 'https://evil.example/x.csv'",
            "SELECT * FROM 's3://bucket/key'",
            "SELECT * FROM pivot.t, 'x.csv'",
            'SELECT * FROM pivot.t, "x.csv"',
            # introspection leaks (verified: duckdb_secrets() leaked
            # key metadata; pragma_* slipped the \bpragma\b boundary)
            "SELECT * FROM pragma_database_size()",
            "SELECT * FROM pragma_table_info('daily_close')",
            "SELECT * FROM duckdb_secrets()",
            "SELECT * FROM duckdb_settings()",
            "SELECT * FROM duckdb_databases()",
            "SELECT current_setting('threads')",
            "SELECT * FROM which_secret('s','t')",
            "SELECT setvariable('v', 1), getvariable('v')",
            # session/catalog side effects
            "SELECT * FROM force_checkpoint()",
            "SELECT * FROM checkpoint()",
            "SELECT nextval('s'), setval('s',1), currval('s')",
            "SELECT setseed(0.5)",
            "SELECT * FROM copy_database('a','b')",
            "SELECT pg_sleep(60)",
            "SELECT sleep_ms(500)",
            # serialized-plan execution bypass
            "SELECT json_execute_serialized_sql('{}')",
            "SELECT json_serialize_sql('SELECT 1')",
            # --- F26-T3: generator/side-effect family --------------
            # (challenger-verified: dbgen wrote 8 tpch tables into
            # main via the CTAS fallback, create_view was a latent
            # write bomb, dsdgen SEGFAULTED the server — these are
            # asserted REJECTED at validation, NEVER executed)
            "SELECT * FROM dbgen(sf=0.001)",
            "SELECT * FROM dbgen()",
            "SELECT * FROM dsdgen(sf=0.001)",
            "SELECT * FROM tpch_queries()",
            "SELECT * FROM tpch_answers()",
            "SELECT * FROM tpcds_queries()",
            "SELECT * FROM tpcds_answers()",
            "SELECT * FROM enable_profiling()",
            "SELECT * FROM disable_profiling()",
            "SELECT * FROM enable_logging()",
            "SELECT * FROM disable_optimizer()",
            "SELECT * FROM enable_checkpoint_on_shutdown()",
            "SELECT * FROM verify_parallelism()",
            "SELECT * FROM add_parquet_key('k','v')",
            "SELECT import_database('d')",
            "SELECT * FROM export_database('d')",
            "SELECT * FROM load_aws_credentials('p')",
            "SELECT * FROM aws_load_something()",
            "SELECT * FROM azure_transport_option()",
            # statement-forms usable as table refs (verified live:
            # DESCRIBE/DESC/SUMMARIZE/SHOW TABLES all execute inside
            # FROM (...); desc is position-scoped so ORDER BY ... DESC
            # stays legal)
            "SELECT * FROM (DESCRIBE t)",
            "SELECT * FROM (DESC t)",
            "SELECT * FROM (SUMMARIZE t)",
            "SELECT * FROM (SHOW TABLES)",
            "SELECT * FROM (SHOW ALL TABLES)",
            "SELECT * FROM (EXPLAIN SELECT 1)",
            "SELECT * FROM show_tables()",
            "SELECT * FROM show_databases()",
            # bundled autoloadable-extension families (calling one
            # triggers extension auto-install = egress + native code)
            "SELECT * FROM start_ui()",
            "SELECT * FROM start_ui_server()",
            "SELECT * FROM start_quack_server()",
            "SELECT * FROM quack_x()",
            "SELECT * FROM vortex_scan('x')",
            "SELECT * FROM lance_scan('x')",
            "SELECT * FROM ducklake_flush_inlined_data('x')",
            "SELECT * FROM vss_match('a','b')",
            "SELECT * FROM delta_history('x')",
            "SELECT * FROM iceberg_to_ducklake('x')",
            "SELECT * FROM st_layers('x.shp')",
            "SELECT * FROM st_read_meta('x.shp')",
            # --- F26-T3: comment gap in the table-literal bridge -----
            "SELECT * FROM /* c */ 'x.csv'",
            "SELECT * FROM -- c\n 'x.csv'",
            "SELECT * FROM/**/'x.csv'",
            'SELECT * FROM t, /* c */ "a/b.csv"',
            # extension-less single-quoted literal in table position —
            # isolates the comment-gap fix (no path-literal fallback)
            "SELECT * FROM /* c */ 'relpath'",
            "SELECT * FROM 'x--y.csv'",
            # documented false-positive cost: a string *value* that
            # looks like a data file is rejected too
            "SELECT * FROM t WHERE note = 'report.csv'",
            # --- F26-T4: extension entrypoints + built-in side-effect
            # functions (challenger-verified bypasses — autoload is now
            # killed at the platform layer too, see db.py) -----------
            # autocomplete / fts / inet / excel entrypoints
            "SELECT * FROM sql_auto_complete('SEL')",
            "SELECT check_peg_parser('x')",
            "SELECT stem('running','english')",
            "SELECT create_fts_index('t','c')",
            "SELECT drop_fts_index('t')",
            "SELECT host('127.0.0.1')",
            "SELECT broadcast('127.0.0.1')",
            "SELECT netmask('127.0.0.1/24')",
            "SELECT network('127.0.0.1/24')",
            "SELECT family('127.0.0.1')",
            "SELECT html_escape('<a>')",
            "SELECT html_unescape('&lt;')",
            "SELECT excel_text('x')",
            "SELECT text('x')",
            "SELECT * FROM read_xlsx('x')",
            "SELECT * FROM read_avro('x')",
            # built-in side-effect functions (core, not ext-gated)
            "SELECT * FROM truncate_duckdb_logs()",
            "SELECT write_log('msg')",
            "SELECT error('msg')",
            "SELECT stats(1)",
            # comment-separated call forms
            "SELECT host /*c*/ ('127.0.0.1')",
            "SELECT error -- c\n ('x')",
            # B2: desc positional arm comment gap
            "SELECT * FROM (/*c*/ DESC client_dml_audit)",
            "SELECT * FROM ( /*a*/ /*b*/ DESC t)",
            "SELECT * FROM (-- c\n DESC t)",
        ],
    )
    def test_denied_sources_rejected(self, sql):
        with pytest.raises(client_dml.DmlRejected):
            client_dml.validate_source_sql(sql)

    def test_legit_literals_and_qualified_idents_accepted(self):
        """Date/string literals and quoted qualified names must NOT
        trip the literal denylist."""
        for sql in [
            "SELECT Ticker, Close FROM daily_close "
            "WHERE Date >= '2026-01-01' ORDER BY 1",
            "SELECT a, 'hello world' FROM t",
            "SELECT a, 'and/or' FROM t",
            'SELECT * FROM "pivot"."t" JOIN "pivot"."u" '
            'ON "t"."a" = "u"."a"',
            "SELECT a FROM t WHERE d = '2026/01/02'",
            # F26-T3: ORDER BY DESC must survive — the `desc` deny arm
            # is position-scoped to `(DESC ` only.
            "SELECT a FROM t ORDER BY a DESC",
            "SELECT a FROM t ORDER BY a desc, b ASC",
            # F26-T4: generic-word entrypoints are denied in call-form
            # only — legit column refs and TEXT casts must survive.
            "SELECT host, network, family, stats, error, broadcast "
            "FROM t",
            "SELECT CAST(1 AS TEXT)",
            "SELECT 1::TEXT AS t",
            # Read-only statement form (extracts as one SELECT, no
            # side effects) — documented as still allowed.
            "SELECT * FROM (UNPIVOT t ON a INTO NAME n VALUE v)",
        ]:
            assert client_dml.validate_source_sql(sql)

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


class TestExfilVectorsRejected:
    """F26-T2 — every vector the challenger verified live as an
    exfil-via-SELECT hole must now be REJECTED + audited on ALL
    source-SQL ops (create_as / create_view / insert_select share the
    one ``validate_source_sql`` chokepoint)."""

    EXFIL_SQL = [
        "SELECT * FROM read_text('C:/tmp/x.txt')",
        "SELECT * FROM read_blob('/etc/passwd')",
        "SELECT * FROM glob('C:/data/*')",
        "SELECT * FROM read_json('x.json')",
        "SELECT * FROM 'C:/data/x.csv'",
        "SELECT * FROM 'https://evil.example/x.csv'",
        "SELECT * FROM pragma_database_size()",
        "SELECT * FROM duckdb_secrets()",
        "SELECT * FROM csv_sniffer('x.csv')",
        "SELECT * FROM sqlite_attach('x.db')",
        "SELECT * FROM force_checkpoint()",
        "SELECT nextval('s')",
    ]

    @pytest.mark.parametrize("sql", EXFIL_SQL)
    def test_create_as_rejects_and_audits(self, env, sql):
        _, eng = env
        setup_namespace(env)
        result = eng.run_dml_admin(
            {"op": "create_as", "table": "pivot.t", "sql": sql}
        )
        assert result["status"] == "REJECTED"
        rows = audit_df(env)
        assert rows.iloc[-1]["status"] == "REJECTED"
        assert rows.iloc[-1]["op"] == "create_as"
        # Nothing may have been materialized.
        db, _ = env
        assert client_dml.catalog_object_kind(db.conn, "pivot", "t") is None

    @pytest.mark.parametrize(
        "sql",
        [
            "SELECT * FROM read_text('C:/tmp/x.txt')",
            "SELECT * FROM 'x.csv'",
            "SELECT * FROM duckdb_secrets()",
        ],
    )
    def test_create_view_rejects_and_audits(self, env, sql):
        _, eng = env
        setup_namespace(env)
        result = eng.run_dml_admin(
            {"op": "create_view", "table": "pivot.v", "sql": sql}
        )
        assert result["status"] == "REJECTED"
        rows = audit_df(env)
        assert rows.iloc[-1]["status"] == "REJECTED"
        assert rows.iloc[-1]["op"] == "create_view"

    @pytest.mark.parametrize(
        "sql",
        [
            "SELECT * FROM read_text('C:/tmp/x.txt')",
            "SELECT * FROM 'x.csv'",
            "SELECT * FROM pragma_database_size()",
        ],
    )
    def test_insert_select_rejects_and_audits(self, env, sql):
        _, eng = env
        setup_week_calendar(env)
        result = eng.run_dml_admin(
            {
                "op": "insert_select",
                "table": "pivot.week_calendar",
                "mode": "append",
                "sql": sql,
            }
        )
        assert result["status"] == "REJECTED"
        rows = audit_df(env)
        assert rows.iloc[-1]["status"] == "REJECTED"
        assert rows.iloc[-1]["op"] == "insert_select"


class TestGeneratorFamilyRejected:
    """F26-T3 — the bundled generator/side-effect family and
    statement-form table refs are REJECTED + audited on ALL three
    source-SQL ops (create_as / create_view / insert_select share the
    one ``validate_source_sql`` chokepoint).

    SAFETY: ``dbgen`` writes the 8 TPC-H tables into ``main``
    (audited-SUCCESS confinement breach) and ``dsdgen`` SEGFAULTS the
    process (client-triggerable DoS).  Every vector is therefore FIRST
    asserted against ``validate_source_sql`` directly — if a future
    change stops rejecting, the test fails on that line and the
    dangerous SQL never reaches ``run_dml_admin``/execution.  None of
    these functions is ever invoked by this suite."""

    DENIED_SQL = [
        # generator/side-effect family (live-verified breach/crash)
        "SELECT * FROM dbgen(sf=0.001)",
        "SELECT * FROM dbgen()",
        "SELECT * FROM dsdgen(sf=0.001)",
        "SELECT * FROM tpch_queries()",
        "SELECT * FROM tpch_answers()",
        "SELECT * FROM tpcds_queries()",
        "SELECT * FROM tpcds_answers()",
        "SELECT * FROM enable_profiling()",
        "SELECT * FROM disable_profiling()",
        "SELECT * FROM enable_logging()",
        "SELECT * FROM disable_optimizer()",
        "SELECT * FROM verify_parallelism()",
        "SELECT * FROM add_parquet_key('k','v')",
        "SELECT import_database('d')",
        "SELECT * FROM export_database('d')",
        "SELECT * FROM load_aws_credentials('p')",
        # statement-forms usable as table refs
        "SELECT * FROM (DESCRIBE t)",
        "SELECT * FROM (DESC t)",
        "SELECT * FROM (SUMMARIZE t)",
        "SELECT * FROM (SHOW TABLES)",
        "SELECT * FROM (EXPLAIN SELECT 1)",
        "SELECT * FROM show_tables()",
        # bundled autoloadable-extension families
        "SELECT * FROM start_ui()",
        "SELECT * FROM start_quack_server()",
        "SELECT * FROM vortex_scan('x')",
        "SELECT * FROM lance_scan('x')",
        "SELECT * FROM ducklake_flush_inlined_data('x')",
        "SELECT * FROM vss_match('a','b')",
        "SELECT * FROM delta_history('x')",
        "SELECT * FROM st_layers('x.shp')",
        # comment-gap in the table-literal bridge
        "SELECT * FROM /* c */ 'x.csv'",
        "SELECT * FROM -- c\n 'x.csv'",
        "SELECT * FROM/* c */ 'relpath'",
        "SELECT * FROM 'x--y.csv'",
    ]

    #: Catalog objects dbgen/dsdgen would land in ``main`` if they ran.
    GENERATOR_OUTPUT_TABLES = (
        "customer", "lineitem", "nation", "orders", "part", "partsupp",
        "region", "supplier",          # tpch (dbgen)
        "call_center", "catalog_sales", "store_sales", "web_sales",
        "date_dim", "item", "store",   # tpcds (dsdgen)
    )

    def _assert_no_side_effects(self, env):
        """The generator must never have run: no TPC-H/TPC-DS tables in
        ``main`` and no pivot object materialized."""
        db, _ = env
        names = ", ".join(
            f"'{n}'" for n in self.GENERATOR_OUTPUT_TABLES
        )
        count = db.execute_query(
            "SELECT COUNT(*) FROM information_schema.tables "
            f"WHERE table_schema = 'main' AND table_name IN ({names})"
        ).to_pandas().iloc[0, 0]
        assert int(count) == 0
        assert client_dml.catalog_object_kind(db.conn, "pivot", "t") is None
        assert client_dml.catalog_object_kind(db.conn, "pivot", "v") is None

    @pytest.mark.parametrize("sql", DENIED_SQL)
    def test_create_as_rejects_and_audits(self, env, sql):
        # Guard FIRST: if validation ever stops rejecting this vector,
        # fail here — the dangerous SQL must never reach execution.
        with pytest.raises(client_dml.DmlRejected):
            client_dml.validate_source_sql(sql)
        _, eng = env
        setup_namespace(env)
        result = eng.run_dml_admin(
            {"op": "create_as", "table": "pivot.t", "sql": sql}
        )
        assert result["status"] == "REJECTED"
        rows = audit_df(env)
        assert rows.iloc[-1]["status"] == "REJECTED"
        assert rows.iloc[-1]["op"] == "create_as"
        self._assert_no_side_effects(env)

    @pytest.mark.parametrize("sql", DENIED_SQL)
    def test_create_view_rejects_and_audits(self, env, sql):
        # Same guard: a create_view on dbgen is a latent write bomb —
        # never let it be created.
        with pytest.raises(client_dml.DmlRejected):
            client_dml.validate_source_sql(sql)
        _, eng = env
        setup_namespace(env)
        result = eng.run_dml_admin(
            {"op": "create_view", "table": "pivot.v", "sql": sql}
        )
        assert result["status"] == "REJECTED"
        rows = audit_df(env)
        assert rows.iloc[-1]["status"] == "REJECTED"
        assert rows.iloc[-1]["op"] == "create_view"
        self._assert_no_side_effects(env)

    @pytest.mark.parametrize("sql", DENIED_SQL)
    def test_insert_select_rejects_and_audits(self, env, sql):
        with pytest.raises(client_dml.DmlRejected):
            client_dml.validate_source_sql(sql)
        _, eng = env
        setup_week_calendar(env)
        result = eng.run_dml_admin(
            {
                "op": "insert_select",
                "table": "pivot.week_calendar",
                "mode": "append",
                "sql": sql,
            }
        )
        assert result["status"] == "REJECTED"
        rows = audit_df(env)
        assert rows.iloc[-1]["status"] == "REJECTED"
        assert rows.iloc[-1]["op"] == "insert_select"
        # The registered target must be untouched and empty.
        db, _ = env
        n = db.execute_query(
            'SELECT COUNT(*) FROM "pivot"."week_calendar"'
        ).to_pandas().iloc[0, 0]
        assert int(n) == 0
        self._assert_no_side_effects(env)


class TestAutoloadKillSwitch:
    """F26-T4 PRIMARY FIX — ``DatabaseManager`` applies
    ``SET autoinstall_known_extensions=false`` +
    ``SET autoload_known_extensions=false`` at open.  Both settings are
    GLOBAL scope on the DB instance, so every cursor/read-connection
    (the engine's execution path AND ``do_get``'s read path) inherits
    them: any uninstalled extension entrypoint dies at bind with a
    CatalogException ("exists in the X extension") — it can never
    INSTALL (network egress + remote native code).  Verified live on
    duckdb 1.5.5.  Unlike ``enable_external_access`` this is not a
    one-way switch.  No server path uses an extension function (r2 =
    boto3, Excel = pandas/openpyxl, weekly/signal/fear_greed =
    built-ins + requests), so no explicit LOAD is issued."""

    def test_settings_applied_at_open(self, env):
        """Startup unaffected: the manager opened and both kill-switches
        read back false on the write connection."""
        db, _ = env
        for setting in (
            "autoinstall_known_extensions",
            "autoload_known_extensions",
        ):
            value = db.conn.execute(
                f"SELECT current_setting('{setting}')"
            ).fetchone()[0]
            assert value is False

    @pytest.mark.parametrize(
        "sql",
        [
            "SELECT stem('a','english')",
            "SELECT * FROM sql_auto_complete('SEL')",
            "SELECT host('127.0.0.1')",
            "SELECT broadcast('127.0.0.1')",
            "SELECT '1.2.3.4'::INET",
        ],
    )
    def test_cursor_bind_fails_no_autoload(self, env, sql):
        """A cursor on the write conn (what run_dml_admin executes on)
        inherits the GLOBAL kill — each call raises a CatalogException
        naming the extension instead of installing/loading it."""
        db, _ = env
        cur = db.conn.cursor()
        try:
            with pytest.raises(Exception) as excinfo:
                cur.execute(sql)
            assert "extension" in str(excinfo.value).lower()
        finally:
            cur.close()

    @pytest.mark.parametrize(
        "sql",
        [
            "SELECT stem('a','english')",
            "SELECT * FROM sql_auto_complete('SEL')",
            "SELECT host('127.0.0.1')",
        ],
    )
    def test_do_get_path_killed_too(self, env, sql):
        """``execute_query`` is the do_get read path — the settings are
        instance-global, so the identical pre-existing autoload hole in
        do_get is closed as well."""
        db, _ = env
        with pytest.raises(Exception) as excinfo:
            db.execute_query(sql)
        assert "extension" in str(excinfo.value).lower()

    def test_legit_reads_unaffected(self, env):
        """Plain SELECTs — on the write conn, a cursor and the do_get
        path — are untouched by the kill-switch."""
        db, _ = env
        assert db.conn.execute("SELECT 42").fetchone()[0] == 42
        assert db.conn.execute("SELECT CAST(1 AS TEXT)").fetchone()[0] == "1"
        cur = db.conn.cursor()
        try:
            assert cur.execute("SELECT 1 + 1").fetchone()[0] == 2
        finally:
            cur.close()
        t = db.execute_query("SELECT COUNT(*) AS n FROM daily_close")
        assert int(t.to_pandas().iloc[0, 0]) == 0

    def test_daily_write_path_smoke(self, env):
        """The daily-path builtins still work under the kill-switch:
        upsert + the weekly rebuilds (arg_min/date_trunc/window/LN —
        no extension functions)."""
        import pandas as pd

        db, _ = env
        n = db.upsert_dataframe(
            pd.DataFrame(
                {
                    "Date": pd.to_datetime(["2026-01-02", "2026-01-05"]),
                    "Ticker": ["AAA", "AAA"],
                    "Open": [1.0, 2.0],
                    "High": [1.0, 2.0],
                    "Low": [1.0, 2.0],
                    "Close": [1.0, 2.0],
                    "Volume": [100.0, 200.0],
                }
            )
        )
        assert n == 2
        assert db.rebuild_weekly_close() == 2
        assert db.rebuild_weekly_indicators() == 2

    def test_engine_bind_failure_is_failed_not_success(self, env):
        """Belt vs kill-switch: an autoloadable TYPE the denylist does
        not cover (``::INET`` cast — ``inet`` is deliberately not a
        denied word) still passes validation but dies at bind → the op
        is FAILED + audited, never SUCCESS, nothing materialized."""
        db, eng = env
        sql = "SELECT '1.2.3.4'::INET AS x"
        # Validation genuinely lets this through — the fix under test
        # is the bind-time kill, not a denylist hit.
        assert client_dml.validate_source_sql(sql) == sql
        setup_namespace(env)
        result = eng.run_dml_admin(
            {"op": "create_view", "table": "pivot.v", "sql": sql}
        )
        assert result["status"] == "FAILED"
        assert "extension" in result["error"].lower()
        rows = audit_df(env)
        assert rows.iloc[-1]["status"] == "FAILED"
        assert rows.iloc[-1]["op"] == "create_view"
        assert client_dml.catalog_object_kind(db.conn, "pivot", "v") is None


class TestExtensionEntrypointsRejected:
    """F26-T4 BELT — the autoloadable-extension entrypoints the
    challenger demonstrated live (autocomplete/fts/inet/excel + the
    built-in log/error/stats functions) are REJECTED + audited on all
    three source-SQL ops.

    SAFETY: every vector is FIRST asserted against
    ``validate_source_sql`` — if a future change stops rejecting, the
    test fails there and the call never reaches execution.  (With the
    F26-T4 kill-switch these calls cannot install anything anyway, but
    the guard keeps the test honest and non-vacuous.)"""

    DENIED_SQL = [
        # autocomplete / fts / inet / excel entrypoints (enumerated on
        # duckdb 1.5.5 via LOAD-diff of duckdb_functions())
        "SELECT * FROM sql_auto_complete('SEL')",
        "SELECT check_peg_parser('x')",
        "SELECT stem('running','english')",
        "SELECT create_fts_index('t','c')",
        "SELECT drop_fts_index('t')",
        "SELECT host('127.0.0.1')",
        "SELECT broadcast('127.0.0.1')",
        "SELECT netmask('127.0.0.1/24')",
        "SELECT network('127.0.0.1/24')",
        "SELECT family('127.0.0.1')",
        "SELECT html_escape('<a>')",
        "SELECT html_unescape('&lt;')",
        "SELECT excel_text('x')",
        "SELECT text('x')",
        "SELECT * FROM read_xlsx('x')",
        "SELECT * FROM read_avro('x')",
        # built-in side-effect functions (core — NOT extension-gated)
        "SELECT * FROM truncate_duckdb_logs()",
        "SELECT write_log('msg')",
        "SELECT error('msg')",
        "SELECT stats(1)",
        # comment-separated call forms
        "SELECT host /*c*/ ('127.0.0.1')",
        "SELECT error -- c\n ('x')",
        # B2: desc positional arm comment gap
        "SELECT * FROM (/*c*/ DESC client_dml_audit)",
        "SELECT * FROM ( /*a*/ /*b*/ DESC t)",
        "SELECT * FROM (-- c\n DESC t)",
    ]

    #: Extensions that may legitimately be loaded — the statically
    #: linked set present before any op.  The kill-switch plus the
    #: denylist must keep this set unchanged.
    PRELOADED_EXTS = {"core_functions", "icu", "json", "parquet"}

    def _assert_no_side_effects(self, env):
        """No extension got installed/loaded by the attempted calls and
        no pivot object materialized."""
        db, _ = env
        loaded = {
            r[0]
            for r in db.conn.execute(
                "SELECT extension_name FROM duckdb_extensions() "
                "WHERE loaded"
            ).fetchall()
        }
        assert loaded <= self.PRELOADED_EXTS
        assert client_dml.catalog_object_kind(db.conn, "pivot", "t") is None
        assert client_dml.catalog_object_kind(db.conn, "pivot", "v") is None

    @pytest.mark.parametrize("sql", DENIED_SQL)
    def test_create_as_rejects_and_audits(self, env, sql):
        # Guard FIRST: a validation miss fails here — the vector never
        # reaches run_dml_admin.
        with pytest.raises(client_dml.DmlRejected):
            client_dml.validate_source_sql(sql)
        _, eng = env
        setup_namespace(env)
        result = eng.run_dml_admin(
            {"op": "create_as", "table": "pivot.t", "sql": sql}
        )
        assert result["status"] == "REJECTED"
        rows = audit_df(env)
        assert rows.iloc[-1]["status"] == "REJECTED"
        assert rows.iloc[-1]["op"] == "create_as"
        self._assert_no_side_effects(env)

    @pytest.mark.parametrize("sql", DENIED_SQL)
    def test_create_view_rejects_and_audits(self, env, sql):
        with pytest.raises(client_dml.DmlRejected):
            client_dml.validate_source_sql(sql)
        _, eng = env
        setup_namespace(env)
        result = eng.run_dml_admin(
            {"op": "create_view", "table": "pivot.v", "sql": sql}
        )
        assert result["status"] == "REJECTED"
        rows = audit_df(env)
        assert rows.iloc[-1]["status"] == "REJECTED"
        assert rows.iloc[-1]["op"] == "create_view"
        self._assert_no_side_effects(env)

    @pytest.mark.parametrize("sql", DENIED_SQL)
    def test_insert_select_rejects_and_audits(self, env, sql):
        with pytest.raises(client_dml.DmlRejected):
            client_dml.validate_source_sql(sql)
        _, eng = env
        setup_week_calendar(env)
        result = eng.run_dml_admin(
            {
                "op": "insert_select",
                "table": "pivot.week_calendar",
                "mode": "append",
                "sql": sql,
            }
        )
        assert result["status"] == "REJECTED"
        rows = audit_df(env)
        assert rows.iloc[-1]["status"] == "REJECTED"
        assert rows.iloc[-1]["op"] == "insert_select"
        db, _ = env
        n = db.execute_query(
            'SELECT COUNT(*) FROM "pivot"."week_calendar"'
        ).to_pandas().iloc[0, 0]
        assert int(n) == 0
        self._assert_no_side_effects(env)


class TestCreateAsTimeout:
    """F26-T2/F3 — the create_as snapshot read is bounded by
    ``dml_timeout_seconds`` (worker + interrupt), not run synchronously
    outside any deadline."""

    def test_heavy_snapshot_read_times_out(self, tmp_path):
        db = DatabaseManager(str(tmp_path / "t.duckdb"))
        cfg = make_cfg(dml_timeout_seconds=0.5)
        eng = IngestionEngine(db, cfg)
        try:
            eng.run_dml_admin(
                {"op": "register_namespace", "namespace": "pivot"}
            )
            # sum() forces a real multi-billion-row scan (verified
            # interruptible on duckdb 1.5.5: ~4s+ of work, killed by
            # interrupt() in <0.1s).
            start = time.time()
            result = eng.run_dml_admin(
                {
                    "op": "create_as",
                    "table": "pivot.t",
                    "sql": "SELECT sum(i) AS s FROM "
                           "range(5000000000) t(i)",
                }
            )
            elapsed = time.time() - start
            assert result["status"] == "FAILED"
            assert (
                "timeout" in result["error"].lower()
                or "interrupt" in result["error"].lower()
            )
            assert elapsed < 20  # unbounded pre-fix; ~0.5s now
            rows = (
                db.execute_query(
                    "SELECT status, error FROM client_dml_audit "
                    "ORDER BY audit_id"
                )
                .to_pandas()
            )
            assert rows.iloc[-1]["status"] == "FAILED"
            # A timed-out snapshot must NOT degrade to inline CTAS —
            # no table may land after the FAILED audit.
            assert db.get_registered_table("pivot", "t") is None
            assert (
                client_dml.catalog_object_kind(db.conn, "pivot", "t")
                is None
            )
        finally:
            db.close()

    def test_fast_snapshot_still_succeeds(self, env):
        """The bounded path doesn't break the normal Arrow snapshot."""
        _, eng = env
        setup_namespace(env)
        result = eng.run_dml_admin(
            {
                "op": "create_as",
                "table": "pivot.t",
                "sql": "SELECT 1 AS a",
            }
        )
        assert result["status"] == "SUCCESS"
        assert result["materialized_via"] == "arrow_snapshot"
        assert result["rows_written"] == 1


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
