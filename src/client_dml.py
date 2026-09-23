"""Validation + DDL composition for client-issued DML (F26-T1).

Implements the negotiated ``pivot``-namespace contract from
``docs/tech_spec/survey_reply_from_bollinger_pm.txt`` /
``survey_reply3_from_bollinger_pm.md`` per the F21 study
(``docs/client_dml_sql_study.md``): trusted local clients materialize
derived tables inside allowlisted schemas of the canonical DuckDB file,
exclusively through the Flight service (the server holds the file lock;
there is no bypass channel).

Every enforcement boundary in this module is a regex or a closed enum —
never a SQL parser:

* namespace/table names must match ``^[a-z][a-z0-9_]*$`` and the server
  composes the fully-qualified ``"ns"."table"`` identifier — a client
  can never spell ``main.daily_close`` into a write path.  NOTE: all
  composed identifiers are double-quoted because ``pivot`` is a DuckDB
  reserved word (``CREATE SCHEMA pivot`` is a ParserException;
  ``"pivot"`` works).
* ``create_table`` takes a *structured* column spec
  ``[{name, type, not_null?, pk?}]`` — never SQL text — and the DDL is
  composed server-side against a type whitelist.
* ``create_as``/``create_view``/``insert_select`` take a *source* SELECT
  that must pass :func:`validate_source_sql`: SELECT/WITH prefix +
  keyword/function denylist + replacement-scan literal denylist +
  ``duckdb.extract_statements`` single-statement SELECT-type check.
  The denylist blocks the file-read/remote-read/introspection/
  side-effect families enumerated in its comment — including the
  ``FROM '<path>'``/``FROM 'https://…'`` replacement-scan shorthands —
  but cannot prove total closure (documented residual gaps); it is
  defense-in-depth on a trusted-client write surface, not a sandbox.
* Op and mode enums are closed sets — anything outside them is a
  :class:`DmlRejected`, which the engine audits with status REJECTED.

Platform facts relied on (all verified live on DuckDB 1.5.5 by the
F21 study — do not re-probe): ``extract_statements`` classifies
``WITH ... INSERT`` as INSERT and ``PRAGMA`` as SELECT (the denylist
catches the latter); there is no GRANT so enforcement is entirely
application-layer; PK/NOT NULL are enforced at INSERT time.
"""

from __future__ import annotations

import hashlib
import re
from typing import Any, Dict, List, Optional, Tuple

import duckdb


class DmlRejected(ValueError):
    """Raised when a client-DML request fails validation.

    The engine catches this, writes a ``client_dml_audit`` row with
    ``status='REJECTED'`` and returns ``{"status": "REJECTED", ...}``
    (or, for ``do_put``, re-raises so the Flight layer can convert it
    to a transport error).  Distinct from a plain ``ValueError`` so a
    validation rejection can never be confused with an execution
    failure (FAILED).
    """


# ---------------------------------------------------------------------
# Closed enums
# ---------------------------------------------------------------------

#: Closed op enum for the ``dml_admin`` Flight action.
DML_OPS = frozenset(
    {
        "register_namespace",
        "create_table",
        "create_as",
        "create_view",
        "insert_select",
        "drop_table",
        "list_tables",
        "pin",
        "unpin",
        "dml_sweep",
    }
)

#: ``insert_select`` / generalized-``do_put`` mode enums.
INSERT_SELECT_MODES = frozenset({"append", "replace"})
DO_PUT_MODES = frozenset({"create_or_replace", "append", "replace"})

#: Type whitelist for the structured ``create_table`` column spec.
#: Covers every type the Bollinger PM's canonical DDL uses
#: (DATE/INTEGER/DOUBLE/VARCHAR/BOOLEAN/TIMESTAMP) plus BIGINT and the
#: small ints used by their id/counter columns.
ALLOWED_COLUMN_TYPES = frozenset(
    {
        "DATE",
        "INTEGER",
        "BIGINT",
        "DOUBLE",
        "VARCHAR",
        "BOOLEAN",
        "TIMESTAMP",
        "UTINYINT",
        "SMALLINT",
    }
)

# ---------------------------------------------------------------------
# Identifier rules
# ---------------------------------------------------------------------

# Namespace/table names: strict lowercase slug.  This is the confinement
# boundary — a name that cannot be expressed cannot escape the
# allowlisted schema.
NAME_RE = re.compile(r"^[a-z][a-z0-9_]*$")

# Column names inside a ``create_table`` spec: mixed case allowed
# (``Ticker``/``Date``/``SplitDate`` appear verbatim in the Bollinger
# PM's canonical DDL and their store.py depends on those names).  Still
# a safe quoted identifier — no dots, quotes, or punctuation.
COLUMN_NAME_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9_]*$")


def validate_name(value: Any, kind: str = "name") -> str:
    """Validate a namespace/table name against :data:`NAME_RE`.

    Returns the name unchanged.  Raises :class:`DmlRejected` for a
    non-string or non-matching value.
    """
    if not isinstance(value, str) or not NAME_RE.fullmatch(value):
        raise DmlRejected(
            f"{kind} must match ^[a-z][a-z0-9_]*$, got {value!r}"
        )
    return value


def quote_ident(name: str) -> str:
    """Double-quote a pre-validated identifier for SQL composition.

    Only call with names that already passed :func:`validate_name` or
    :data:`COLUMN_NAME_RE` — the regex guarantees no embedded quote can
    appear, so quoting is a pure escape.
    """
    return '"' + name + '"'


def qualified_name(namespace: str, table: str) -> str:
    """Compose the fully-qualified ``"ns"."table"`` identifier.

    Quoting is mandatory, not cosmetic: ``pivot`` is a DuckDB reserved
    word, so ``pivot.t`` does not parse while ``"pivot"."t"`` does.
    """
    return f"{quote_ident(namespace)}.{quote_ident(table)}"


def parse_table_ref(
    payload: Dict[str, Any], allowed_namespaces
) -> Tuple[str, str]:
    """Resolve ``(namespace, table)`` from a request payload.

    Accepts either ``{"table": "ns.name"}`` or
    ``{"table": "name", "namespace": "ns"}``.  When both are given they
    must agree.  Both parts must match :data:`NAME_RE` and the
    namespace must be in ``allowed_namespaces`` — this is the
    confinement check that keeps client writes out of ``main``.
    """
    if not isinstance(payload, dict):
        raise DmlRejected("payload must be a JSON object")
    table = payload.get("table")
    if not isinstance(table, str) or not table:
        raise DmlRejected(f"table must be a non-empty string, got {table!r}")
    namespace = payload.get("namespace")
    if "." in table:
        parts = table.split(".")
        if len(parts) != 2 or not all(parts):
            raise DmlRejected(
                f"table must be 'name' or 'ns.name', got {table!r}"
            )
        dotted_ns, name = parts
        if namespace is not None and namespace != dotted_ns:
            raise DmlRejected(
                f"namespace {namespace!r} conflicts with table {table!r}"
            )
        namespace = dotted_ns
    else:
        name = table
    validate_name(name, "table")
    if namespace is None:
        raise DmlRejected(
            "namespace is required (table 'ns.name' or a 'namespace' "
            "field) — unqualified writes are not allowed"
        )
    validate_name(namespace, "namespace")
    allowed = set(allowed_namespaces or [])
    if namespace not in allowed:
        raise DmlRejected(
            f"namespace {namespace!r} is not in client_dml."
            f"allowed_namespaces {sorted(allowed)}"
        )
    return namespace, name


# ---------------------------------------------------------------------
# Source-SQL validation (create_as / create_view / insert_select)
# ---------------------------------------------------------------------

# Source-SQL denylist — applied to the raw statement text.  F26-T1,
# hardened in F26-T2 after the challenger review demonstrated live that
# the original list did NOT close the exfil-via-SELECT path it claimed
# to close (read_text/read_blob/glob/read_json materialized arbitrary
# server files into pivot objects; ``FROM '<path>'`` and
# ``FROM 'https://...'`` replacement-scan shorthands worked, the latter
# autoloading httpfs = network egress/SSRF; ``pragma_*``/``duckdb_*``
# table functions slipped past the ``\bpragma\b`` boundary and
# ``duckdb_secrets()`` leaked key metadata).
#
# WHAT IS ENFORCED NOW (all verified live on duckdb 1.5.5):
#   * file/remote-read functions — the ``read_*``, ``parquet_*``,
#     ``sqlite_*``, ``st_read*`` prefix families plus ``glob``,
#     ``sniff_csv``/``csv_sniffer``, ``delta_scan``/``iceberg_scan``
#     and the remote-DB scanners ``postgres_*``/``mysql_*``/``odbc_*``;
#   * replacement-scan file/URL literals — a quoted token directly in
#     table position (``FROM '<x>'``, ``JOIN '<x>'``, and
#     double-quoted/after-comma tokens carrying path characters), plus
#     ``'…://…'`` URL literals, absolute/relative/UNC/home path
#     literals, and literals ending in a known data-file extension
#     ANYWHERE in the text;
#   * catalog/introspection leaks — ``pragma*`` (prefix: the old
#     ``\bpragma\b`` boundary missed ``pragma_*``), ``duckdb_*``,
#     ``current_setting``, ``which_secret``,
#     ``setvariable``/``getvariable``;
#   * session/catalog side effects — ``checkpoint``/``force_checkpoint``,
#     ``nextval``/``setval``/``currval``, ``setseed``, ``copy_database``,
#     ``pg_sleep``/``sleep_ms``, the ``query``/``query_table``
#     SQL-in-SQL functions and the ``json_*serialize*_sql``
#     serialized-plan execution helpers;
#   * plus the original mutation/session verbs (INSERT/UPDATE/DELETE/
#     DROP/CREATE/ALTER/MERGE, ATTACH/DETACH/LOAD/INSTALL, SECRET,
#     SET/PRAGMA/CALL/PREPARE/EXECUTE, COPY).
#
# WHY NOT THE PLATFORM SETTING (probed on duckdb 1.5.5 — do not
# re-probe): ``enable_external_access``, ``allowed_directories`` and
# ``allowed_paths`` are all GLOBAL scope — a ``SET`` on a dml cursor
# leaks to the main connection and ``do_get``; ``SET LOCAL`` is
# unimplemented; ``RESET enable_external_access`` is refused while the
# database is running (a one-way switch — setting it once would
# permanently break the server's own httpfs/R2 access until restart);
# and a second ``duckdb.connect`` to the same file cannot carry a
# different configuration, so a per-cursor restricted context is
# impossible.  The denylist is therefore the enforcement layer.
#
# KNOWN RESIDUAL GAPS (accepted + documented): a regex denylist cannot
# prove total closure — file readers introduced by future DuckDB
# versions/extensions and not in these families, or a relative-path
# literal deeper in a FROM list with no separator/extension (e.g.
# ``FROM t, 'relpath'`` — verified on 1.5.5 that extension-less tokens
# resolve as *table names*, not files, so this is narrow), can still
# slip through.  The contract already assumes *trusted* local clients —
# this is defense-in-depth on a WRITE surface, not a sandbox against
# hostile SQL.  False positives inside string literals/comments/values
# (e.g. a ``'report.csv'`` string value or a column named ``query``)
# are an acceptable, documented cost of a closed API.
_DENIED_KEYWORDS_RE = re.compile(
    r"\b(?:copy|attach|detach|load|install|set|call|prepare|execute|"
    r"secret|create|insert|update|delete|drop|alter|merge|"
    r"glob|sniff_csv|csv_sniffer|query|query_table|"
    r"delta_scan|delta_metadata|"
    r"iceberg_scan|iceberg_metadata|iceberg_snapshots|"
    r"which_secret|copy_database|current_setting|"
    r"checkpoint|force_checkpoint|nextval|setval|currval|setseed|"
    r"setvariable|getvariable|pg_sleep|sleep_ms|st_drivers|"
    r"json_serialize_sql|json_deserialize_sql|"
    r"json_execute_serialized_sql|json_serialize_plan)\b"
    r"|\bread_\w+"
    r"|\bparquet_\w+"
    r"|\bpragma\w*"
    r"|\bduckdb_\w+"
    r"|\bsqlite_\w+"
    r"|\bst_read\w*"
    r"|\bpostgres_\w+|\bmysql_\w+|\bodbc_\w+",
    re.IGNORECASE,
)

# Quoted literals in table position.  A single-quoted token right after
# FROM/JOIN is ALWAYS a file/URL reference (single quotes are never
# identifiers); a double-quoted token is an identifier UNLESS it
# carries path characters or a dot — every server-creatable name
# matches ``^[a-z][a-z0-9_]*$`` so no legitimate qualified reference
# contains them.  The ``,`` arm covers ``FROM t, "x.csv"`` — the same
# path characters that mark it non-identifier keep false positives to
# pathological identifier names only.
_DENIED_TABLE_LITERAL_RE = re.compile(
    r"\b(?:from|join)\s*'"
    r"|(?:\b(?:from|join)\s*|,\s*)\"[^\"]*[\\/:*?\[\].]",
    re.IGNORECASE,
)

# URL / path / data-file-extension literals ANYWHERE in the text —
# they also appear as arguments to readers we may have missed, or
# deeper in a FROM list (``FROM t, 'x.csv'``).
_DENIED_PATH_LITERAL_RE = re.compile(
    r"'[^']*://"
    r"|'(?:[a-zA-Z]:[\\/]|\.{1,2}[\\/]|~[\\/]|[\\/]{1,2})"
    r"|'[^']*\.(?:csv|tsv|parquet|jsonl?|ndjson|duckdb|db|sqlite3?|"
    r"xlsx?|txt|avro|arrow|feather|ipc|orc|geojson|shp|xml|ya?ml|"
    r"gz|zst|zip|7z|tar|bson|sas7bdat|xpt)'",
    re.IGNORECASE,
)


def validate_source_sql(sql: Any) -> str:
    """Validate a *source* SELECT for create_as/create_view/insert_select.

    Five independent layers (belt + suspenders, per the study):

    1. SELECT/WITH prefix on the stripped text (same as ``do_get``).
    2. Keyword/function denylist on the raw text — mutation/session
       verbs, file/remote-read families, introspection and side-effect
       names (see module comment).
    3. Replacement-scan literal denylist — quoted tokens in table
       position (``FROM '<path>'`` / ``JOIN 'https://…'`` and
       path-bearing double-quoted tokens) plus URL/path/data-file
       literals anywhere in the text.
    4. ``duckdb.extract_statements`` — exactly one statement whose type
       is ``StatementType.SELECT`` (a ``WITH ... INSERT`` classifies as
       INSERT and is rejected even though it passes the prefix check).

    Returns the stripped SQL (trailing ``;`` removed) on success.
    """
    if not isinstance(sql, str) or not sql.strip():
        raise DmlRejected(f"sql must be a non-empty string, got {sql!r}")
    stripped = sql.strip()
    upper = stripped.upper()
    if not (upper.startswith("SELECT") or upper.startswith("WITH")):
        raise DmlRejected("source sql must start with SELECT or WITH")
    match = _DENIED_KEYWORDS_RE.search(stripped)
    if match:
        raise DmlRejected(
            f"source sql contains denied keyword {match.group(0)!r}"
        )
    match = _DENIED_TABLE_LITERAL_RE.search(stripped)
    if match:
        raise DmlRejected(
            f"source sql contains a quoted file/URL reference in table "
            f"position {match.group(0)!r}"
        )
    match = _DENIED_PATH_LITERAL_RE.search(stripped)
    if match:
        raise DmlRejected(
            f"source sql contains a file/URL literal "
            f"{match.group(0)!r}"
        )
    try:
        statements = duckdb.extract_statements(stripped)
    except Exception as exc:
        raise DmlRejected(f"source sql does not parse: {exc}") from exc
    if len(statements) != 1:
        raise DmlRejected(
            f"source sql must be exactly one statement, "
            f"got {len(statements)}"
        )
    stmt_type = statements[0].type
    if stmt_type != duckdb.StatementType.SELECT:
        raise DmlRejected(
            f"source sql must be a SELECT statement, got {stmt_type}"
        )
    return stripped.rstrip(";").rstrip()


def sql_sha256(sql: str) -> str:
    """SHA-256 hex of the source SQL — stored on the audit row so the
    consumer's evidence packets can tie an artifact to its generator."""
    return hashlib.sha256(sql.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------
# create_table — structured column spec -> composed DDL
# ---------------------------------------------------------------------

def compose_create_table_ddl(
    namespace: str, table: str, columns: Any
) -> Tuple[str, bool]:
    """Compose ``CREATE TABLE`` DDL from a structured column spec.

    ``columns`` is a non-empty list of ``{name, type, not_null?, pk?}``
    maps.  Types must be in :data:`ALLOWED_COLUMN_TYPES`; ``pk: true``
    columns are gathered into a real ``PRIMARY KEY (c1, c2, ...)``
    constraint (and implied ``NOT NULL`` — DuckDB enforces both at
    INSERT, so a duplicate-key insert fails loudly).

    Returns ``(ddl, has_pk)``.  The statement is a plain ``CREATE
    TABLE`` (not IF NOT EXISTS): the engine rejects a create over an
    existing object first, so a silently-divergent spec can never be
    masked by IF NOT EXISTS.
    """
    if not isinstance(columns, (list, tuple)) or not columns:
        raise DmlRejected(
            f"columns must be a non-empty list of column specs, "
            f"got {columns!r}"
        )
    col_parts: List[str] = []
    pk_cols: List[str] = []
    seen: set = set()
    # justified: per-column spec validation — each element must be
    # individually type-checked (dict shape, name regex, type whitelist,
    # bool flags); vectorizing cannot raise on the offending element.
    for col in columns:
        if not isinstance(col, dict):
            raise DmlRejected(
                f"each column spec must be an object, got {col!r}"
            )
        col_name = col.get("name")
        if not isinstance(col_name, str) or not COLUMN_NAME_RE.fullmatch(
            col_name
        ):
            raise DmlRejected(
                f"column name must match ^[a-zA-Z][a-zA-Z0-9_]*$, "
                f"got {col_name!r}"
            )
        if col_name.lower() in seen:
            raise DmlRejected(f"duplicate column name {col_name!r}")
        seen.add(col_name.lower())
        col_type = col.get("type")
        if not isinstance(col_type, str):
            raise DmlRejected(
                f"column {col_name!r} type must be a string, "
                f"got {col_type!r}"
            )
        normalized_type = col_type.strip().upper()
        if normalized_type not in ALLOWED_COLUMN_TYPES:
            raise DmlRejected(
                f"column {col_name!r} type {col_type!r} is not in the "
                f"whitelist {sorted(ALLOWED_COLUMN_TYPES)}"
            )
        not_null = col.get("not_null", False)
        pk = col.get("pk", False)
        if not isinstance(not_null, bool) or not isinstance(pk, bool):
            raise DmlRejected(
                f"column {col_name!r} not_null/pk must be bools, "
                f"got not_null={not_null!r} pk={pk!r}"
            )
        if pk:
            not_null = True
            pk_cols.append(col_name)
        part = f"{quote_ident(col_name)} {normalized_type}"
        if not_null:
            part += " NOT NULL"
        col_parts.append(part)
    if pk_cols:
        col_parts.append(
            "PRIMARY KEY ("
            + ", ".join(quote_ident(c) for c in pk_cols)
            + ")"
        )
    ddl = (
        f"CREATE TABLE {qualified_name(namespace, table)} "
        f"({', '.join(col_parts)})"
    )
    return ddl, bool(pk_cols)


# ---------------------------------------------------------------------
# Catalog helpers (run on a caller-supplied connection/cursor)
# ---------------------------------------------------------------------

def catalog_object_kind(con, schema: str, name: str) -> Optional[str]:
    """Return ``'table'``, ``'view'`` or ``None`` for a catalog object.

    Uses ``duckdb_tables()``/``duckdb_views()`` — the post-write
    catalog assertion (an object must actually land in the claimed
    namespace) and drop-time kind resolution (``DROP TABLE`` on a view
    is a CatalogException, so the correct verb must be picked).
    """
    rows = con.execute(
        "SELECT table_name FROM duckdb_tables() "
        "WHERE schema_name = ? AND table_name = ?",
        [schema, name],
    ).fetchall()
    if rows:
        return "table"
    rows = con.execute(
        "SELECT view_name FROM duckdb_views() "
        "WHERE schema_name = ? AND view_name = ?",
        [schema, name],
    ).fetchall()
    if rows:
        return "view"
    return None


def catalog_column_names(con, schema: str, table: str) -> List[str]:
    """Column names of a catalog table (information_schema)."""
    rows = con.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = ? AND table_name = ? "
        "ORDER BY ordinal_position",
        [schema, table],
    ).fetchall()
    return [r[0] for r in rows]


# ---------------------------------------------------------------------
# Config resolution
# ---------------------------------------------------------------------

def resolve_settings(cfg: Any) -> Dict[str, Any]:
    """Extract the ``client_dml:`` config block defensively.

    Works on the real ``PipelineConfig`` and on the bare test doubles
    used in unit tests (``SimpleNamespace``/``MagicMock``): any field
    that is absent or the wrong type falls back to a safe default —
    ``allowed_namespaces`` defaults to ``[]`` (nothing is writable),
    ``dml_timeout_seconds`` defaults to ``server.query_timeout_seconds``
    (then 120), ``max_namespace_mb`` to ``None`` (quota off) and
    ``ttl_days`` to 90.
    """
    dml_cfg = getattr(cfg, "client_dml", None)

    allowed = getattr(dml_cfg, "allowed_namespaces", None)
    if isinstance(allowed, (list, tuple, set)):
        allowed_namespaces = [a for a in allowed if isinstance(a, str)]
    else:
        allowed_namespaces = []

    timeout = getattr(dml_cfg, "dml_timeout_seconds", None)
    if (
        timeout is None
        or isinstance(timeout, bool)
        or not isinstance(timeout, (int, float))
        or timeout <= 0
    ):
        server_cfg = getattr(cfg, "server", None)
        timeout = getattr(server_cfg, "query_timeout_seconds", None)
    if (
        timeout is None
        or isinstance(timeout, bool)
        or not isinstance(timeout, (int, float))
        or timeout <= 0
    ):
        timeout = 120.0

    quota = getattr(dml_cfg, "max_namespace_mb", None)
    if (
        quota is not None
        and (
            isinstance(quota, bool)
            or not isinstance(quota, (int, float))
            or quota <= 0
        )
    ):
        quota = None

    ttl = getattr(dml_cfg, "ttl_days", None)
    if (
        ttl is None
        or isinstance(ttl, bool)
        or not isinstance(ttl, (int, float))
        or ttl < 1
    ):
        ttl = 90

    return {
        "allowed_namespaces": allowed_namespaces,
        "dml_timeout_seconds": float(timeout),
        "max_namespace_mb": quota,
        "ttl_days": int(ttl),
    }


def check_client_tag(value: Any) -> Optional[str]:
    """Validate the advisory ``client_tag`` identity field."""
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise DmlRejected(
            f"client_tag must be a non-empty string, got {value!r}"
        )
    return value.strip()


def check_quota(settings: Dict[str, Any], namespace_mb: float) -> None:
    """Reject a write when the namespace already exceeds the quota.

    ``max_namespace_mb`` is ``None`` by default (quota off).  The size
    figure is an estimate from ``duckdb_tables().estimated_size`` —
    advisory, not byte-exact.
    """
    quota = settings.get("max_namespace_mb")
    if quota is not None and namespace_mb >= quota:
        raise DmlRejected(
            f"namespace size estimate {namespace_mb:.1f} MB meets/exceeds "
            f"client_dml.max_namespace_mb {quota} MB"
        )


__all__ = [
    "DmlRejected",
    "DML_OPS",
    "INSERT_SELECT_MODES",
    "DO_PUT_MODES",
    "ALLOWED_COLUMN_TYPES",
    "NAME_RE",
    "COLUMN_NAME_RE",
    "validate_name",
    "quote_ident",
    "qualified_name",
    "parse_table_ref",
    "validate_source_sql",
    "sql_sha256",
    "compose_create_table_ddl",
    "catalog_object_kind",
    "catalog_column_names",
    "resolve_settings",
    "check_client_tag",
    "check_quota",
]
