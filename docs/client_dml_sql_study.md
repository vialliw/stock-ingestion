# Feasibility Study — Client-Issued DML on the Flight Service

**Task:** F21-T1 (study only — no implementation)
**Date:** 2026-09-21
**Status:** complete — verdict in §6
**DuckDB version probed:** 1.5.5 (all engine claims below were verified
live against the installed version unless marked otherwise)

## 0. Question and context

Downstream projects — the motivating example is
`Bollinger_reversal_analysis` — currently pull `weekly_close`,
`weekly_indicators`, and `daily_close` over Flight `do_get`
(SELECT-only, `src/flight_server.py:294-319`) into a local
`data/bollinger.duckdb` and build derived tables there (a canonical
`week_calendar` with dense `week_id`, `weekly_bars`/`daily_bars`
snapshots, signal/decision/trade ledgers). The question: **should
trusted local clients be able to issue DML (CREATE SCHEMA/TABLE,
INSERT, UPDATE, DELETE, CREATE OR REPLACE, DROP) through the Flight
service so derived tables live in the canonical DB?**

The primary option evaluated is **schema-first**: a dedicated
`research` namespace (or per-project namespaces) where client DML is
permitted, while core tables (`daily_close`, `weekly_close`,
`weekly_indicators`, `fear_greed_history`, `company_metadata`,
`ingestion_runs`, `splits_processed`, `schema_version` —
`src/db.py:82-188`) remain read-only. Open DML is the comparison
baseline.

### Verified platform facts (grounding for everything below)

| Fact | Evidence |
|---|---|
| One `threading.Lock` (`_write_lock`) serializes **every** write path | `src/db.py:71`, held by `upsert_dataframe` (:288), `purge_ticker(s)` (:310,:327), `record_split` (:357), `delete_ticker_data` (:369), `upsert_company_metadata` (:475), `upsert_fear_greed` (:521), `rebuild_weekly_close` (:556), `rebuild_weekly_indicators` (:607), `batch_record_splits` (:746), `record_ingestion_run` (:785), `close`/CHECKPOINT (:1742) |
| Single read-write connection `self.conn` | `src/db.py:72` |
| `execute_query` (the `do_get` path) does **not** take `_write_lock`; it uses a short-lived read-only conn — in practice a `self.conn.cursor()` wrapped in `BEGIN TRANSACTION READ ONLY`, because a same-process `duckdb.connect(path, read_only=True)` is refused once a RW conn exists | `src/db.py:245-268` (fallback documented at :249-254) |
| `do_get` validation is a *prefix* check: stripped SQL must start with `SELECT` or `WITH` | `src/flight_server.py:35-47` |
| `do_put` **already exists**: reads an Arrow stream, validates against `_DAILY_CLOSE_SCHEMA`, filters `exclude_tickers`, upserts into `daily_close` via `upsert_dataframe`. The `descriptor` argument is currently **ignored** | `src/flight_server.py:74-101`, `:578-620` |
| `do_action` routes named actions → JSON payload → `_ActionResult` JSON | `src/flight_server.py:325-572`, `_ActionResult` at :58-71 |
| Per-query timeout = worker thread + `threading.Timer` racing a `queue.Queue` (`server.query_timeout_seconds`, default 120 s — `src/config.yaml:1240`) | `src/flight_server.py:259-292` |
| DuckDB has **no** `GRANT`/per-schema privileges | `GRANT SELECT ...` → `ParserException` on 1.5.5 |
| `duckdb.extract_statements` exists; returns `Statement` objects with `.type` (`StatementType.CREATE`, `INSERT`, `UPDATE`, `DELETE`, `DROP`, `ALTER`, `MERGE_INTO`, `TRANSACTION`, `ATTACH`, `DETACH`, `COPY`, `EXPORT`, `LOAD`, `EXTENSION`, `SET`, `VARIABLE_SET`, `PREPARE`, `EXECUTE`, `CALL`, `VACUUM`, `ANALYZE`, `SELECT`, …) and `.query` text | verified 1.5.5 |
| `CREATE SECRET` classifies as `StatementType.CREATE` — a CREATE allowlist must sub-classify by keyword | verified 1.5.5 |
| `PRAGMA database_size` classifies as `StatementType.SELECT`; `CHECKPOINT` as `CALL`; `WITH … INSERT` as `INSERT` | verified 1.5.5 |
| `ATTACH 'research.duckdb' AS research` creates a **separate catalog in a separate file with its own WAL**; `research.t` resolves to catalog `research`, schema `main`; writes work; `PRAGMA database_size` reports one row per attached DB | verified 1.5.5 |
| A single transaction may write to **only one** attached database — cross-catalog write txns raise `TransactionException` | verified 1.5.5 |
| `USE research` sets `search_path='research.main'`; unqualified `CREATE TABLE` then lands in the `research` catalog, and an unqualified `DROP TABLE victim` fails to resolve a `main_db` table ("Did you mean main_db.victim?") — name resolution does **not** silently fall back to the core catalog | verified 1.5.5 |
| `SET schema='x'` / `SET search_path=…` / `USE` all exist; `schema`/`search_path` are **LOCAL**-scope settings (per-connection, so a dedicated cursor isolates them from `self.conn`) | verified 1.5.5 |
| No `statement_timeout` setting in 1.5.5; `DuckDBPyConnection.interrupt()` exists | verified 1.5.5 |
| `allowed_paths`, `allowed_directories`, `enable_external_access`, `lock_configuration`, `disabled_filesystems`, `max_temp_directory_size` settings exist (GLOBAL scope) | `duckdb_settings()` on 1.5.5 |
| **While the server holds `stock_ingestion.duckdb` RW, no external process can open it at all — not even `read_only=True`** (Windows file lock: "The process cannot access the file because it is being used by another process. File is already open in …"). Same for an attached `research.duckdb`. The consumer contract already says exactly this ("Read via Flight only… read-only fallback only when no writer holds the lock") | two-process probe, 1.5.5; `docs/DATA_SERVICE_API.md:236-243` |

Consequence of the last fact: there is no bypass channel — client DML
can only ever arrive *through* the service, and any research catalog
attached to the server's connection is likewise reachable only through
Flight (`do_get` already serves `SELECT * FROM research.t` with zero
code changes, since qualified names resolve across catalogs).

---

## 1. Concurrency & locking

### 1.1 How client DML would join the write queue

All writes go through `DatabaseManager._write_lock`
(`src/db.py:71`). Client DML submitted via a new Flight endpoint would
execute on the same `self.conn` (or a cursor of it) under the same
lock → writes become a **serialized FIFO queue inside the process**,
regardless of how many gRPC handler threads pyarrow Flight runs
concurrently (each `do_get`/`do_action`/`do_put` runs on its own
gRPC thread; `server.serve()` blocks in `src/main.py:85`).

Important correction to the naive mental model: **`run_daily_load`
does not hold the lock for the load's duration.** The dominant cost —
`fetch_and_clean` (yfinance download + normalization) at
`src/ingestion.py:1299` and `:1333` — runs entirely *outside* the
lock. Lock acquisitions during a daily load are short, discrete
statements:

- `upsert_dataframe(df_new)` backfill merge — `src/ingestion.py:1302`
- `upsert_dataframe(df)` trailing-refresh merge — `src/ingestion.py:1338`
  (one `MERGE INTO daily_close` over the whole batch, `src/db.py:290-305`)
- split-repair deletes/upserts — `src/ingestion.py:227-243` (per-ticker
  `DELETE` + one batch `MERGE` + `batch_record_splits`)
- `record_ingestion_run` — `src/ingestion.py:1354`
- `_weekly_bridge` → `rebuild_weekly_close` + `rebuild_weekly_indicators`
  (`src/ingestion.py:884-885`) — the heaviest serialized units: two
  full-table rebuild statements (~17 s combined per the API doc note at
  `docs/DATA_SERVICE_API.md:187`)
- `_fear_greed_bridge`, `_metadata_bridge` — small upserts.

So client DML doesn't queue behind a multi-minute load; it interleaves
*between* the load's short lock holds. Worst-case client wait ≈ one
weekly-rebuild statement (~seconds, occasionally ~17 s). Conversely a
long client statement delays only the load's *next* upsert, not the
whole run — and is itself bounded by a timeout (§4.4).

Quantification caveat: lock hold time per statement is workload-sized.
A daily-load `MERGE` is ~900 tickers × ~4 sessions — milliseconds. A
client `CREATE TABLE research.x AS SELECT … FROM daily_close`
full-history scan is the largest realistic client unit (seconds). This
is bounded and schedulable, not a structural problem.

### 1.2 CREATE OR REPLACE mid-load

Safe under this design. Each client statement executes under
`_write_lock` as a single auto-committed statement (§4.1): readers
(`do_get` cursors under READ ONLY txns, `src/db.py:259-261`) see a
consistent MVCC snapshot — either the old table or the new one, never a
half-replaced one. The scheduled load writes only core tables; a
client `CREATE OR REPLACE TABLE research.t …` touches only the
research namespace — disjoint targets, no interference beyond lock
queuing. `CREATE OR REPLACE`/`DROP` of a *core* table must be blocked
by the namespace guardrail (§3), not by the lock.

### 1.3 Can a long client transaction stall the scheduled load?

Only if explicit transactions are allowed. A client-held `BEGIN` on the
shared write connection would hold `_write_lock` (if we hold it across
the txn) — starving the load — or, if run on a separate cursor without
the lock, would create an unbounded MVCC pin and possible
write-conflict aborts. **Recommendation: forbid `TRANSACTION`-type
statements entirely in phase 1** (§4.1) — then the longest stall is
one client statement, bounded by the DML timeout.

---

## 2. API shape

Three candidate shapes; all reuse existing transport patterns.

### (a) `execute_dml` `do_action` + server-side statement classification

Client sends `{"sql": "…", "namespace": "research"}`; the server
parses (`duckdb.extract_statements`), classifies verb + target object,
enforces the allowlist, executes under `_write_lock`, returns JSON
`{status, statements_run, rows_affected, duration_ms}`.

- **Pros:** most expressive — covers INSERT/UPDATE/DELETE/DDL in one
  endpoint; fits the established `do_action` routing pattern
  (`src/flight_server.py:325+`); consistent result envelope.
- **Cons:** the guardrail is **SQL parsing** — the fragile part of the
  design (§3.3). Every new verb allowed widens the parse surface.
  Data still has to be serialized *into* SQL text (client builds
  `INSERT INTO … VALUES` strings — clumsy for bulk frames; no
  parameter binding protocol exists, `DATA_SERVICE_API.md:244-246`).

### (b) Generalized `do_put` with a descriptor

`do_put` already exists (`src/flight_server.py:578-620`) but ignores
`descriptor` and hard-codes the `daily_close` schema
(`_validate_daily_close_schema`, :88-101). Generalize: descriptor
`{"table": "research.<name>", "mode": "create_or_replace|append|upsert",
"pk": [...]}`; server validates `table` ∈ allowed namespace, executes
`CREATE OR REPLACE TABLE research.<name> AS SELECT * FROM
uploaded_batch` (or `INSERT INTO`/`MERGE INTO` for append/upsert) under
`_write_lock`.

- **Pros:** builds on an existing endpoint and an existing schema
  validation pattern; **zero SQL parsing for the write itself** —
  the Arrow schema *is* the contract; bulk frames stream natively
  (the actual downstream use case is materializing pandas/Arrow
  frames, not hand-writing INSERTs); `mode` is an enum, trivially
  allowlisted.
- **Cons:** cannot express `DELETE`/`UPDATE`/`DROP`/ad-hoc DDL —
  needs a small companion action or per-mode flags for cleanup;
  CREATE-AS-SELECT from a source query (e.g. `CREATE TABLE t AS
  SELECT … FROM weekly_close`) isn't expressible without either SQL
  or a two-step "do_get → do_put" round-trip through the client.

### (c) Two-call create-schema-then-write

A `create_research_table`/`register_namespace` admin-style action
provisions the namespace/table shell; data flows via (b). This is not
a third transport so much as a **lifecycle split**: DDL via action,
DML via stream.

- **Pros:** smallest write surface; table creation becomes an
  auditable, typed RPC (name validation is a regex, not SQL parsing);
  matches how the existing admin actions already work.
- **Cons:** two calls per workflow; still needs (b) for data.

### Recommendation: **(b) + (c) hybrid — no arbitrary-SQL endpoint**

The honest assessment is that (a)'s flexibility is mostly unneeded for
the real workload (materialize derived frames; occasionally drop a
stale table) while carrying all of the parser-fragility risk.
Recommend:

1. **Generalized `do_put`** (option b) as the only write transport —
   descriptor `{table: "research.<name>", mode: create_or_replace |
   append | upsert, pk: [...]}`.
2. **A `research_admin` do_action** (option c) with a *closed*
   sub-command enum — `{"op": "drop_table", "table": "research.x"}`,
   `{"op": "list_tables"}`, `{"op": "register_namespace",
   "namespace": "bollinger"}` — where `table`/`namespace` are
   validated by `^[a-z][a-z0-9_]*$`-style regexes and joined server-
   side into fully-qualified names. No SQL text ever crosses the wire
   for writes.
3. `CREATE TABLE … AS SELECT` derivations from core tables are served
   by **do_get → do_put round-trip** (fetch the SELECT result as
   Arrow, put it back as a research table) — already possible with
   today's endpoints plus (b); if profiling shows the round-trip is a
   real cost, a *narrow* `{"op": "create_as", "table": "research.x",
   "sql": "<SELECT>"}` variant can reuse the existing SELECT-only
   validation (prefix check + READ-ONLY-txn execution on the source
   query) rather than opening general DML parsing.

This gives 95% of the value with an enforcement boundary made of
regexes and enums — not a SQL parser. Free-form `execute_dml` (a) is
the phase-2 fallback if a genuine need emerges (§6).

---

## 3. Schema scoping & guardrails

### 3.1 The hard fact: no GRANT

DuckDB 1.5.5 has no SQL privilege system — `GRANT` is a
`ParserException`. There is no `CREATE USER`, no per-schema ACL.
**All enforcement is application-layer, in the Flight server**, and
its robustness determines whether "core tables read-only" is a policy
or a hope.

### 3.2 Enforcement options, ranked by robustness

**Option 1 — Attached catalog per boundary (most robust, recommended).**

```sql
ATTACH 'C:\Intel\parquet\research.duckdb' AS research;   -- server-side at startup
-- per-project namespaces are schemas inside the research catalog:
CREATE SCHEMA IF NOT EXISTS research.bollinger;
```

- The boundary is **catalog-level and enforced by name resolution**:
  `research.<schema>.<table>` vs `main_db.main.<core_table>`. With
  option-(b/c) API shape the server composes the qualified name from
  validated tokens — the client literally cannot spell a core table
  into the write path.
- Even under a hypothetical SQL path, verified behavior helps:
  `USE research` confines unqualified writes to the research catalog
  (unqualified `DROP TABLE victim` on a `main_db` table raised
  `CatalogException … Did you mean main_db.victim?`), and a single
  transaction cannot write two catalogs (`TransactionException:
  a single transaction can only write to a single attached database`).
- **Cleanup is physical**: `DETACH research` + delete
  `research.duckdb` removes the entire namespace, its WAL, and any
  abandoned tables — no per-table bookkeeping needed for bulk reclaim.
- Quota is physical too: `os.path.getsize("research.duckdb")` or the
  `research` row of `PRAGMA database_size` (verified — one row per
  attached DB).
- Separate WAL (`research.duckdb.wal` verified) means research churn
  doesn't bloat the core WAL, and a corrupt/catastrophic research file
  doesn't take `daily_close` down with it.
- Caveats: attached files are exclusively locked while attached (even
  RO opens by other processes fail — verified), so research tables are
  reachable **only via Flight** — acceptable, arguably a feature
  (single access path). `ATTACH` must be re-issued at server start
  (`DatabaseManager._init_schema`, `src/db.py:79`, is the natural
  place; note attached-DB existence persists across restarts since the
  file persists — attach-by-name is idempotent via `ATTACH IF NOT
  EXISTS` semantics or a catalog check). R2 sync
  (`src/r2_sync.py`, boto3 upload of `cfg.db_path`) would need an
  explicit decision whether `research.duckdb` is synced — recommend
  *not* (derived data is reproducible).

**Option 2 — Same-catalog schema + statement parsing.**

`CREATE SCHEMA research` inside `stock_ingestion.duckdb`; every client
statement is parsed with `duckdb.extract_statements` (verified
available) and each `Statement`'s `.type` checked against a verb
allowlist (`CREATE`, `INSERT`, `UPDATE`, `DELETE`, `DROP` — with
`CREATE` sub-classified because `CREATE SECRET`/`CREATE MACRO`/
`CREATE VIEW`/`CREATE SEQUENCE` all report `StatementType.CREATE`,
and SECRET especially must be denied — credentials and
`enable_external_access` surface). The **target object** must then be
extracted from `.query` text (extract_statements does not provide it)
and required to carry a `research.` (or registered-namespace) prefix —
`CREATE TABLE t2 AS SELECT * FROM daily_close` is type CREATE with an
unqualified target that would land wherever `search_path` points.

- This works, and `USE research`/`SET schema='research'` (LOCAL scope,
  so a dedicated `self.conn.cursor()` per request keeps it off the
  shared conn) makes *unqualified* writes land in the right place.
- But it is the **fragile** path, honestly: verb classification is
  solid (real parser, catches `WITH … INSERT` correctly as INSERT —
  unlike the do_get prefix check), yet *target extraction* is regex/
  token-parsing over the raw text. `CREATE TABLE research.t AS
  SELECT`, qualified-vs-unqualified names, quoted identifiers
  (`"research"."t"`), `MERGE INTO`, `INSERT INTO … BY NAME`, CTE
  shadows — each is a place a mismatch between what the checker
  thinks the target is and what DuckDB resolves could slip. A
  defense-in-depth second layer (execute under
  `SET search_path='research'` on an isolated cursor, plus a
  post-statement catalog assertion that the affected object's
  `database_name`/`schema_name` via `duckdb_tables()` is in-scope)
  mitigates but does not eliminate this.
- Cheaper operationally (one file, existing R2 backup path covers it)
  but strictly weaker isolation: a bug writes into the canonical file.

**Option 3 — Open DML.** Rejected outright: no namespace boundary at
all, full parser burden, one `DROP TABLE daily_close` typo away from
a re-ingest. Listed only for completeness — do not implement.

### 3.3 Guardrail checklist (whichever boundary)

- **Denylist that must hold regardless of parser:** `ATTACH`, `DETACH`,
  `COPY`, `EXPORT`, `IMPORT`, `LOAD`, `INSTALL`/`EXTENSION`,
  `PRAGMA`, `SET`/`VARIABLE_SET`, `CALL` (includes `CHECKPOINT`),
  `PREPARE`, `EXECUTE`, `CREATE SECRET`, `TRANSACTION` — all carry
  distinct `StatementType`s (verified), so a *denylist-first* pass on
  `extract_statements` output is cheap insurance even in the option-b
  design (defense in depth against any future SQL-accepting path).
  The challenger validator already demonstrates this pattern —
  `_FORBIDDEN_RE` at `src/flight_server_challenger.py:78-82`.
- **CREATE OR REPLACE / DROP accidents:** in option (b), `mode` is
  explicit — `create_or_replace` is a deliberate flag, never implied.
  For option (a)/SQL, require `DROP`/`CREATE OR REPLACE` to name a
  registered-namespace object *and* (optionally) a `confirm` payload
  flag for DROP. Consider `research_registry` (§3.4) as the source of
  truth: refuse DROP on objects not in the registry.
- **Unbounded growth:** quota = server-side check on
  `PRAGMA database_size`'s `research` row (or file size) before each
  write + a periodic sweep; reject writes past a configured cap
  (e.g. `research.max_bytes`). `max_temp_directory_size` exists for
  spill bounds but is GLOBAL — set it deliberately, not per-request.
- **Abandoned tables / ownership:** a server-owned registry table in
  the **main** catalog (clients can't write it):

  ```sql
  CREATE TABLE IF NOT EXISTS research_registry (
      namespace   VARCHAR NOT NULL,     -- 'bollinger'
      table_name  VARCHAR NOT NULL,     -- 'weekly_bars'
      full_name   VARCHAR NOT NULL,     -- 'research.bollinger.weekly_bars'
      owner       VARCHAR,              -- client-supplied identity token
      created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      last_write  TIMESTAMP,
      rows_est    BIGINT,
      PRIMARY KEY (namespace, table_name)
  );
  ```
  TTL/cleanup policy: `data_health`-style admin sweep (or a weekly
  job) drops `research.*` objects whose `last_write` is older than a
  configured TTL *and* not marked `pinned`; with Option 1 the nuclear
  cleanup is `DETACH` + delete file. Ownership is advisory (trusted
  local clients) — the registry exists for audit/TTL, not security.

- **Read path note:** `SELECT * FROM research.bollinger.weekly_bars`
  through existing `do_get` needs no changes — qualified catalog names
  resolve transparently (verified). Update `DATA_SERVICE_API.md` to
  document the namespace when/if shipped.

---

## 4. Consistency & failure modes

### 4.1 Atomicity model

**Auto-commit per statement; forbid explicit transactions in phase 1.**
`extract_statements` classifies `BEGIN`/`COMMIT`/`ROLLBACK` as
`StatementType.TRANSACTION` — trivially deniable. Rationale:

- Single-statement auto-commit is already the codebase's model
  (every `with self._write_lock:` block wraps discrete `conn.execute`
  calls; `run_daily_load` is a sequence of auto-committed statements,
  `src/ingestion.py:1302-1354`).
- An explicit client txn either holds `_write_lock` across RPC
  round-trips (starves the scheduled load — §1.3) or runs unlocked on
  a cursor (MVCC pin + write-conflict surface + no serialization
  guarantee vs the load). Neither is worth it for the research-table
  use case.
- Multi-statement idempotency is handled client-side (re-run the
  `create_or_replace` do_put — it is naturally idempotent) or via the
  CTAS action; a failed mid-sequence leaves the *previous* committed
  state, which is the right failure semantics.

### 4.2 WAL / checkpoint implications

Each attached DB gets its own WAL (`research.duckdb.wal` — verified).
Writes checkpoint on clean close (`DatabaseManager.close()` runs
`CHECKPOINT` under the lock, `src/db.py:1741-1749` — it checkpoints
the whole DBMS including attached catalogs). A crash mid-write leaves
WAL recovery to next open — DuckDB's standard path, same exposure as
today's core writes; the difference is a *client-triggered* crash could
strand a bigger `research` WAL — bounded by quota + per-statement
timeout. Long-running `do_get` snapshot readers can delay WAL
checkpoint reclamation (DuckDB keeps versions needed by open
snapshots) — pre-existing behavior, unchanged.

### 4.3 Audit logging — `client_dml_audit`

Mirror `ingestion_runs` (`src/db.py:109-122`, populated by
`record_ingestion_run`, :760-811). Server-owned, main catalog:

```sql
CREATE TABLE IF NOT EXISTS client_dml_audit (
    audit_id      BIGINT PRIMARY KEY,          -- MAX+1, same idiom as ingestion_runs
    client_tag    VARCHAR,                     -- payload-supplied identity (advisory)
    op            VARCHAR NOT NULL,            -- 'do_put:create_or_replace' | 'admin:drop_table' | 'dml:INSERT' …
    target        VARCHAR NOT NULL,            -- validated fully-qualified object name
    bytes_in      BIGINT,                      -- Arrow stream bytes (do_put) or 0
    rows_affected BIGINT,
    sql_text      VARCHAR,                     -- truncated via _truncate_sql (flight_server.py:50-55); NULL for do_put
    status        VARCHAR NOT NULL,            -- SUCCESS | FAILED | REJECTED
    error         VARCHAR,
    duration_ms   INTEGER,
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

Log **rejections too** (`status='REJECTED'`) — that's the value of an
audit trail. Insert under `_write_lock` after each op, exactly as
`record_ingestion_run` does (`src/db.py:785-810`); expose read-side via
the existing `do_get` (it's just a table) or a `get_dml_history`
action mirroring `get_run_history` (`src/flight_server.py:550-565`).

### 4.4 Timeouts

Reuse the `_execute_query_with_timeout` pattern verbatim
(`src/flight_server.py:259-292`): worker thread runs the DML unit,
`threading.Timer` posts `TimeoutError` into a queue. Two honest
caveats inherited from that design:

- The timer fires an *error back to the client* but does not by itself
  cancel the DuckDB statement — duckdb 1.5.5 has no
  `statement_timeout` setting; `connection.interrupt()` exists
  (verified) and should be called on the executing cursor when the
  timer fires, so the statement actually stops rather than holding
  `_write_lock` while a dead RPC's work completes.
- The lock should be held only around `conn.execute`, not around
  `queue.get` — i.e. timeout wraps the whole unit; the lock wraps the
  statement (same structure as today's writers).

Default: reuse `server.query_timeout_seconds` (120 s,
`src/config.yaml:1240`) with a separate `dml_timeout_seconds` knob if
needed.

### 4.5 Partial-write recovery

- `do_put` mode `create_or_replace`: CTAS is atomic — failure leaves
  the old table. `append`/`upsert`: one `INSERT`/`MERGE` statement =
  one auto-commit — all-or-nothing. So partial writes are
  *impossible by construction* as long as one request = one statement.
- A failed statement inside a multi-statement `execute_dml` request
  (if phase 2 ever adds it) should abort the remaining statements and
  report per-statement status — document it, don't silently continue.

---

## 5. Alternatives

### 5.1 Status quo — derived tables stay local

`Bollinger_reversal_analysis` keeps pulling core tables over `do_get`
into `data/bollinger.duckdb` and building `week_calendar`,
`weekly_bars`, ledgers locally.

- **Cost:** every downstream project re-derives the same week mapping
  and re-snapshots the same bars; no shared `week_id` canonical
  definition; local copies drift.
- **Benefit:** zero write surface, zero new failure modes, zero
  guardrail code. The service stays a read API + ingestion daemon —
  its current, well-tested shape.

### 5.2 The sharp middle path — server-hosted canonical `week_calendar`

The actual motivating use case is *shared week_id mapping*, not
general client SQL. The service can host a `week_calendar` table
itself — built by `rebuild_weekly_close`'s owner (`src/db.py:543`) or
a sibling server-side rebuild under `weekly_refresh`
(`src/flight_server.py:431-441`), populated from the same
`pandas_market_calendars` NYSE machinery already imported in
`src/db.py:21`/`src/ingestion.py`:

```sql
CREATE TABLE IF NOT EXISTS week_calendar (
    week_id      INTEGER NOT NULL,   -- dense, monotonic
    week_start   DATE NOT NULL,      -- Monday of ISO week
    week_end     DATE NOT NULL,      -- nominal Friday key = weekly_close.Date basis
    sessions     INTEGER,            -- trading days actually in the week
    is_partial   BOOLEAN,            -- current forming week
    PRIMARY KEY (week_id)
);
```

**Benefits it must deliver to justify itself** (concrete, checkable):

1. One canonical `week_id` every downstream project joins on —
   eliminates the per-project recompute that motivated this study.
2. Zero client-write surface: built and owned by the service under the
   existing `_write_lock`, inside the existing `weekly_refresh`
   action — no new endpoint, no parser, no quota, no audit table.
3. Readable through today's `do_get` with no code change.
4. Extends naturally: `daily_bars`/`weekly_bars` *snapshots* — if the
   downstream need is a frozen basis rather than live `weekly_close`,
   a server-side `CREATE TABLE weekly_bars_snapshot_<yyyymmdd>` driven
   by an admin action is still service-owned DML, not client DML.

If after this the *remaining* unmet need is "persist my own computed
frames beside the canonical data," that is the only genuine argument
for §2's write surface.

### 5.3 Verdict-driving comparison

| | Status quo | Server-hosted `week_calendar` | Research namespace (b+c) | Open SQL DML (a) |
|---|---|---|---|---|
| Shared week_id | no | **yes** | yes (if clients build it — reintroduces divergence) | same |
| Write surface | none | none | descriptor-validated, namespace-bound | arbitrary SQL |
| Enforcement | n/a | n/a | regex/enum + catalog boundary | full SQL parsing |
| New failure modes | none | none (existing writer) | bounded | wide |

---

## 6. Verdict

**Implement-with-restrictions — but implement the middle path first,
and implement client writes as descriptor-driven `do_put` +
`research_admin`, not free-form SQL.**

1. **Phase 1 (do this first — likely sufficient):** server-hosted
   `week_calendar` under `weekly_refresh` (§5.2). Delivers the actual
   motivating benefit with no write-surface change.
2. **Phase 2 (only if a concrete cross-project need survives Phase
   1):** `ATTACH '…\research.duckdb' AS research` at startup
   (`_init_schema`), generalized `do_put` descriptor
   `{table, mode, pk}` confined to the `research` catalog by
   token-validated names, `research_admin` action with a closed op
   enum (`drop_table`/`list_tables`/`register_namespace`/`create_as`
   with SELECT-only source), `client_dml_audit` + `research_registry`
   tables, per-request timeout via worker+Timer+`interrupt()`,
   `TRANSACTION` verbs forbidden, quota via `PRAGMA
   database_size`/file size, TTL sweep for abandoned tables.
3. **Do not implement:** `execute_dml` arbitrary-SQL endpoint (option
   a) and open DML — the parser-fragility risk buys flexibility the
   known workload doesn't need. Revisit only if Phase 2 users
   demonstrate a real requirement for ad-hoc UPDATE/DELETE, and even
   then prefer widening the `mode` enum over accepting SQL text.

### Risk list

| Risk | Severity | Mitigation |
|---|---|---|
| Core-table corruption via client write | High impact / low likelihood under (b+c): names are server-composed from validated tokens; impossible under the do_put descriptor unless `table` validation is bypassed | Catalog-boundary (Option 1), `^[a-z][a-z0-9_]*$` name regex, post-write catalog assertion via `duckdb_tables()` |
| Parser fragility (if SQL path ever added) | Medium | Denylist-first on `extract_statements` types (all verified distinct); require `research.`-qualified targets; treat as fragile, never sole enforcement — keep the `search_path`/cursor second layer |
| Scheduled-load stall | Low | Writes interleave between the load's short lock holds (§1.1); statement timeout + `interrupt()`; forbid transactions |
| `CREATE OR REPLACE`/DROP of the wrong table | Medium | Explicit `mode`/`op`; registry check before DROP; audit log makes accidents attributable |
| Unbounded growth of research file | Medium | Quota check per write + periodic sweep; separate file keeps blast radius off `stock_ingestion.duckdb` |
| Service crash mid-client-write | Low | Per-statement auto-commit; WAL replay on next start; research file is disposable/rebuildable |
| Research file locking blocks external readers | Accepted | Verified: attached files are exclusively held; all research reads go through `do_get` — consistent with the existing single-door model |
| `enable_external_access` hardening | Deferred | Setting `enable_external_access=false`/`allowed_paths` is GLOBAL and would affect all consumers' `COPY`/parquet/secret use; evaluate separately — not required for (b+c) since no SQL reaches the engine |

### What this study verified vs assumed

Verified live on duckdb 1.5.5: `extract_statements` API and all
`StatementType` classifications cited; no `GRANT`; ATTACH catalog/file/
WAL separation; single-catalog-per-transaction write restriction; `USE
research` name-resolution confinement; LOCAL scope of `schema`/
`search_path`; exclusive file locking of attached DBs (no external RO
open); `PRAGMA database_size` per-attached-DB rows; presence of
`interrupt()` and absence of `statement_timeout`. Assumed (not probed):
exact per-statement lock-hold timings (workload-dependent, bounded
argument given in §1.1); pyarrow gRPC thread-pool concurrency
(uncontroversial; consistent with spec §9's "Arrow Flight serves
concurrent requests" note, `docs/tech_spec/gemini_tech_spec.1.0.2.md:36`).

---

*Prepared by executor agent B under task F21-T1. Source citations are
against the working tree at claim time (commit of `devin_blackboard`
claim 6b426cf era); line numbers may drift with later commits.*
