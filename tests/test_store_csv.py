"""Critic (T4) tests for store_csv — parametrized over both duel implementations.

Hermetic: each test runs against a tmp_path-isolated DuckDB file via
monkeypatched STORE_DB_PATH / STORE_DB_PATH_CHALLENGER, so ./stock.duckdb and
./stock_challenger.duckdb are never touched.
"""

import importlib

import duckdb
import pytest

IMPLS = [
    ("src.store", "STORE_DB_PATH"),
    ("src.store_challenger", "STORE_DB_PATH_CHALLENGER"),
]


def _make_impl(request, tmp_path, monkeypatch):
    module_name, env_var = request.param
    db_path = tmp_path / "isolated.duckdb"
    monkeypatch.setenv(env_var, str(db_path))
    module = importlib.import_module(module_name)
    return module, db_path


@pytest.fixture(params=IMPLS, ids=[name for name, _ in IMPLS])
def store(request, tmp_path, monkeypatch):
    module, db_path = _make_impl(request, tmp_path, monkeypatch)
    return module.store_csv, db_path


def _query(db_path, sql, params=()):
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        return conn.execute(sql, params).fetchall()
    finally:
        conn.close()


# --- Mandatory acceptance criteria ----------------------------------------


def test_first_call_returns_int_id_gte_1(store):
    """Criterion 1: first store_csv returns an int id >= 1."""
    store_csv, _ = store
    row_id = store_csv("AAPL", "2024-01-01", b"a,b\n1,2")
    assert isinstance(row_id, int), f"expected int, got {type(row_id)}"
    assert row_id >= 1, f"expected id >= 1, got {row_id}"


def test_second_call_same_key_returns_strictly_greater_id(store):
    """Criterion 2: second call with same (symbol, date) -> strictly greater id."""
    store_csv, _ = store
    id1 = store_csv("AAPL", "2024-01-01", b"a,b\n1,2")
    id2 = store_csv("AAPL", "2024-01-01", b"a,b\n3,4")
    assert isinstance(id2, int)
    assert id2 > id1, f"expected {id2} > {id1} (append semantics, no upsert)"


def test_append_semantics_both_rows_persist(store):
    """Criterion 3: COUNT(*) for same (symbol, date) == 2 after two calls."""
    store_csv, db_path = store
    store_csv("AAPL", "2024-01-01", b"a,b\n1,2")
    store_csv("AAPL", "2024-01-01", b"a,b\n3,4")
    rows = _query(
        db_path,
        "SELECT COUNT(*) FROM raw_csv WHERE symbol = ? AND date = ?",
        ("AAPL", "2024-01-01"),
    )
    assert rows[0][0] == 2, f"expected 2 persisted rows, got {rows[0][0]}"


def test_csv_bytes_round_trip(store):
    """Criterion 4: persisted csv_bytes blob round-trips equal to input."""
    store_csv, db_path = store
    payload = b"a,b\n1,2"
    row_id = store_csv("AAPL", "2024-01-01", payload)
    rows = _query(db_path, "SELECT csv_bytes FROM raw_csv WHERE id = ?", (row_id,))
    assert len(rows) == 1
    persisted = rows[0][0]
    assert bytes(persisted) == payload, f"blob mismatch: {persisted!r} != {payload!r}"


def test_empty_bytes_persists_row(store):
    """Criterion 5: store_csv with b'' does not raise and persists a row."""
    store_csv, db_path = store
    row_id = store_csv("AAPL", "2024-01-01", b"")
    assert isinstance(row_id, int) and row_id >= 1
    rows = _query(
        db_path,
        "SELECT COUNT(*) FROM raw_csv WHERE symbol = ? AND date = ?",
        ("AAPL", "2024-01-01"),
    )
    assert rows[0][0] == 1


# --- Additional breaking tests (critic edge cases) -------------------------


def test_binary_bytes_with_nulls_and_non_utf8_round_trip(store):
    """BLOB must survive arbitrary bytes, not just ASCII CSV text."""
    store_csv, db_path = store
    payload = bytes(range(256)) + b"\x00\xff\xfe"
    row_id = store_csv("BIN", "2024-02-02", payload)
    rows = _query(db_path, "SELECT csv_bytes FROM raw_csv WHERE id = ?", (row_id,))
    assert bytes(rows[0][0]) == payload


def test_ids_strictly_increasing_across_many_appends(store):
    """Sequence must not reset between calls (fresh connection per call)."""
    store_csv, _ = store
    ids = [store_csv("MSFT", "2024-01-0%d" % i, b"x,y\n1,2") for i in range(1, 6)]
    assert ids == sorted(ids), f"ids not increasing: {ids}"
    assert len(set(ids)) == len(ids), f"duplicate ids: {ids}"
    for earlier, later in zip(ids, ids[1:]):
        assert later > earlier


def test_symbol_and_date_persisted_verbatim(store):
    """Symbol/date columns must store exactly what was passed (no mangling)."""
    store_csv, db_path = store
    row_id = store_csv("br n.de", "2024-12-31", b"h\n1")
    rows = _query(
        db_path, "SELECT symbol, date FROM raw_csv WHERE id = ?", (row_id,)
    )
    assert rows[0] == ("br n.de", "2024-12-31")


def test_created_at_populated(store):
    """created_at column should be non-NULL for the inserted row."""
    store_csv, db_path = store
    row_id = store_csv("TSLA", "2024-03-03", b"c,d\n5,6")
    rows = _query(
        db_path, "SELECT created_at FROM raw_csv WHERE id = ?", (row_id,)
    )
    assert rows[0][0] is not None, "created_at is NULL"


def test_returned_id_matches_persisted_row(store):
    """The returned id must identify the row actually written."""
    store_csv, db_path = store
    store_csv("AAPL", "2024-01-01", b"first\n1")
    row_id = store_csv("AAPL", "2024-01-01", b"second\n2")
    rows = _query(
        db_path,
        "SELECT csv_bytes FROM raw_csv WHERE id = ? AND symbol = ? AND date = ?",
        (row_id, "AAPL", "2024-01-01"),
    )
    assert len(rows) == 1, "returned id does not match a persisted row"
    assert bytes(rows[0][0]) == b"second\n2"


def test_hermetic_no_default_db_files_created(store, tmp_path, monkeypatch):
    """Calling store_csv must not create ./stock.duckdb in cwd."""
    store_csv, _ = store
    monkeypatch.chdir(tmp_path)
    store_csv("XYZ", "2024-04-04", b"p,q\n9,9")
    leftovers = [p.name for p in tmp_path.iterdir() if p.name.endswith(".duckdb")]
    assert "stock.duckdb" not in leftovers
    assert "stock_challenger.duckdb" not in leftovers
