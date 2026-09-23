"""Tests for the F24-T1 server-side signal pipeline.

Covers ``src/signal_pipeline.py`` (the six ported steps) plus
``IngestionEngine.run_signal_pipeline`` (the legacy ``__main__`` step
order) against a real ``DatabaseManager`` on a temp DuckDB file —
mirroring how ``tests/test_do_put.py`` exercises the service.
"""

from __future__ import annotations

import datetime as dt
import os

import pandas as pd
import pytest

from src import signal_pipeline as sp
from src.config import PipelineConfig, SignalsConfig
from src.db import DatabaseManager, SCHEMA_VERSION
from src.ingestion import IngestionEngine


# =====================================================================
# Helpers
# =====================================================================

@pytest.fixture
def db(tmp_path):
    """Fresh DatabaseManager (schema v7) on a temporary DuckDB file."""
    return DatabaseManager(str(tmp_path / "test.duckdb"))


def _signal_row(**over):
    """A complete 21-column signal row (values pass all prune gates)."""
    row = {
        "Model": "M1",
        "Trading_Date": "2024-01-02",
        "Ticker": "AAA",
        "Open": 100.0,
        "High": 105.0,
        "Low": 99.0,
        "Close": 102.0,
        "Cluster": 1,
        "Hist_WinRate": 60.0,
        "ATR": 2.0,
        "best_k": 3.0,
        "Target": 110.0,
        "Current_Price": 102.0,
        "Stop_Price": 95.0,
        "Target_Percent": 7.8,
        "Stop_Percent": 6.9,
        "Reward_Risk_Ratio": 2.0,
        "Shares_to_buy": 50,
        "Max_Drawdown": 10,
        "Centroid_Dist": 0.5,
        "Drift_Warning": False,
    }
    row.update(over)
    return row


def _write_signal_file(folder, name, rows):
    """Write a signal xlsx file from a list of row dicts."""
    df = pd.DataFrame(rows)
    path = os.path.join(str(folder), name)
    df.to_excel(path, index=False)
    return path


def _insert_signal(con, signal_file="f1.xlsx", **over):
    """Insert one row directly into ``signal_records``."""
    row = _signal_row(**over)
    cols = ["Signal_File"] + sp.REQUIRED_SIGNAL_HEADERS
    placeholders = ", ".join("?" for _ in cols)
    con.execute(
        f"INSERT INTO signal_records ({', '.join(cols)}) "
        f"VALUES ({placeholders})",
        [signal_file] + [row[c] for c in sp.REQUIRED_SIGNAL_HEADERS],
    )


def _seed_prices(con, ticker, start_date, closes):
    """Insert daily_close rows for consecutive calendar dates."""
    base = dt.date.fromisoformat(start_date)
    rows = [
        (base + dt.timedelta(days=i), ticker, c)
        for i, c in enumerate(closes)
    ]
    con.executemany(
        "INSERT INTO daily_close (Date, Ticker, Close) VALUES (?, ?, ?)",
        rows,
    )


# =====================================================================
# Schema v7
# =====================================================================

class TestSchemaV7:
    def test_signal_tables_created(self, db):
        tables = {
            r[0]
            for r in db.conn.execute(
                "SELECT table_name FROM information_schema.tables"
            ).fetchall()
        }
        assert {
            "signal_records",
            "signal_performance",
            "cluster_metadata",
        } <= tables

    def test_schema_version_recorded(self, db):
        assert SCHEMA_VERSION >= 7  # signal-pipeline schema landed at v7
        row = db.conn.execute(
            "SELECT MAX(version) FROM schema_version"
        ).fetchone()
        assert row[0] == SCHEMA_VERSION

    def test_signal_records_column_layout(self, db):
        cols = db.conn.execute(
            "SELECT column_name, data_type "
            "FROM information_schema.columns "
            "WHERE table_name = 'signal_records' ORDER BY ordinal_position"
        ).fetchall()
        names = [c[0] for c in cols]
        assert names == [
            "Signal_File",
        ] + sp.REQUIRED_SIGNAL_HEADERS
        types = dict(cols)
        assert types["Trading_Date"] == "DATE"
        assert types["Open"] == "DECIMAL(12,2)"
        assert types["Cluster"] == "DECIMAL(1,0)"
        assert types["Shares_to_buy"] == "DECIMAL(5,0)"
        assert types["Drift_Warning"] == "BOOLEAN"

    def test_migration_idempotent_on_reopen(self, tmp_path):
        path = str(tmp_path / "reopen.duckdb")
        db1 = DatabaseManager(path)
        db1.close()
        db2 = DatabaseManager(path)  # re-init on existing migrated DB
        row = db2.conn.execute(
            "SELECT MAX(version) FROM schema_version"
        ).fetchone()
        assert row[0] == SCHEMA_VERSION
        # tables still exist, no error
        db2.conn.execute("SELECT COUNT(*) FROM signal_records").fetchone()


# =====================================================================
# Step 1 — ingest
# =====================================================================

class TestIngest:
    def test_valid_file_inserts_rows(self, db, tmp_path):
        folder = tmp_path / "signals"
        folder.mkdir()
        _write_signal_file(
            folder,
            "run_20240101_active_signal_combined.xlsx",
            [_signal_row(), _signal_row(Ticker="BBB")],
        )
        result = sp.ingest_signal_folder(db.conn, str(folder), 3)
        assert result["status"] == "SUCCESS"
        assert result["files_processed"] == 1
        assert result["inserted"] == 2
        assert result["skipped_duplicates"] == 0
        count = db.conn.execute(
            "SELECT COUNT(*) FROM signal_records"
        ).fetchone()[0]
        assert count == 2
        # Signal_File column is stamped with the file name
        sf = db.conn.execute(
            "SELECT DISTINCT Signal_File FROM signal_records"
        ).fetchone()[0]
        assert sf == "run_20240101_active_signal_combined.xlsx"

    def test_header_mismatch_skips_file(self, db, tmp_path):
        folder = tmp_path / "signals"
        folder.mkdir()
        bad = _signal_row()
        bad["Extra_Col"] = 1
        path = _write_signal_file(
            folder, "bad_active_signal_combined.xlsx", [bad]
        )
        result = sp.ingest_signal_folder(db.conn, str(folder), 3)
        assert result["files_processed"] == 0
        assert result["files_skipped"] == 1
        assert result["files"][0]["reason"] == "header mismatch"
        count = db.conn.execute(
            "SELECT COUNT(*) FROM signal_records"
        ).fetchone()[0]
        assert count == 0

    def test_pk_dedup_on_reingest(self, db, tmp_path):
        folder = tmp_path / "signals"
        folder.mkdir()
        _write_signal_file(
            folder, "dup_active_signal_combined.xlsx", [_signal_row()]
        )
        sp.ingest_signal_folder(db.conn, str(folder), 3)
        result = sp.ingest_signal_folder(db.conn, str(folder), 3)
        assert result["inserted"] == 0
        assert result["skipped_duplicates"] == 1
        count = db.conn.execute(
            "SELECT COUNT(*) FROM signal_records"
        ).fetchone()[0]
        assert count == 1

    def test_ignores_wrong_suffix_and_stale_files(self, db, tmp_path):
        folder = tmp_path / "signals"
        folder.mkdir()
        _write_signal_file(
            folder, "other_name.xlsx", [_signal_row()]  # wrong suffix
        )
        stale = _write_signal_file(
            folder, "old_active_signal_combined.xlsx", [_signal_row()]
        )
        old = dt.datetime.now() - dt.timedelta(days=30)
        os.utime(stale, (old.timestamp(), old.timestamp()))
        result = sp.ingest_signal_folder(db.conn, str(folder), 3)
        assert result["status"] == "NO_FILES"
        assert result["files_seen"] == 0

    def test_missing_folder_returns_skipped(self, db, tmp_path):
        result = sp.ingest_signal_folder(
            db.conn, str(tmp_path / "nonexistent"), 3
        )
        assert result["status"] == "SKIPPED"
        assert "folder not found" in result["reason"]

    def test_null_close_ingested_as_null(self, db, tmp_path):
        folder = tmp_path / "signals"
        folder.mkdir()
        row = _signal_row()
        row["Close"] = None
        _write_signal_file(
            folder, "nc_active_signal_combined.xlsx", [row]
        )
        sp.ingest_signal_folder(db.conn, str(folder), 3)
        nulls = db.conn.execute(
            "SELECT COUNT(*) FROM signal_records WHERE Close IS NULL"
        ).fetchone()[0]
        assert nulls == 1


# =====================================================================
# Step 2 — dedupe
# =====================================================================

class TestDedupe:
    def test_keeps_latest_signal_file(self, db):
        _insert_signal(db.conn, "sig_1000.xlsx")
        _insert_signal(db.conn, "sig_1500.xlsx")
        _insert_signal(db.conn, "sig_1200.xlsx")
        result = sp.delete_older_signal_runs(db.conn)
        assert result["deleted"] == 2
        remaining = db.conn.execute(
            "SELECT Signal_File FROM signal_records"
        ).fetchall()
        assert [r[0] for r in remaining] == ["sig_1500.xlsx"]

    def test_no_duplicates(self, db):
        _insert_signal(db.conn, "a.xlsx")
        _insert_signal(db.conn, "b.xlsx", Ticker="BBB")
        result = sp.delete_older_signal_runs(db.conn)
        assert result["deleted"] == 0


# =====================================================================
# Step 3 — repair
# =====================================================================

class TestRepair:
    def test_fills_null_close_from_daily_close(self, db):
        _insert_signal(db.conn, Close=None)
        _seed_prices(db.conn, "AAA", "2024-01-02", [50.0])
        result = sp.repair_missing_close_prices(db.conn)
        assert result["null_before"] == 1
        assert result["repaired"] == 1
        assert result["still_null"] == 0
        close = db.conn.execute(
            "SELECT Close FROM signal_records WHERE Ticker = 'AAA'"
        ).fetchone()[0]
        assert float(close) == 50.0

    def test_unmatched_reported(self, db):
        _insert_signal(db.conn, Close=None, Ticker="ZZZ")
        result = sp.repair_missing_close_prices(db.conn)
        assert result["still_null"] == 1
        assert result["unmatched"] == [
            {"Ticker": "ZZZ", "Trading_Date": "2024-01-02"}
        ]

    def test_no_nulls_is_noop(self, db):
        _insert_signal(db.conn)
        result = sp.repair_missing_close_prices(db.conn)
        assert result["null_before"] == 0
        assert result["repaired"] == 0


# =====================================================================
# Step 4 — prune
# =====================================================================

class TestPrune:
    def test_empty_lists_exclude_nothing_thresholds_active(self, db):
        _insert_signal(db.conn, "good.xlsx")  # RR 2.0 / TP 7.8
        _insert_signal(
            db.conn, "lowrr.xlsx", Ticker="LRR", Reward_Risk_Ratio=1.0
        )
        _insert_signal(
            db.conn, "lowtp.xlsx", Ticker="LTP", Target_Percent=3.0
        )
        result = sp.prune_unwanted_signals(
            db.conn,
            min_reward_risk=1.39,
            min_target_pct=5.5,
            exclude_clusters=[],
            exclude_models=[],
        )
        assert result["pruned_count"] == 2
        assert result["remaining"] == 1
        tickers = {r["Ticker"] for r in result["pruned_records"]}
        assert tickers == {"LRR", "LTP"}
        survivors = db.conn.execute(
            "SELECT Ticker FROM signal_records"
        ).fetchall()
        assert [r[0] for r in survivors] == ["AAA"]

    def test_cluster_and_model_exclusion(self, db):
        _insert_signal(db.conn, "c4.xlsx", Cluster=4)
        _insert_signal(db.conn, "xrt.xlsx", Ticker="B2", Model="XRT")
        result = sp.prune_unwanted_signals(
            db.conn,
            min_reward_risk=1.39,
            min_target_pct=5.5,
            exclude_clusters=[4],
            exclude_models=["XRT"],
        )
        assert result["pruned_count"] == 2
        assert result["remaining"] == 0

    def test_below_open_flag(self, db):
        _insert_signal(
            db.conn, Current_Price=90.0, Open=100.0  # below open
        )
        kept = sp.prune_unwanted_signals(
            db.conn, 1.39, 5.5, [], [], filter_below_open=False
        )
        assert kept["pruned_count"] == 0
        pruned = sp.prune_unwanted_signals(
            db.conn, 1.39, 5.5, [], [], filter_below_open=True
        )
        assert pruned["pruned_count"] == 1


# =====================================================================
# Step 5 — performance CTE branches
# =====================================================================

class TestPerformance:
    """All signals share Trading_Date 2024-01-02, Target 110,
    Stop_Price 95; max_holding_days=4.  Forward prices start
    2024-01-03 — the CTE requires ``p.Date > Trading_Date`` (strict),
    so holding_t=1 is the first day AFTER the signal."""

    def _run(self, db, overwrite=False, days=4):
        return sp.calculate_and_save_performance(
            db.conn, max_holding_days=days, overwrite_existing=overwrite
        )

    def _perf_row(self, db, ticker):
        return db.conn.execute(
            "SELECT Hit_Target_Date, Hit_Target_T, Hit_Stop_Date, "
            "Hit_Stop_T, Time_Exit_Close, T "
            "FROM signal_performance WHERE Ticker = ?",
            [ticker],
        ).fetchone()

    def test_target_first_hit(self, db):
        _insert_signal(db.conn, Ticker="TGT")
        _seed_prices(db.conn, "TGT", "2024-01-03", [105, 111, 108, 100])
        result = self._run(db)
        assert result["rows_processed"] == 1
        row = self._perf_row(db, "TGT")
        assert row[0] == dt.date(2024, 1, 4)  # Hit_Target_Date
        assert row[1] == 2  # Hit_Target_T
        assert row[2] is None and row[3] is None  # no stop hit
        assert row[4] is None  # Time_Exit_Close
        assert row[5] == 2  # T

    def test_stop_first_hit(self, db):
        _insert_signal(db.conn, Ticker="STP")
        _seed_prices(
            db.conn, "STP", "2024-01-03", [94, 100, 101, 102]
        )
        self._run(db)
        row = self._perf_row(db, "STP")
        assert row[2] == dt.date(2024, 1, 3)  # Hit_Stop_Date
        assert row[3] == 1  # Hit_Stop_T
        assert row[0] is None
        assert row[4] is None
        assert row[5] == 1

    def test_both_hits_least_wins(self, db):
        _insert_signal(db.conn, Ticker="BOTH")
        # stop hit t=2 (94 <= 95), target hit t=4 (112 >= 110)
        _seed_prices(
            db.conn, "BOTH", "2024-01-03", [100, 94, 105, 112]
        )
        self._run(db)
        row = self._perf_row(db, "BOTH")
        assert row[3] == 2  # Hit_Stop_T
        assert row[1] == 4  # Hit_Target_T
        assert row[4] is None  # no time-exit when a hit occurred
        assert row[5] == 2  # LEAST(4, 2)

    def test_time_exit_when_neither_hit(self, db):
        _insert_signal(db.conn, Ticker="FLAT")
        # 5 forward days, all flat — neither target nor stop hit
        _seed_prices(
            db.conn, "FLAT", "2024-01-03", [100, 100, 100, 100, 100]
        )
        self._run(db)
        row = self._perf_row(db, "FLAT")
        assert row[1] is None and row[3] is None
        assert float(row[4]) == 100.0  # Time_Exit_Close at t=4
        assert row[5] == 4  # T = max_holding_days

    def test_insufficient_forward_days_skipped(self, db):
        _insert_signal(db.conn, Ticker="SHORT")
        # only 2 forward days, no hit — below max_holding_days
        _seed_prices(db.conn, "SHORT", "2024-01-03", [100, 100])
        result = self._run(db)
        assert result["rows_processed"] == 0
        assert self._perf_row(db, "SHORT") is None

    def test_missing_ticker_reported(self, db):
        _insert_signal(db.conn, Ticker="NOPX")
        result = self._run(db)
        assert result["missing_tickers"] == ["NOPX"]
        assert self._perf_row(db, "NOPX") is None

    def test_overwrite_vs_append(self, db):
        _insert_signal(db.conn, Ticker="OVR")
        _seed_prices(
            db.conn, "OVR", "2024-01-03", [100, 105, 111, 108]
        )
        self._run(db, overwrite=True)
        assert self._perf_row(db, "OVR")[5] == 3

        # Lower the target so it is hit at t=1 on recompute.
        db.conn.execute(
            "UPDATE signal_records SET Target = 100 "
            "WHERE Ticker = 'OVR'"
        )
        self._run(db, overwrite=False)  # append mode — keeps old row
        assert self._perf_row(db, "OVR")[5] == 3
        self._run(db, overwrite=True)  # replace — updated
        assert self._perf_row(db, "OVR")[5] == 1


# =====================================================================
# Step 6 — cluster_metadata
# =====================================================================

class TestClusterMetadata:
    def test_six_rows_verbatim(self, db):
        result = sp.setup_cluster_metadata(db.conn)
        assert result["rows"] == 6
        rows = dict(
            db.conn.execute(
                "SELECT Cluster, Description FROM cluster_metadata"
            ).fetchall()
        )
        assert rows == {
            0: "0: Bull Market Drift",
            1: "1: High-Confidence V-Recovery",
            2: "2: Volatile Mean Reversion",
            3: "3: Momentum Dip",
            4: "4: Panic / Deep Stress",
            5: "5: Others",
        }


# =====================================================================
# IngestionEngine.run_signal_pipeline — legacy __main__ step order
# =====================================================================

class TestRunSignalPipeline:
    def _engine(self, db, folder):
        cfg = PipelineConfig(
            signals=SignalsConfig(folder_path=str(folder))
        )
        return IngestionEngine(db, cfg)

    def test_step_order_and_summary(self, db, tmp_path):
        folder = tmp_path / "signals"
        folder.mkdir()
        _write_signal_file(
            folder,
            "run_active_signal_combined.xlsx",
            [
                _signal_row(Close=None),  # repaired by step 3
                _signal_row(
                    Ticker="PRN", Reward_Risk_Ratio=1.0
                ),  # pruned by step 4 (1.0 < 1.39)
            ],
        )
        _seed_prices(db.conn, "AAA", "2024-01-02", [50.0, 55, 60, 100])
        engine = self._engine(db, folder)
        result = engine.run_signal_pipeline()

        assert result["status"] == "SUCCESS"
        steps = result["steps"]
        assert list(steps.keys()) == [
            "ingest",
            "dedupe",
            "repair",
            "prune",
            "performance",
            "repair_sweep",
            "null_verification",
            "cluster_metadata",
        ]
        assert steps["ingest"]["inserted"] == 2
        assert steps["repair"]["repaired"] == 1
        assert steps["prune"]["pruned_count"] == 1
        assert steps["prune"]["pruned_records"][0]["Ticker"] == "PRN"
        assert (
            steps["null_verification"]["null_close_remaining"] == 0
        )
        assert steps["cluster_metadata"]["rows"] == 6
        # AAA survives (repaired + not pruned); PRN is gone.
        tickers = {
            r[0]
            for r in db.conn.execute(
                "SELECT Ticker FROM signal_records"
            ).fetchall()
        }
        assert tickers == {"AAA"}

    def test_missing_folder_still_succeeds(self, db, tmp_path):
        engine = self._engine(db, tmp_path / "nonexistent")
        result = engine.run_signal_pipeline()
        assert result["status"] == "SUCCESS"
        assert result["steps"]["ingest"]["status"] == "SKIPPED"

    def test_config_defaults_applied(self, db, tmp_path):
        folder = tmp_path / "signals"
        folder.mkdir()
        # RR below the 1.39 default must be pruned even though no
        # overrides were passed — thresholds come from config.
        _write_signal_file(
            folder,
            "cfg_active_signal_combined.xlsx",
            [_signal_row(Reward_Risk_Ratio=1.0)],
        )
        engine = self._engine(db, folder)
        result = engine.run_signal_pipeline()
        assert result["steps"]["prune"]["pruned_count"] == 1
