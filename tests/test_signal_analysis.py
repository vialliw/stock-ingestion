"""Tests for the F25-T1 server-side signal-analysis pipeline.

Covers ``src/signal_analysis.py`` (the five ported phases —
``aly_``-prefixed objects inside per-set ``signals_<name>`` schemas)
plus ``IngestionEngine.run_signal_analysis`` (the
``process_mutli_sets_signals`` multi-set loop) against a real
``DatabaseManager`` on a temp DuckDB file — mirroring how
``tests/test_signal_pipeline.py`` exercises the F24 pipeline.
"""

from __future__ import annotations

import datetime as dt
import os

import openpyxl
import pandas as pd
import pytest

from src import signal_analysis as sa
from src.config import (
    PipelineConfig,
    ReportsConfig,
    SignalAnalysisConfig,
    SignalAnalysisSetConfig,
)
from src.db import DatabaseManager
from src.ingestion import IngestionEngine


# =====================================================================
# Helpers
# =====================================================================

TODAY = dt.date.today()
RECENT = TODAY - dt.timedelta(days=10)


@pytest.fixture
def db(tmp_path):
    """Fresh DatabaseManager (schema v7) on a temporary DuckDB file."""
    return DatabaseManager(str(tmp_path / "test.duckdb"))


def _signal_row(**over):
    """A complete signal row as it appears in an
    ``*_active_signal_combined.xlsx`` file (21 data columns)."""
    row = {
        "Model": "M1",
        "Trading_Date": RECENT.isoformat(),
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


def _write_signal_file(folder, name, rows, sheets=None):
    """Write a signal xlsx — ``sheets`` maps sheet name -> row list;
    when None, ``rows`` lands on a single default sheet."""
    os.makedirs(str(folder), exist_ok=True)
    path = os.path.join(str(folder), name)
    with pd.ExcelWriter(path, engine="openpyxl") as w:
        if sheets is None:
            pd.DataFrame(rows).to_excel(w, index=False)
        else:
            for sheet_name, sheet_rows in sheets.items():
                pd.DataFrame(sheet_rows).to_excel(
                    w, sheet_name=sheet_name, index=False
                )
    return path


def _fname(day, hhmm):
    return f"{day.strftime('%Y%m%d')}_{hhmm}_active_signal_combined.xlsx"


def _create_raw(con, schema):
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    sa.setup_raw_table_schema(con, schema)


def _insert_raw(con, schema, signal_file="f1.xlsx", intraday=False, **over):
    """Insert one row directly into ``{schema}.aly_signal_records_raw``."""
    _create_raw(con, schema)
    row = _signal_row(**over)
    cols = [
        "Signal_File", "Model", "Trading_Date", "Ticker", "Open",
        "High", "Low", "Close", "Cluster", "Hist_WinRate", "ATR",
        "best_k", "Target", "Current_Price", "Stop_Price",
        "Target_Percent", "Stop_Percent", "Reward_Risk_Ratio",
        "Shares_to_buy", "Max_Drawdown", "Centroid_Dist",
        "Drift_Warning", "Is_Intraday",
    ]
    placeholders = ", ".join("?" for _ in cols)
    con.execute(
        f"INSERT INTO {schema}.aly_signal_records_raw "
        f"({', '.join(cols)}) VALUES ({placeholders})",
        [signal_file]
        + [row[c] for c in cols[1:-1]]
        + [intraday],
    )


def _seed_prices(
    con, ticker, start_date, bars, step_days=1
):
    """Insert daily_close rows.

    ``bars`` is a list of ``(Open, High, Low, Close)`` tuples for
    consecutive days starting ``step_days`` after ``start_date``.
    """
    base = dt.date.fromisoformat(str(start_date))
    rows = [
        (base + dt.timedelta(days=i + step_days), ticker, o, h, l, c)
        for i, (o, h, l, c) in enumerate(bars)
    ]
    con.executemany(
        "INSERT INTO daily_close (Date, Ticker, Open, High, Low, Close) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        rows,
    )


def _flat_bars(n, o=100.0, h=101.0, l=99.0, c=100.0):
    """``n`` days of neutral bars — never hits target/stop."""
    return [(o, h, l, c)] * n


def _run_perf(con, schema, report_dir, max_holding_days=8):
    return sa.calculate_and_save_performance(
        con, schema, report_dir, max_holding_days=max_holding_days
    )


def _perf_row(con, schema, ticker="AAA"):
    cols = [
        r[0]
        for r in con.execute(
            "SELECT column_name FROM information_schema.columns "
            f"WHERE table_schema = '{schema}' "
            "AND table_name = 'aly_signal_performance' "
            "ORDER BY ordinal_position"
        ).fetchall()
    ]
    row = con.execute(
        f"SELECT * FROM {schema}.aly_signal_performance "
        f"WHERE Ticker = '{ticker}'"
    ).fetchone()
    return dict(zip(cols, row)) if row else None


# =====================================================================
# Phase 1 — is_intraday_signal (London -> NYSE RTH)
# =====================================================================

class TestIsIntradaySignal:
    """London-time filename timestamps converted to NY RTH 09:30-16:00.

    July dates: London is BST (UTC+1), NY is EDT (UTC-4) — London is
    5h ahead.  January dates: London GMT, NY EST — also 5h ahead.
    """

    def test_no_timestamp_returns_false(self):
        assert sa.is_intraday_signal("no_timestamp_here.xlsx") is False

    def test_invalid_date_returns_false(self):
        assert sa.is_intraday_signal("20261340_1200_f.xlsx") is False

    def test_summer_bst_within_rth(self):
        # 2026-07-17 16:30 BST = 11:30 EDT (Friday, trading day)
        assert sa.is_intraday_signal("x_20260717_1630_x.xlsx") is True

    def test_summer_bst_before_open(self):
        # 13:16 BST = 08:16 EDT — before the 09:30 open
        assert sa.is_intraday_signal("x_20260717_1316_x.xlsx") is False

    def test_summer_rth_boundaries(self):
        # 14:30 BST = 09:30 EDT (inclusive) / 21:00 BST = 16:00 EDT
        assert sa.is_intraday_signal("x_20260717_1430_x.xlsx") is True
        assert sa.is_intraday_signal("x_20260717_2100_x.xlsx") is True
        # 21:01 BST = 16:01 EDT — just past the close
        assert sa.is_intraday_signal("x_20260717_2101_x.xlsx") is False

    def test_winter_gmt_within_rth(self):
        # 2026-01-15 15:30 GMT = 10:30 EST (Thursday)
        assert sa.is_intraday_signal("x_20260115_1530_x.xlsx") is True
        # 14:29 GMT = 09:29 EST — just before the open
        assert sa.is_intraday_signal("x_20260115_1429_x.xlsx") is False

    def test_dst_transition_week(self):
        # 2026-10-30: London already back on GMT (BST ended Oct 25) but
        # NY still on EDT (ends Nov 1) — London only 4h ahead.
        # 13:30 GMT = 09:30 EDT -> True (a naive fixed-5h conversion
        # would say 08:30 -> False).
        assert sa.is_intraday_signal("x_20261030_1330_x.xlsx") is True
        # Mirror week: 2026-03-09 — NY on EDT (since Mar 8), London
        # still GMT (BST starts Mar 29) — again only 4h ahead:
        # 13:30 GMT = 09:30 EDT -> True.
        assert sa.is_intraday_signal("x_20260309_1330_x.xlsx") is True

    def test_weekend_returns_false(self):
        # 2026-07-18 is a Saturday
        assert sa.is_intraday_signal("x_20260718_1600_x.xlsx") is False

    def test_holiday_returns_false(self):
        # 2026-07-03 — Independence Day observed (Jul 4 falls Saturday)
        assert sa.is_intraday_signal("x_20260703_1600_x.xlsx") is False


# =====================================================================
# Phase 1 — ingest_signals
# =====================================================================

class TestIngestSignals:
    def test_creates_aly_table_in_schema(self, db, tmp_path):
        folder = tmp_path / "signals"
        _write_signal_file(folder, _fname(TODAY, "1500"), [_signal_row()])
        res = sa.ingest_signals(db.conn, "signals_t", str(folder))
        assert res["status"] == "SUCCESS"
        cols = [
            r[0]
            for r in db.conn.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'signals_t' "
                "AND table_name = 'aly_signal_records_raw' "
                "ORDER BY ordinal_position"
            ).fetchall()
        ]
        assert cols == [
            "Signal_File", "Model", "Trading_Date", "Ticker", "Open",
            "High", "Low", "Close", "Cluster", "Hist_WinRate", "ATR",
            "best_k", "Target", "Current_Price", "Stop_Price",
            "Target_Percent", "Stop_Percent", "Reward_Risk_Ratio",
            "Shares_to_buy", "Max_Drawdown", "Centroid_Dist",
            "Drift_Warning", "Is_Intraday", "Ingested_At",
        ]
        assert db.conn.execute(
            "SELECT COUNT(1) FROM signals_t.aly_signal_records_raw"
        ).fetchone()[0] == 1

    def test_all_sheets_ingested(self, db, tmp_path):
        folder = tmp_path / "signals"
        _write_signal_file(
            folder,
            _fname(TODAY, "1500"),
            None,
            sheets={
                "S1": [_signal_row(Ticker="AAA")],
                "S2": [_signal_row(Ticker="BBB")],
                "S3": [_signal_row(Ticker="CCC")],
            },
        )
        res = sa.ingest_signals(db.conn, "signals_t", str(folder))
        assert res["sheets_processed"] == 3
        tickers = {
            r[0]
            for r in db.conn.execute(
                "SELECT Ticker FROM signals_t.aly_signal_records_raw"
            ).fetchall()
        }
        assert tickers == {"AAA", "BBB", "CCC"}

    def test_trading_date_cutoff(self, db, tmp_path):
        folder = tmp_path / "signals"
        old_date = TODAY - dt.timedelta(days=400)
        _write_signal_file(
            folder,
            _fname(TODAY, "1500"),
            [
                _signal_row(Ticker="NEW", Trading_Date=RECENT.isoformat()),
                _signal_row(Ticker="OLD", Trading_Date=old_date.isoformat()),
            ],
        )
        sa.ingest_signals(db.conn, "signals_t", str(folder), backdate_days=90)
        tickers = {
            r[0]
            for r in db.conn.execute(
                "SELECT Ticker FROM signals_t.aly_signal_records_raw"
            ).fetchall()
        }
        assert tickers == {"NEW"}

    def test_drop_recreates_per_run(self, db, tmp_path):
        folder = tmp_path / "signals"
        _write_signal_file(
            folder, _fname(TODAY, "1500"), [_signal_row(Ticker="AAA")]
        )
        sa.ingest_signals(db.conn, "signals_t", str(folder))
        # Second run ingests a different set — the table is rebuilt
        # (legacy DROP+recreate, not append).
        os.remove(
            os.path.join(str(folder), _fname(TODAY, "1500"))
        )
        _write_signal_file(
            folder, _fname(TODAY, "1600"), [_signal_row(Ticker="ZZZ")]
        )
        sa.ingest_signals(db.conn, "signals_t", str(folder))
        rows = db.conn.execute(
            "SELECT Ticker, Signal_File "
            "FROM signals_t.aly_signal_records_raw"
        ).fetchall()
        assert rows == [("ZZZ", _fname(TODAY, "1600"))]

    def test_empty_folder_no_files(self, db, tmp_path):
        folder = tmp_path / "empty"
        folder.mkdir()
        res = sa.ingest_signals(db.conn, "signals_t", str(folder))
        assert res["status"] == "NO_FILES"
        assert res["files_seen"] == 0
        # Verbatim legacy: no files -> the table is not created.
        assert not sa._table_exists(
            db.conn, "signals_t", sa.RAW_TABLE
        )

    def test_missing_folder_graceful(self, db, tmp_path):
        res = sa.ingest_signals(
            db.conn, "signals_t", str(tmp_path / "missing")
        )
        assert res["status"] == "NO_FILES"

    def test_insert_or_replace_dedups_pk(self, db, tmp_path):
        """Same PK on two sheets of one file -> a single row."""
        folder = tmp_path / "signals"
        _write_signal_file(
            folder,
            _fname(TODAY, "1500"),
            None,
            sheets={
                "S1": [_signal_row(Ticker="AAA", Close=101.0)],
                "S2": [_signal_row(Ticker="AAA", Close=103.0)],
            },
        )
        sa.ingest_signals(db.conn, "signals_t", str(folder))
        rows = db.conn.execute(
            "SELECT Ticker, Close FROM signals_t.aly_signal_records_raw"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "AAA"

    def test_is_intraday_flag_stored(self, db, tmp_path):
        folder = tmp_path / "signals"
        # 15:00 London summer = 10:00 EDT -> intraday True
        _write_signal_file(
            folder, _fname(TODAY, "1500"), [_signal_row()]
        )
        sa.ingest_signals(db.conn, "signals_t", str(folder))
        flag = db.conn.execute(
            "SELECT Is_Intraday FROM signals_t.aly_signal_records_raw"
        ).fetchone()[0]
        today_sched = sa._nyse.schedule(
            start_date=TODAY, end_date=TODAY
        )
        # On a trading day 15:00 London maps inside RTH; on a holiday
        # or weekend the flag is False — assert consistency with the
        # calendar rather than a hard-coded bool.
        assert flag == (not today_sched.empty)


# =====================================================================
# Phase 2 — dedupe & repair
# =====================================================================

class TestDedupAndRepair:
    def test_dedupe_keeps_latest_signal_file(self, db):
        schema = "signals_t"
        _insert_raw(
            db.conn, schema, signal_file="20260101_0900_f.xlsx",
            Ticker="AAA", Close=100.0,
        )
        _insert_raw(
            db.conn, schema, signal_file="20260102_0900_f.xlsx",
            Ticker="AAA", Close=101.0,
        )
        _insert_raw(
            db.conn, schema, signal_file="20260101_0900_f.xlsx",
            Ticker="BBB", Close=50.0,
        )
        res = sa.deduplicate_signals(db.conn, schema)
        assert res == {"deleted": 1, "remaining": 2}
        rows = db.conn.execute(
            "SELECT Ticker, Signal_File, Close "
            f"FROM {schema}.aly_signal_records_raw ORDER BY Ticker"
        ).fetchall()
        assert rows == [
            ("AAA", "20260102_0900_f.xlsx", 101.0),
            ("BBB", "20260101_0900_f.xlsx", 50.0),
        ]

    def test_dedupe_no_duplicates(self, db):
        schema = "signals_t"
        _insert_raw(db.conn, schema, Ticker="AAA")
        _insert_raw(db.conn, schema, Ticker="BBB")
        res = sa.deduplicate_signals(db.conn, schema)
        assert res == {"deleted": 0, "remaining": 2}

    def test_repair_null_and_nan_close(self, db):
        """Both ``Close IS NULL`` and ``isnan(Close)`` are repaired from
        daily_close (verbatim legacy WHERE clause)."""
        schema = "signals_t"
        _create_raw(db.conn, schema)
        db.conn.execute(
            f"INSERT INTO {schema}.aly_signal_records_raw "
            "(Signal_File, Model, Trading_Date, Ticker, Close, "
            " Is_Intraday) VALUES "
            "('f1', 'M1', ?, 'AAA', NULL, false),"
            "('f1', 'M1', ?, 'BBB', 'NaN'::DOUBLE, false)",
            [RECENT, RECENT],
        )
        _seed_prices(
            db.conn, "AAA", RECENT - dt.timedelta(days=1),
            [(100.0, 101.0, 99.0, 111.0)], step_days=1,
        )
        _seed_prices(
            db.conn, "BBB", RECENT - dt.timedelta(days=1),
            [(200.0, 201.0, 199.0, 222.0)], step_days=1,
        )
        res = sa.repair_missing_close_prices(db.conn, schema)
        assert res["null_before"] == 2
        assert res["repaired"] == 2
        assert res["still_null"] == 0
        closes = {
            r[0]: r[1]
            for r in db.conn.execute(
                f"SELECT Ticker, Close FROM {schema}.aly_signal_records_raw"
            ).fetchall()
        }
        assert closes == {"AAA": 111.0, "BBB": 222.0}

    def test_repair_unmatched_reported(self, db):
        schema = "signals_t"
        _create_raw(db.conn, schema)
        db.conn.execute(
            f"INSERT INTO {schema}.aly_signal_records_raw "
            "(Signal_File, Model, Trading_Date, Ticker, Close, "
            " Is_Intraday) VALUES ('f1', 'M1', ?, 'NOPE', NULL, false)",
            [RECENT],
        )
        res = sa.repair_missing_close_prices(db.conn, schema)
        assert res["null_before"] == 1
        assert res["repaired"] == 0
        assert res["still_null"] == 1
        assert res["unmatched"] == [
            {"Ticker": "NOPE", "Trading_Date": RECENT.isoformat()}
        ]


# =====================================================================
# Phase 3 — views
# =====================================================================

class TestBuildViews:
    def _seed(self, db):
        """Four rows exercising each view predicate boundary."""
        schema = "signals_t"
        # A: baseline only — intraday, RR below thresholds
        _insert_raw(
            db.conn, schema, Ticker="AAA", intraday=True,
            Reward_Risk_Ratio=1.0, Current_Price=90.0, Open=100.0,
        )
        # B: strict_eod — EOD, price above open, RR >= 1.2 (but target
        # too close to pass the ATR-multiple filter: (103-102)/2 = 0.5)
        _insert_raw(
            db.conn, schema, Ticker="BBB", intraday=False,
            Reward_Risk_Ratio=1.3, Current_Price=102.0, Open=100.0,
            Target=103.0, ATR=2.0,
        )
        # C: intraday_high_rr — intraday, RR >= 1.5
        _insert_raw(
            db.conn, schema, Ticker="CCC", intraday=True,
            Reward_Risk_Ratio=1.6, Current_Price=90.0, Open=100.0,
        )
        # D: atr filter — above open, RR >= 1.0, target dist >= 1.5 ATR
        _insert_raw(
            db.conn, schema, Ticker="DDD", intraday=True,
            Reward_Risk_Ratio=1.2, Current_Price=102.0, Open=100.0,
            Target=106.0, ATR=2.0,  # (106-102)/2 = 2.0 >= 1.5
        )
        # E: atr filter fails — target too close (1.0x ATR)
        _insert_raw(
            db.conn, schema, Ticker="EEE", intraday=False,
            Reward_Risk_Ratio=1.3, Current_Price=102.0, Open=100.0,
            Target=104.0, ATR=2.0,  # (104-102)/2 = 1.0 < 1.5
        )
        return schema

    def test_all_four_views_predicates(self, db):
        schema = self._seed(db)
        res = sa.build_views(db.conn, schema)
        assert res["status"] == "SUCCESS"
        expected = {
            "aly_v_signals_baseline_all_signals": {
                "AAA", "BBB", "CCC", "DDD", "EEE"
            },
            "aly_v_signals_strict_eod_price_above_open": {"BBB", "EEE"},
            "aly_v_signals_intraday_high_rr": {"CCC"},
            "aly_v_signals_atr_target_multiple_filter": {"DDD"},
        }
        for view, tickers in expected.items():
            got = {
                r[0]
                for r in db.conn.execute(
                    f"SELECT Ticker FROM {schema}.{view}"
                ).fetchall()
            }
            assert got == tickers, view
            assert res["views"][view] == len(tickers)

    def test_views_carry_aly_prefix(self, db):
        schema = self._seed(db)
        sa.build_views(db.conn, schema)
        views = [
            r[0]
            for r in db.conn.execute(
                "SELECT table_name FROM information_schema.tables "
                f"WHERE table_schema = '{schema}' "
                "AND table_type = 'VIEW'"
            ).fetchall()
        ]
        assert sorted(views) == sorted(sa.ALL_VIEWS)
        assert all(v.startswith("aly_") for v in views)

    def test_missing_raw_table(self, db):
        db.conn.execute("CREATE SCHEMA IF NOT EXISTS signals_empty")
        res = sa.build_views(db.conn, "signals_empty")
        assert res["status"] == "NO_TABLE"
        assert res["views"] == {}


# =====================================================================
# Phase 4 — performance CTE
# =====================================================================

class TestPerformance:
    def test_perf_table_15_columns(self, db, tmp_path):
        schema = "signals_t"
        _insert_raw(db.conn, schema, Ticker="AAA")
        _seed_prices(
            db.conn, "AAA", RECENT, _flat_bars(10)
        )
        _run_perf(db.conn, schema, str(tmp_path))
        cols = [
            r[0]
            for r in db.conn.execute(
                "SELECT column_name FROM information_schema.columns "
                f"WHERE table_schema = '{schema}' "
                "AND table_name = 'aly_signal_performance' "
                "ORDER BY ordinal_position"
            ).fetchall()
        ]
        assert cols == [
            "Ticker", "Trading_Date", "Cluster", "Model", "Signal_File",
            "Hit_Target_Date", "Hit_Target_T", "Hit_Stop_Date",
            "Hit_Stop_T", "Time_Exit_Date", "Time_Exit_Close", "T",
            "Next_Day_Open", "Open_Gap_Pct", "Execution_Status",
        ]

    def test_target_hit_first(self, db, tmp_path):
        schema = "signals_t"
        # Current 100, Target 105, Stop 90 — t2 close 106 >= target.
        _insert_raw(
            db.conn, schema, Ticker="AAA", Current_Price=100.0,
            Close=100.0, Target=105.0, Stop_Price=90.0,
        )
        bars = [(100.0, 101.0, 99.0, 100.0)] + [
            (100.0, 107.0, 99.0, 106.0)
        ] + _flat_bars(8)
        _seed_prices(db.conn, "AAA", RECENT, bars)
        _run_perf(db.conn, schema, str(tmp_path))
        row = _perf_row(db.conn, schema)
        assert row["Hit_Target_T"] == 2
        assert row["Hit_Target_Date"] is not None
        assert row["Hit_Stop_Date"] is None
        assert row["T"] == 2

    def test_stop_hit(self, db, tmp_path):
        schema = "signals_t"
        _insert_raw(
            db.conn, schema, Ticker="AAA", Current_Price=100.0,
            Close=100.0, Target=110.0, Stop_Price=95.0,
        )
        bars = [(100.0, 101.0, 94.0, 94.0)] + _flat_bars(9)
        _seed_prices(db.conn, "AAA", RECENT, bars)
        _run_perf(db.conn, schema, str(tmp_path))
        row = _perf_row(db.conn, schema)
        assert row["Hit_Stop_T"] == 1
        assert row["Hit_Target_Date"] is None
        assert row["T"] == 1

    def test_both_hit_same_day_stop_wins(self, db, tmp_path):
        """The F25 tie-break differs from F24: a day where close hits
        BOTH target and stop (possible when Stop_Price >= Target —
        degenerate but exercised by the legacy predicate) resolves to
        the STOP — target requires ``Raw_Target_T < Raw_Stop_T``
        strictly; stop wins ``Raw_Stop_T <= Raw_Target_T``."""
        schema = "signals_t"
        _insert_raw(
            db.conn, schema, Ticker="AAA", Current_Price=100.0,
            Close=100.0, Target=105.0, Stop_Price=105.0,
        )
        # Day-1 close lands exactly on the shared level: >= target AND
        # <= stop both fire -> Raw_Target_T == Raw_Stop_T == 1.
        bars = [(100.0, 106.0, 99.0, 105.0)] + _flat_bars(9)
        _seed_prices(db.conn, "AAA", RECENT, bars)
        _run_perf(db.conn, schema, str(tmp_path))
        row = _perf_row(db.conn, schema)
        assert row["Hit_Stop_T"] == 1
        assert row["Hit_Stop_Date"] is not None
        # target NULL — stop wins the same-day tie
        assert row["Hit_Target_Date"] is None
        assert row["Hit_Target_T"] is None
        assert row["T"] == 1

    def test_time_exit_when_neither_hits(self, db, tmp_path):
        schema = "signals_t"
        _insert_raw(
            db.conn, schema, Ticker="AAA", Current_Price=100.0,
            Close=100.0, Target=150.0, Stop_Price=50.0,
        )
        bars = _flat_bars(12, c=101.0)
        _seed_prices(db.conn, "AAA", RECENT, bars)
        _run_perf(db.conn, schema, str(tmp_path), max_holding_days=8)
        row = _perf_row(db.conn, schema)
        assert row["Hit_Target_Date"] is None
        assert row["Hit_Stop_Date"] is None
        # window-end = 8th forward day
        expected_exit = RECENT + dt.timedelta(days=8)
        assert row["Time_Exit_Date"] == expected_exit
        assert float(row["Time_Exit_Close"]) == 101.0
        assert row["T"] == 8

    def test_insufficient_forward_days_skipped(self, db, tmp_path):
        schema = "signals_t"
        _insert_raw(
            db.conn, schema, Ticker="AAA", Current_Price=100.0,
            Close=100.0, Target=150.0, Stop_Price=50.0,
        )
        _seed_prices(db.conn, "AAA", RECENT, _flat_bars(3))
        _run_perf(db.conn, schema, str(tmp_path))
        assert _perf_row(db.conn, schema) is None

    def test_execution_status_1_fillable(self, db, tmp_path):
        """T1_Open < Target and T1_Low <= Current_Price -> 1."""
        schema = "signals_t"
        _insert_raw(
            db.conn, schema, Ticker="AAA", Current_Price=100.0,
            Close=100.0, Target=110.0, Stop_Price=50.0,
        )
        bars = [(101.0, 102.0, 99.0, 100.0)] + _flat_bars(9)
        _seed_prices(db.conn, "AAA", RECENT, bars)
        _run_perf(db.conn, schema, str(tmp_path))
        row = _perf_row(db.conn, schema)
        assert row["Execution_Status"] == 1

    def test_execution_status_2_open_above_target(self, db, tmp_path):
        """T1_Open >= Target -> 2."""
        schema = "signals_t"
        _insert_raw(
            db.conn, schema, Ticker="AAA", Current_Price=100.0,
            Close=100.0, Target=110.0, Stop_Price=50.0,
        )
        bars = [(111.0, 112.0, 110.0, 111.0)] + _flat_bars(9)
        _seed_prices(db.conn, "AAA", RECENT, bars)
        _run_perf(db.conn, schema, str(tmp_path))
        row = _perf_row(db.conn, schema)
        assert row["Execution_Status"] == 2

    def test_execution_status_3_gapped_up(self, db, tmp_path):
        """T1_Open < Target but T1_Low > Current_Price -> 3."""
        schema = "signals_t"
        _insert_raw(
            db.conn, schema, Ticker="AAA", Current_Price=100.0,
            Close=100.0, Target=110.0, Stop_Price=50.0,
        )
        bars = [(103.0, 104.0, 102.5, 103.0)] + _flat_bars(9)
        _seed_prices(db.conn, "AAA", RECENT, bars)
        _run_perf(db.conn, schema, str(tmp_path))
        row = _perf_row(db.conn, schema)
        assert row["Execution_Status"] == 3

    def test_next_day_open_and_gap(self, db, tmp_path):
        schema = "signals_t"
        _insert_raw(
            db.conn, schema, Ticker="AAA", Current_Price=100.0,
            Close=100.0, Target=150.0, Stop_Price=50.0,
        )
        bars = [(104.0, 105.0, 99.0, 100.0)] + _flat_bars(9)
        _seed_prices(db.conn, "AAA", RECENT, bars)
        _run_perf(db.conn, schema, str(tmp_path))
        row = _perf_row(db.conn, schema)
        assert float(row["Next_Day_Open"]) == 104.0
        # (104 - 100) / 100 * 100 = 4.0
        assert float(row["Open_Gap_Pct"]) == pytest.approx(4.0)

    def test_excel_report_written(self, db, tmp_path):
        schema = "signals_t"
        report_dir = str(tmp_path / "reports" / "signal_analysis" / "t")
        _insert_raw(db.conn, schema, Ticker="AAA")
        _seed_prices(db.conn, "AAA", RECENT, _flat_bars(10))
        res = _run_perf(db.conn, schema, report_dir)
        path = res["excel_report"]["path"]
        assert path == os.path.join(
            report_dir, "RPT_signal_performance.xlsx"
        )
        assert os.path.exists(path)
        wb = openpyxl.load_workbook(path)
        assert "Signal Performance" in wb.sheetnames

    def test_integrity_check_counts_returned(self, db, tmp_path):
        schema = "signals_t"
        _insert_raw(db.conn, schema, Ticker="AAA")
        _seed_prices(db.conn, "AAA", RECENT, _flat_bars(10))
        res = _run_perf(db.conn, schema, str(tmp_path))
        assert res["execution_status_integrity"] == {
            "err1": 0,
            "err2": 0,
            "err3": 0,
        }


# =====================================================================
# Phase 4 — scorecard + edge validator
# =====================================================================

class TestScorecardAndValidator:
    def _seed_full(self, db, tmp_path):
        schema = "signals_t"
        _insert_raw(db.conn, schema, Ticker="AAA", intraday=True,
                    Reward_Risk_Ratio=2.0, Current_Price=100.0,
                    Close=100.0, Target=110.0, Stop_Price=90.0)
        _insert_raw(db.conn, schema, Ticker="BBB", intraday=False,
                    Reward_Risk_Ratio=2.0, Current_Price=100.0,
                    Close=100.0, Target=110.0, Stop_Price=90.0)
        for ticker in ("AAA", "BBB", "SPY"):
            bars = [(100.0, 101.0, 99.0, 100.0)] + [
                (100.0, 112.0, 99.0, 111.0)
            ] + _flat_bars(9)
            _seed_prices(db.conn, ticker, RECENT, bars)
        sa.build_views(db.conn, schema)
        return sa.run_backtest(
            db.conn, schema, str(tmp_path), mc_iterations=3
        )

    def test_scorecard_rows_and_keys(self, db, tmp_path):
        res = self._seed_full(db, tmp_path)
        assert len(res["scorecard"]) == 4
        keys = set(res["scorecard"][0])
        assert keys == {
            "Config / View",
            "Total Trades",
            "Target Hits",
            "Stop Hits",
            "Time Exits",
            "Realized Target Hits (Filled)",
            "Unfilled Target Hits",
        }
        labels = {r["Config / View"] for r in res["scorecard"]}
        assert labels == {
            "BASELINE_ALL_SIGNALS",
            "STRICT_EOD_PRICE_ABOVE_OPEN",
            "INTRADAY_HIGH_RR",
            "ATR_TARGET_MULTIPLE_FILTER",
        }
        baseline = next(
            r for r in res["scorecard"]
            if r["Config / View"] == "BASELINE_ALL_SIGNALS"
        )
        assert baseline["Total Trades"] == 2
        assert baseline["Target Hits"] == 2

    def test_edge_validator_metric_keys(self, db, tmp_path):
        res = self._seed_full(db, tmp_path)
        edge = res["edge_validation"]
        assert set(edge) == {
            "Total Trades Evaluated",
            "Win Rate (%)",
            "Avg Win (%)",
            "Avg Loss (%)",
            "Expectancy (%)",
            "Profit Factor",
            "Valid Fill Rate (%)",
            "SPY Excess Alpha (%)",
            "Monte Carlo Random Win Rate (%)",
            "Timing Edge Over Random (%)",
        }
        assert edge["Total Trades Evaluated"] == 2
        assert edge["Win Rate (%)"] == 100.0

    def test_edge_validator_empty_perf(self, db, tmp_path):
        schema = "signals_t"
        _create_raw(db.conn, schema)
        res = sa.run_backtest(
            db.conn, schema, str(tmp_path), mc_iterations=1
        )
        assert res["edge_validation"] == {"status": "NO_DATA"}
        assert res["rows_processed"] == 0


# =====================================================================
# Phase 5 — matrix analytics
# =====================================================================

class TestPhase5:
    def test_report_files_written(self, db, tmp_path):
        schema = "signals_t"
        report_dir = str(tmp_path / "reports" / "signal_analysis" / "t")
        _insert_raw(db.conn, schema, Ticker="AAA")
        _seed_prices(db.conn, "AAA", RECENT, _flat_bars(12))
        _run_perf(db.conn, schema, report_dir)
        res = sa.run_phase5(db.conn, schema, report_dir)
        assert res["status"] == "SUCCESS"
        files = {os.path.basename(p) for p in res["files"]}
        assert files == {
            "heatmap_expectancy_target_atr_rr.png",
            "heatmap_expectancy_target_rr.png",
            "heatmap_winrate_model_cluster.png",
            "RPT_phase5_analytics_matrix.xlsx",
        }
        for f in files:
            assert os.path.exists(os.path.join(report_dir, f))
        wb = openpyxl.load_workbook(
            os.path.join(report_dir, "RPT_phase5_analytics_matrix.xlsx")
        )
        assert set(wb.sheetnames) == {
            "Model_vs_Cluster",
            "Target_vs_RR",
            "Target_ATR_vs_RR",
            "Raw_Evaluated_Data",
        }

    def test_empty_perf_no_data(self, db, tmp_path):
        schema = "signals_t"
        _create_raw(db.conn, schema)
        sa.calculate_and_save_performance(
            db.conn, schema, str(tmp_path)
        )
        res = sa.run_phase5(db.conn, schema, str(tmp_path / "out"))
        assert res["status"] == "NO_DATA"
        assert res["files"] == []


# =====================================================================
# Engine — run_signal_analysis (multi-set loop)
# =====================================================================

def _engine(tmp_path, sets):
    db = DatabaseManager(str(tmp_path / "eng.duckdb"))
    cfg = PipelineConfig(
        reports=ReportsConfig(report_dir=str(tmp_path / "reports")),
        signal_analysis=SignalAnalysisConfig(sets=sets),
    )
    return db, IngestionEngine(db, cfg)


class TestRunSignalAnalysis:
    def test_two_sets_isolated_schemas(self, tmp_path):
        folder_a = tmp_path / "fa"
        folder_b = tmp_path / "fb"
        _write_signal_file(
            folder_a, _fname(TODAY, "1500"),
            [_signal_row(Ticker="AAA"), _signal_row(Ticker="BBB")],
        )
        _write_signal_file(
            folder_b, _fname(TODAY, "1500"),
            [_signal_row(Ticker="ZZZ")],
        )
        db, eng = _engine(
            tmp_path,
            [
                SignalAnalysisSetConfig(name="set_a", folder=str(folder_a)),
                SignalAnalysisSetConfig(name="set_b", folder=str(folder_b)),
            ],
        )
        res = eng.run_signal_analysis()
        assert res["status"] == "SUCCESS"
        assert set(res["sets"]) == {"set_a", "set_b"}

        # Same object names under two different schemas — no leakage.
        objs_a = db.conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'signals_set_a' ORDER BY 1"
        ).fetchall()
        objs_b = db.conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'signals_set_b' ORDER BY 1"
        ).fetchall()
        assert objs_a == objs_b
        assert all(n[0].startswith("aly_") for n in objs_a)

        assert db.conn.execute(
            "SELECT COUNT(1) FROM signals_set_a.aly_signal_records_raw"
        ).fetchone()[0] == 2
        assert db.conn.execute(
            "SELECT COUNT(1) FROM signals_set_b.aly_signal_records_raw"
        ).fetchone()[0] == 1
        tickers_a = {
            r[0]
            for r in db.conn.execute(
                "SELECT Ticker FROM signals_set_a.aly_signal_records_raw"
            ).fetchall()
        }
        tickers_b = {
            r[0]
            for r in db.conn.execute(
                "SELECT Ticker FROM signals_set_b.aly_signal_records_raw"
            ).fetchall()
        }
        assert tickers_a == {"AAA", "BBB"}
        assert tickers_b == {"ZZZ"}
        db.close()

    def test_aly_prefix_on_every_created_object(self, tmp_path):
        folder_a = tmp_path / "fa"
        _write_signal_file(
            folder_a, _fname(TODAY, "1500"), [_signal_row()]
        )
        db, eng = _engine(
            tmp_path,
            [SignalAnalysisSetConfig(name="eng", folder=str(folder_a))],
        )
        eng.run_signal_analysis(set_name="eng")
        objs = db.conn.execute(
            "SELECT table_name, table_type FROM information_schema.tables "
            "WHERE table_schema = 'signals_eng'"
        ).fetchall()
        assert objs, "expected objects in signals_eng"
        assert all(n.startswith("aly_") for n, _ in objs)
        assert {t for _, t in objs} == {"BASE TABLE", "VIEW"}
        db.close()

    def test_set_name_selects_one_set(self, tmp_path):
        folder_a = tmp_path / "fa"
        folder_b = tmp_path / "fb"
        _write_signal_file(
            folder_a, _fname(TODAY, "1500"), [_signal_row()]
        )
        _write_signal_file(
            folder_b, _fname(TODAY, "1500"), [_signal_row()]
        )
        db, eng = _engine(
            tmp_path,
            [
                SignalAnalysisSetConfig(name="set_a", folder=str(folder_a)),
                SignalAnalysisSetConfig(name="set_b", folder=str(folder_b)),
            ],
        )
        res = eng.run_signal_analysis(set_name="set_b")
        assert set(res["sets"]) == {"set_b"}
        schemas = {
            r[0]
            for r in db.conn.execute(
                "SELECT schema_name FROM information_schema.schemata"
            ).fetchall()
        }
        assert "signals_set_b" in schemas
        assert "signals_set_a" not in schemas
        db.close()

    def test_unknown_set_raises(self, tmp_path):
        db, eng = _engine(
            tmp_path,
            [SignalAnalysisSetConfig(name="set_a", folder="x")],
        )
        with pytest.raises(ValueError, match="unknown signal_analysis"):
            eng.run_signal_analysis(set_name="nope")
        db.close()

    def test_empty_sets_config(self, tmp_path):
        db, eng = _engine(tmp_path, [])
        res = eng.run_signal_analysis()
        assert res["status"] == "SUCCESS"
        assert res["sets"] == {}
        db.close()

    def test_empty_folder_graceful(self, tmp_path):
        folder = tmp_path / "empty_folder"
        folder.mkdir()
        db, eng = _engine(
            tmp_path,
            [SignalAnalysisSetConfig(name="set_a", folder=str(folder))],
        )
        res = eng.run_signal_analysis()
        assert res["status"] == "SUCCESS"
        set_res = res["sets"]["set_a"]
        assert set_res["status"] == "SUCCESS"
        assert set_res["steps"]["ingest"]["status"] == "NO_FILES"
        assert set_res["steps"]["phase5"]["status"] == "SKIPPED"
        db.close()

    def test_reports_under_report_dir_signal_analysis(self, tmp_path):
        folder_a = tmp_path / "fa"
        _write_signal_file(
            folder_a, _fname(TODAY, "1500"), [_signal_row(Ticker="AAA")],
        )
        db, eng = _engine(
            tmp_path,
            [SignalAnalysisSetConfig(name="myset", folder=str(folder_a))],
        )
        _seed_prices(db.conn, "AAA", RECENT, _flat_bars(12))
        res = eng.run_signal_analysis()
        assert res["status"] == "SUCCESS"
        report_dir = tmp_path / "reports" / "signal_analysis" / "myset"
        assert res["sets"]["myset"]["report_dir"] == str(report_dir)
        expected = {
            "RPT_signal_performance.xlsx",
            "RPT_phase5_analytics_matrix.xlsx",
            "heatmap_expectancy_target_atr_rr.png",
            "heatmap_expectancy_target_rr.png",
            "heatmap_winrate_model_cluster.png",
        }
        assert expected <= set(os.listdir(report_dir))
        db.close()

    def test_config_override_params(self, tmp_path):
        """Explicit overrides beat the config defaults."""
        folder_a = tmp_path / "fa"
        _write_signal_file(
            folder_a, _fname(TODAY, "1500"), [_signal_row()]
        )
        db, eng = _engine(
            tmp_path,
            [SignalAnalysisSetConfig(name="set_a", folder=str(folder_a))],
        )
        res = eng.run_signal_analysis(
            set_name="set_a", max_holding_days=5, mc_iterations=2
        )
        assert res["status"] == "SUCCESS"
        assert (
            res["sets"]["set_a"]["steps"]["performance"][
                "max_holding_days"
            ]
            == 5
        )
        db.close()


# =====================================================================
# Config — signal_analysis block
# =====================================================================

class TestSignalAnalysisConfig:
    def test_defaults(self):
        cfg = SignalAnalysisConfig()
        assert cfg.sets == []
        assert cfg.backdate_days == 90
        assert cfg.max_holding_days == 8
        assert cfg.mc_iterations == 50
        assert cfg.benchmark == "SPY"

    @pytest.mark.parametrize(
        "bad", ["Version_P", "SET A", "", "x-y", 123, None]
    )
    def test_invalid_set_name_rejected(self, bad):
        with pytest.raises(Exception):
            SignalAnalysisSetConfig(name=bad, folder="f")

    def test_empty_folder_rejected(self):
        with pytest.raises(Exception):
            SignalAnalysisSetConfig(name="ok", folder="  ")

    def test_duplicate_names_rejected(self):
        with pytest.raises(Exception):
            SignalAnalysisConfig(
                sets=[
                    {"name": "a", "folder": "x"},
                    {"name": "a", "folder": "y"},
                ]
            )

    def test_real_config_yaml_loads(self):
        cfg = PipelineConfig.from_yaml("src/config.yaml")
        names = [s.name for s in cfg.signal_analysis.sets]
        assert names == ["version_p", "version_q"]
        assert cfg.signal_analysis.backdate_days == 90
        assert cfg.signal_analysis.max_holding_days == 8
        assert cfg.signal_analysis.mc_iterations == 50
        assert cfg.signal_analysis.benchmark == "SPY"
        folders = {s.name: s.folder for s in cfg.signal_analysis.sets}
        assert folders["version_p"].endswith(
            r"quant_strategy_building\version_p"
        )
        assert folders["version_q"].endswith(
            r"quant_strategy_building\version_q"
        )
