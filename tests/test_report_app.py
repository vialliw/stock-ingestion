"""Tests for ``scripts/report_app.py`` — the Streamlit report app.

Only the non-UI surface is exercised: the SQL builders, the
threshold-inversion helper, the Flight-first / DuckDB-fallback fetch
logic, the ``st.cache_data`` key derivation (via a fake ``st``), the
in-memory xlsx serializer, the verbatim interpretation-guide
constants, and the ordered bar-chart spec helpers.  No Streamlit
server is started;
``streamlit`` is never imported by these tests (report_app imports it
lazily inside the UI functions only).
"""

from __future__ import annotations

import io
import json
import sys
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import duckdb
import openpyxl
import pandas as pd
import pyarrow as pa
import pyarrow.flight as flight
import pytest

# Add scripts dir to path so report_app can be imported (same pattern
# as tests/test_scheduler.py).
SCRIPTS_DIR = Path(__file__).parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

import report_app

from src.config import PipelineConfig


def _config(tmp_path) -> PipelineConfig:
    """Config whose DB lives under ``tmp_path`` (nothing touches the
    real C:\\Intel\\parquet)."""
    return PipelineConfig(
        database={
            "duckdb_dir": str(tmp_path),
            "duckdb_filename": "test.duckdb",
        },
        logging={"log_file": str(tmp_path / "logs" / "run.log")},
    )


def _seed_daily_close(db_path: str, rows: list) -> None:
    """Create a fresh DuckDB file with a ``daily_close`` table holding
    ``rows`` of (date_str, ticker, open, high, low, close, volume)."""
    conn = duckdb.connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE daily_close (
                Date DATE NOT NULL,
                Ticker VARCHAR NOT NULL,
                Open DOUBLE,
                High DOUBLE,
                Low DOUBLE,
                "Close" DOUBLE,
                Volume DOUBLE,
                PRIMARY KEY (Date, Ticker)
            )
            """
        )
        conn.executemany(
            "INSERT INTO daily_close VALUES (?, ?, ?, ?, ?, ?, ?)", rows
        )
    finally:
        conn.close()


def _mock_flight(table=None):
    """Stand-in for the ``pyarrow.flight`` module inside ``report_app``
    (same pattern as ``_mock_flight_for_report`` in test_scheduler)."""
    mock_flight = MagicMock()
    mock_flight.Ticket = flight.Ticket
    mock_flight.FlightCallOptions = flight.FlightCallOptions
    mock_flight.FlightError = flight.FlightError
    mock_flight.FlightServerError = flight.FlightServerError
    mock_flight.FlightTimedOutError = flight.FlightTimedOutError
    mock_flight.FlightUnavailableError = flight.FlightUnavailableError
    if table is not None:
        reader = MagicMock()
        reader.read_all.return_value = table
        mock_flight.FlightClient.return_value.do_get.return_value = reader
    return mock_flight


class TestBuildTop25Sql:
    """Tab A builder — top-N by latest dollar volume + edge metrics."""

    def test_defaults_injected(self):
        sql = report_app.build_top25_sql()
        assert "FROM daily_close" in sql
        assert "period_rank <= 30" in sql
        assert "LIMIT 25" in sql
        assert ">= 2.5 THEN 'Strong Acceleration'" in sql
        assert ">= 1.8 THEN 'Acceleration'" in sql
        assert "<= 0.6 THEN 'Strong Deceleration'" in sql
        assert "<= 0.8 THEN 'Deceleration'" in sql
        assert "ELSE 'Normal'" in sql
        assert "ORDER BY c.latest_trade_money DESC" in sql
        assert "latest_dollar_volume" in sql
        assert "pct_of_total_avg_dollar" in sql
        # No regime filter by default.
        assert "volume_regime IN" not in sql

    def test_params_injected(self):
        sql = report_app.build_top25_sql(
            lookback=20,
            limit=10,
            strong_accel=3.0,
            accel=2.0,
            decel=0.7,
            strong_decel=0.5,
        )
        assert "period_rank <= 20" in sql
        assert "LIMIT 10" in sql
        assert ">= 3.0 THEN 'Strong Acceleration'" in sql
        assert ">= 2.0 THEN 'Acceleration'" in sql
        assert "<= 0.5 THEN 'Strong Deceleration'" in sql
        assert "<= 0.7 THEN 'Deceleration'" in sql

    def test_regime_filter_injected(self):
        sql = report_app.build_top25_sql(
            regimes=["Acceleration", "Strong Acceleration"]
        )
        assert (
            "WHERE c.volume_regime IN ('Acceleration', 'Strong Acceleration')"
            in sql
        )

    def test_empty_regime_selection_means_no_filter(self):
        sql = report_app.build_top25_sql(regimes=[])
        assert "volume_regime IN" not in sql

    def test_unknown_regime_rejected(self):
        # Values come from a fixed whitelist — anything else (e.g. a
        # hand-crafted string trying to break out of the literal) must
        # raise before reaching the SQL text.
        with pytest.raises(ValueError):
            report_app.build_top25_sql(
                regimes=["Acceleration'); DROP TABLE daily_close; --"]
            )

    def test_non_numeric_params_rejected(self):
        with pytest.raises(TypeError):
            report_app.build_top25_sql(lookback="30")
        with pytest.raises(TypeError):
            report_app.build_top25_sql(limit=True)
        with pytest.raises(TypeError):
            report_app.build_top25_sql(accel="1.8")

    def test_generated_sql_executes_on_duckdb(self, tmp_path):
        """The built query is valid SQL: runs against a seeded
        daily_close table and returns the expected shape/ordering."""
        db_path = str(tmp_path / "test.duckdb")
        rows = []
        # 40 distinct periods for two tickers; T2 has higher latest
        # volume.
        for i in range(40):
            day = (date(2026, 7, 1) + timedelta(days=i)).isoformat()
            # justified: seeding distinct synthetic rows per period —
            # deterministic test fixture, not data transformation.
            for ticker, vol in (("T1", 100.0), ("T2", 200.0)):
                rows.append(
                    (day, ticker, 1.0, 1.0, 1.0, 1.0, vol + i)
                )
        _seed_daily_close(db_path, rows)

        conn = duckdb.connect(db_path, read_only=True)
        try:
            df = conn.execute(
                report_app.build_top25_sql(limit=5)
            ).df()
        finally:
            conn.close()

        assert len(df) == 2
        assert "latest_dollar_volume" in df.columns
        assert "rvol" in df.columns
        assert "volume_regime" in df.columns
        # Ranked by latest dollar volume: T2's last row is larger.
        assert list(df["Ticker"]) == ["T2", "T1"]


class TestBuildAccelSql:
    """Tab B builder — acceleration candidates within top-K liquid."""

    def test_defaults_injected(self):
        sql = report_app.build_accel_sql()
        assert "FROM daily_close" in sql
        assert "period_rank <= 30" in sql
        assert "dollar_rank <= 50" in sql
        assert ">= 2.5 THEN 'Strong Acceleration'" in sql
        assert ">= 1.8 THEN 'Acceleration'" in sql
        assert "ELSE 'Other'" in sql
        assert (
            "volume_regime IN ('Acceleration', 'Strong Acceleration')"
            in sql
        )
        assert "ORDER BY rvol DESC, latest_trade_money DESC" in sql
        assert "ranked_by_dollar" in sql

    def test_params_injected(self):
        sql = report_app.build_accel_sql(
            lookback=15, accel=2.0, strong_accel=3.0, top_k=10
        )
        assert "period_rank <= 15" in sql
        assert ">= 3.0 THEN 'Strong Acceleration'" in sql
        assert ">= 2.0 THEN 'Acceleration'" in sql
        assert "dollar_rank <= 10" in sql

    def test_non_numeric_params_rejected(self):
        with pytest.raises(TypeError):
            report_app.build_accel_sql(lookback="30")
        with pytest.raises(TypeError):
            report_app.build_accel_sql(top_k="50")
        with pytest.raises(TypeError):
            report_app.build_accel_sql(accel=None)

    def test_generated_sql_executes_on_duckdb(self, tmp_path):
        """Tab B SQL filters to accelerating names among the most
        liquid and orders by rvol descending."""
        db_path = str(tmp_path / "test.duckdb")
        rows = []
        # 35 distinct periods: a huge latest-volume spike for SPIKE;
        # QUIET stays flat (rvol ~1 -> 'Other', filtered out).
        for i in range(35):
            day = (date(2026, 8, 1) + timedelta(days=i)).isoformat()
            spike_vol = 100.0 if i < 34 else 1000.0
            # justified: deterministic synthetic fixture rows.
            for ticker, vol in (("SPIKE", spike_vol), ("QUIET", 500.0)):
                rows.append((day, ticker, 1.0, 1.0, 1.0, 1.0, vol))
        _seed_daily_close(db_path, rows)

        conn = duckdb.connect(db_path, read_only=True)
        try:
            df = conn.execute(
                report_app.build_accel_sql(lookback=30, top_k=50)
            ).df()
        finally:
            conn.close()

        assert list(df["Ticker"]) == ["SPIKE"]
        assert df["volume_regime"].iloc[0] in (
            "Acceleration",
            "Strong Acceleration",
        )
        assert df["rvol"].iloc[0] >= 1.8


def _seed_weekly_db(db_path: str) -> None:
    """Seed a ``daily_close`` table spanning 8 weeks (Mon–Fri rows per
    week) with four tickers covering every weekly regime:

    * ``SPIKE`` — flat 100 then a 500 latest week -> rvol ~3.33
      ('Strong Acceleration')
    * ``BUMP``  — flat 100 then a 250 latest week -> rvol ~2.11
      ('Acceleration')
    * ``MID``   — flat 100 then a 60 latest week -> rvol ~0.63
      ('Deceleration')
    * ``DRY``   — flat 100 then a 20 latest week -> rvol ~0.22
      ('Strong Deceleration')
    * ``FLAT``  — always 100 -> rvol ~1.0 ('Normal' / 'Other')
    """
    rows = []
    last_week_volume = {
        "SPIKE": 500.0,
        "BUMP": 250.0,
        "MID": 60.0,
        "DRY": 20.0,
        "FLAT": 100.0,
    }
    # justified: deterministic synthetic fixture rows — 8 weeks x 5
    # trading days x 5 tickers, not data transformation.
    for w in range(8):
        for d in range(5):
            day = (date(2026, 1, 5) + timedelta(weeks=w, days=d)).isoformat()
            for ticker, last_vol in last_week_volume.items():
                vol = last_vol if w == 7 else 100.0
                rows.append((day, ticker, 1.0, 1.0, 1.0, 1.0, vol))
    _seed_daily_close(db_path, rows)


def _run_sql(db_path: str, sql: str) -> pd.DataFrame:
    conn = duckdb.connect(db_path, read_only=True)
    try:
        return conn.execute(sql).df()
    finally:
        conn.close()


class TestBuildWeeklyCombinedSql:
    """Weekly combined builder — all four regimes among top-K liquid,
    regime-priority ordering then rvol DESC."""

    def test_defaults_injected(self):
        sql = report_app.build_weekly_combined_sql()
        assert "FROM daily_close" in sql
        assert "DATE_TRUNC('week', Date)" in sql
        assert "ARRAY_AGG" in sql
        assert "week_rank <= 30" in sql
        assert "dollar_rank <= 50" in sql
        assert ">= 2.5 THEN 'Strong Acceleration'" in sql
        assert ">= 1.8 THEN 'Acceleration'" in sql
        # Weekly decel defaults differ from the daily 0.6 / 0.8.
        assert "<= 0.5 THEN 'Strong Deceleration'" in sql
        assert "<= 0.7 THEN 'Deceleration'" in sql
        assert "ELSE 'Normal'" in sql
        # All four regimes are kept.
        for regime in (
            "Strong Acceleration",
            "Acceleration",
            "Deceleration",
            "Strong Deceleration",
        ):
            assert f"'{regime}'" in sql
        # Regime-priority ordering, then rvol descending.  The
        # priority CASE follows the md's "Recommended reading order"
        # guide (Strong Deceleration third), deliberately deviating
        # from the md's own SQL which had Deceleration=3 /
        # Strong Deceleration=4 (PM-approved, F7-T25).
        assert "WHEN 'Strong Acceleration' THEN 1" in sql
        assert "WHEN 'Acceleration' THEN 2" in sql
        assert "WHEN 'Strong Deceleration' THEN 3" in sql
        assert "WHEN 'Deceleration' THEN 4" in sql
        assert "rvol DESC" in sql

    def test_params_injected(self):
        sql = report_app.build_weekly_combined_sql(
            lookback=12,
            top_k=20,
            strong_accel=3.0,
            accel=2.0,
            decel=0.6,
            strong_decel=0.4,
        )
        assert "week_rank <= 12" in sql
        assert "dollar_rank <= 20" in sql
        assert ">= 3.0 THEN 'Strong Acceleration'" in sql
        assert ">= 2.0 THEN 'Acceleration'" in sql
        assert "<= 0.4 THEN 'Strong Deceleration'" in sql
        assert "<= 0.6 THEN 'Deceleration'" in sql

    def test_non_numeric_params_rejected(self):
        with pytest.raises(TypeError):
            report_app.build_weekly_combined_sql(lookback="30")
        with pytest.raises(TypeError):
            report_app.build_weekly_combined_sql(top_k=True)
        with pytest.raises(TypeError):
            report_app.build_weekly_combined_sql(decel="0.7")

    def test_generated_sql_executes_on_duckdb(self, tmp_path):
        """Runs on a seeded weekly dataset; 'Normal' names are
        filtered out and rows come out in regime-priority order."""
        db_path = str(tmp_path / "test.duckdb")
        _seed_weekly_db(db_path)

        df = _run_sql(db_path, report_app.build_weekly_combined_sql())

        # Regime priority per the md's reading-order guide: Strong
        # Accel (1), Accel (2), Strong Decel (3), Decel (4) — DRY now
        # precedes MID; FLAT ('Normal') is filtered out.
        assert list(df["Ticker"]) == ["SPIKE", "BUMP", "DRY", "MID"]
        assert list(df["volume_regime"]) == [
            "Strong Acceleration",
            "Acceleration",
            "Strong Deceleration",
            "Deceleration",
        ]
        for col in (
            "week_start",
            "latest_week_end",
            "latest_close",
            "pct_change",
            "avg_week_volume",
            "rvol",
            "latest_dollar_volume",
            "avg_dollar_volume",
            "relative_dollar_vol",
        ):
            assert col in df.columns


class TestBuildWeeklyAccelSql:
    """Weekly accel builder — accel regimes only, rvol DESC."""

    def test_defaults_injected(self):
        sql = report_app.build_weekly_accel_sql()
        assert "FROM daily_close" in sql
        assert "DATE_TRUNC('week', Date)" in sql
        assert "week_rank <= 30" in sql
        assert "dollar_rank <= 50" in sql
        assert ">= 2.5 THEN 'Strong Acceleration'" in sql
        assert ">= 1.8 THEN 'Acceleration'" in sql
        assert "ELSE 'Other'" in sql
        assert (
            "volume_regime IN ('Acceleration', 'Strong Acceleration')"
            in sql
        )
        # Decel bounds must not appear in the accel-only CASE.
        assert "'Deceleration'" not in sql
        assert "'Strong Deceleration'" not in sql
        assert "ORDER BY rvol DESC, latest_dollar_volume DESC" in sql

    def test_params_injected(self):
        sql = report_app.build_weekly_accel_sql(
            lookback=15, accel=2.0, strong_accel=3.0, top_k=10
        )
        assert "week_rank <= 15" in sql
        assert "dollar_rank <= 10" in sql
        assert ">= 3.0 THEN 'Strong Acceleration'" in sql
        assert ">= 2.0 THEN 'Acceleration'" in sql

    def test_non_numeric_params_rejected(self):
        with pytest.raises(TypeError):
            report_app.build_weekly_accel_sql(lookback="30")
        with pytest.raises(TypeError):
            report_app.build_weekly_accel_sql(top_k="50")
        with pytest.raises(TypeError):
            report_app.build_weekly_accel_sql(accel=None)

    def test_generated_sql_executes_on_duckdb(self, tmp_path):
        """Only acceleration names survive, ordered rvol DESC."""
        db_path = str(tmp_path / "test.duckdb")
        _seed_weekly_db(db_path)

        df = _run_sql(db_path, report_app.build_weekly_accel_sql())

        assert list(df["Ticker"]) == ["SPIKE", "BUMP"]
        assert list(df["volume_regime"]) == [
            "Strong Acceleration",
            "Acceleration",
        ]
        assert list(df["rvol"]) == sorted(df["rvol"], reverse=True)


class TestBuildWeeklyDecelSql:
    """Weekly decel builder — decel regimes only, rvol ASC, weekly
    defaults 0.50 / 0.70 (not the daily 0.6 / 0.8)."""

    def test_defaults_injected(self):
        sql = report_app.build_weekly_decel_sql()
        assert "FROM daily_close" in sql
        assert "DATE_TRUNC('week', Date)" in sql
        assert "week_rank <= 30" in sql
        assert "dollar_rank <= 50" in sql
        assert "<= 0.5 THEN 'Strong Deceleration'" in sql
        assert "<= 0.7 THEN 'Deceleration'" in sql
        assert "ELSE 'Other'" in sql
        assert (
            "volume_regime IN ('Deceleration', 'Strong Deceleration')"
            in sql
        )
        # Quietest names first — the mirror of the accel tab.
        assert "ORDER BY rvol ASC, latest_dollar_volume DESC" in sql

    def test_params_injected(self):
        sql = report_app.build_weekly_decel_sql(
            lookback=15, decel=0.6, strong_decel=0.4, top_k=10
        )
        assert "week_rank <= 15" in sql
        assert "dollar_rank <= 10" in sql
        assert "<= 0.4 THEN 'Strong Deceleration'" in sql
        assert "<= 0.6 THEN 'Deceleration'" in sql

    def test_non_numeric_params_rejected(self):
        with pytest.raises(TypeError):
            report_app.build_weekly_decel_sql(lookback="30")
        with pytest.raises(TypeError):
            report_app.build_weekly_decel_sql(top_k="50")
        with pytest.raises(TypeError):
            report_app.build_weekly_decel_sql(strong_decel=None)

    def test_generated_sql_executes_on_duckdb(self, tmp_path):
        """Only deceleration names survive, ordered rvol ASC
        (quietest first)."""
        db_path = str(tmp_path / "test.duckdb")
        _seed_weekly_db(db_path)

        df = _run_sql(db_path, report_app.build_weekly_decel_sql())

        assert list(df["Ticker"]) == ["DRY", "MID"]
        assert list(df["volume_regime"]) == [
            "Strong Deceleration",
            "Deceleration",
        ]
        assert list(df["rvol"]) == sorted(df["rvol"])


class TestWeeklyInterpretationGuides:
    """The interpretation-guide module constants are non-empty and
    reproduce the guide sections of the source md files verbatim."""

    REPO_ROOT = Path(__file__).parent.parent

    @staticmethod
    def _guide_from_md(name: str) -> str:
        text = (
            TestWeeklyInterpretationGuides.REPO_ROOT
            / "docs"
            / "report_idea"
            / name
        ).read_text(encoding="utf-8")
        return text[text.index("### "):].strip()

    def test_constants_non_empty(self):
        for guide in (
            report_app.WEEKLY_COMBINED_GUIDE,
            report_app.WEEKLY_ACCEL_GUIDE,
            report_app.WEEKLY_DECEL_GUIDE,
        ):
            assert isinstance(guide, str)
            assert len(guide) > 100

    def test_guides_match_source_md_verbatim(self):
        assert report_app.WEEKLY_COMBINED_GUIDE.strip() == (
            self._guide_from_md("report_weekly_trends.md")
        )
        assert report_app.WEEKLY_ACCEL_GUIDE.strip() == (
            self._guide_from_md("report_weekly_trend_accelerating.md")
        )
        assert report_app.WEEKLY_DECEL_GUIDE.strip() == (
            self._guide_from_md("report_weekly_trend_decelerating.md")
        )


class TestBasicAndDailyGuides:
    """``BASIC_GUIDE`` / ``DAILY_*_GUIDE`` — non-empty module constants
    reproducing the generic / interpretation-guide sections of their
    source md files verbatim."""

    REPO_ROOT = Path(__file__).parent.parent

    # Lines deliberately dropped from the otherwise-verbatim
    # BASIC_GUIDE extract (F7-T27, PM-approved): they are permanently
    # stale on live data — a session-dated title banner, a "Data
    # sources" line naming feeds the app does not use, and a
    # market-closed methodology note.
    STALE_BASIC_GUIDE_LINES = (
        "**Session: September 11, 2026 (most recent full trading day as of Sep 13)**",
        "Data sources: MarketBeat, MarketGrep, Yahoo Finance, Seeking Alpha/AP most-actives, and related session reports (volumes and averages consistent across sources within normal reporting variance).",
        "- Markets closed Sep 13; this reflects the Sep 11 session. Refresh with live feeds for the next open.",
    )

    @classmethod
    def _read_md(cls, name: str) -> str:
        return (
            cls.REPO_ROOT / "docs" / "report_idea" / name
        ).read_text(encoding="utf-8")

    def test_constants_non_empty(self):
        for guide in (
            report_app.BASIC_GUIDE,
            report_app.DAILY_TOP25_GUIDE,
            report_app.DAILY_ACCEL_GUIDE,
        ):
            assert isinstance(guide, str)
            assert len(guide) > 100

    def test_daily_guides_match_source_md_verbatim(self):
        # Same extraction pattern as the weekly guide test: the guide
        # is everything from the first "### " heading to end of file.
        # The daily md filenames are swapped vs contents — tab A's
        # query lives in the "Acceleration Candidates" file.
        accel_md = self._read_md(
            "report_Acceleration Candidates "
            "(RVOL significantly above norm).md"
        )
        assert report_app.DAILY_TOP25_GUIDE.strip() == (
            accel_md[accel_md.index("### ") :].strip()
        )
        top25_md = self._read_md(
            "report_Top 25 by Approximate Dollar Volume.md"
        )
        assert report_app.DAILY_ACCEL_GUIDE.strip() == (
            top25_md[top25_md.index("### ") :].strip()
        )

    def test_basic_guide_matches_generic_sections_verbatim(self):
        """BASIC_GUIDE = title block + "Purpose & Trading Edge" +
        "Recommended Usage for Edge" + "Methodology Notes" (+ closing
        paragraph) — the two contiguous generic runs joined by one
        blank line — verbatim EXCEPT for the 3 stale session-dated
        lines dropped per F7-T27 (PM-approved exception)."""
        text = self._read_md("report_basic.md")
        # Head: title block + Purpose & Trading Edge, up to the
        # session-specific "### Top 25" data section.
        head = text[: text.index("### Top 25")].rstrip("\n")
        # Tail: Recommended Usage for Edge + Methodology Notes +
        # closing paragraph (to end of file).
        tail = text[text.index("### Recommended Usage") :].strip()
        expected = head + "\n\n" + tail
        # Sanity: all 3 stale lines are present in the extracted
        # source (guards against md drift turning the drop below into
        # a silent no-op).  rstrip() absorbs the markdown "  "
        # hard-break trailing whitespace on the Session title line.
        stale = set(self.STALE_BASIC_GUIDE_LINES)
        assert sum(1 for l in expected.split("\n") if l.rstrip() in stale) == 3
        # Drop the 3 stale whole lines, then collapse the double
        # blank line left by the removed standalone "Data sources"
        # paragraph back to a single blank line.
        expected = "\n".join(
            l for l in expected.split("\n") if l.rstrip() not in stale
        )
        while "\n\n\n" in expected:
            expected = expected.replace("\n\n\n", "\n\n")
        assert report_app.BASIC_GUIDE.strip() == expected.strip()

    def test_basic_guide_drops_stale_session_lines(self):
        """The 3 stale session-dated lines must not be rendered over
        live data (F7-T27 PM-approved verbatim exception)."""
        guide = report_app.BASIC_GUIDE
        for stale in self.STALE_BASIC_GUIDE_LINES:
            assert stale not in guide
        # Guard against re-added paraphrases of the same stale facts.
        assert "Session: September 11, 2026" not in guide
        assert "Data sources:" not in guide
        assert "Markets closed Sep 13" not in guide

    def test_basic_guide_excludes_session_specific_sections(self):
        """The Sep-11 data table, named candidates and "Key Insights"
        describe one specific session — all must be excluded."""
        guide = report_app.BASIC_GUIDE
        assert "### Top 25 by Approximate Dollar Volume" not in guide
        assert "### Acceleration Candidates" not in guide
        assert "### Deceleration Candidates" not in guide
        assert "Key Insights" not in guide
        assert "| Rank |" not in guide
        # Named candidates only appear in the excluded sections.
        assert "ORCL" not in guide
        # But the generic section headings are present.
        for heading in (
            "### Purpose & Trading Edge",
            "### Recommended Usage for Edge",
            "### Methodology Notes",
        ):
            assert heading in guide


class TestOrderedBarChart:
    """The bar-chart helpers keep the bars in the query's ``ORDER BY``
    (df row) order — the nominal Ticker axis must carry ``sort=None``
    so Vega-Lite does not re-sort categories alphabetically."""

    @staticmethod
    def _df() -> pd.DataFrame:
        # Deliberately non-alphabetical order: an alphabetical x-axis
        # sort would reorder these rows, so it is detectable.
        return pd.DataFrame(
            {
                "Ticker": ["ZZZ", "AAA", "MMM"],
                "rvol": [3.0, 1.0, 2.0],
                "latest_dollar_volume": [30.0, 10.0, 20.0],
            }
        )

    def test_ordered_bar_spec_sort_none_preserves_row_order(self):
        spec = report_app._ordered_bar_spec(self._df(), "rvol")
        assert spec["mark"] == "bar"
        assert spec["encoding"]["x"]["field"] == "Ticker"
        assert spec["encoding"]["x"]["type"] == "nominal"
        assert spec["encoding"]["x"]["sort"] is None
        assert spec["encoding"]["y"]["field"] == "rvol"
        assert spec["encoding"]["y"]["type"] == "quantitative"
        # Embedded data values stay in df row order.
        assert spec["data"]["values"] == [
            {"Ticker": "ZZZ", "rvol": 3.0},
            {"Ticker": "AAA", "rvol": 1.0},
            {"Ticker": "MMM", "rvol": 2.0},
        ]

    def test_altair_chart_serializes_sort_none_and_row_order(self):
        altair = pytest.importorskip("altair")
        df = self._df()
        chart = report_app._build_ordered_bar_chart(df, "rvol")
        assert isinstance(chart, altair.Chart)
        spec = chart.to_dict()
        assert spec["encoding"]["x"]["field"] == "Ticker"
        assert spec["encoding"]["x"]["type"] == "nominal"
        # sort=None must serialize to "sort": null — not be dropped.
        assert spec["encoding"]["x"]["sort"] is None
        assert spec["encoding"]["y"]["field"] == "rvol"
        # The embedded dataset keeps df row order.
        rows = spec["datasets"][spec["data"]["name"]]
        assert [row["Ticker"] for row in rows] == ["ZZZ", "AAA", "MMM"]

    def test_render_uses_altair_chart(self):
        pytest.importorskip("altair")
        fake_st = MagicMock()
        report_app._render_ordered_bar_chart(
            fake_st, self._df(), "rvol"
        )
        fake_st.altair_chart.assert_called_once()
        chart = fake_st.altair_chart.call_args.args[0]
        spec = chart.to_dict()
        assert spec["encoding"]["x"]["sort"] is None
        fake_st.vega_lite_chart.assert_not_called()

    def test_render_falls_back_to_vega_lite_when_altair_missing(self):
        """If ``altair`` cannot be imported, the helper renders the
        equivalent hand-built spec via ``st.vega_lite_chart``."""
        fake_st = MagicMock()
        with patch.object(
            report_app,
            "_build_ordered_bar_chart",
            side_effect=ImportError("No module named 'altair'"),
        ):
            report_app._render_ordered_bar_chart(
                fake_st, self._df(), "latest_dollar_volume"
            )
        fake_st.vega_lite_chart.assert_called_once()
        spec = fake_st.vega_lite_chart.call_args.args[0]
        assert spec["encoding"]["x"]["sort"] is None
        assert spec["encoding"]["y"]["field"] == "latest_dollar_volume"
        assert [row["Ticker"] for row in spec["data"]["values"]] == [
            "ZZZ",
            "AAA",
            "MMM",
        ]
        fake_st.altair_chart.assert_not_called()


class TestFetchDataframe:
    """Flight-first / direct-DuckDB fallback logic."""

    def test_flight_success_returns_dataframe(self, tmp_path):
        config = _config(tmp_path)
        table = pa.table(
            {"Ticker": ["MSFT"], "rvol": [2.1], "latest_dollar_volume": [1.0]}
        )
        mock_flight = _mock_flight(table)
        mock_client = mock_flight.FlightClient.return_value

        with patch.object(report_app, "flight", mock_flight):
            df = report_app.fetch_dataframe("SELECT 1", config)

        assert list(df["Ticker"]) == ["MSFT"]
        mock_client.wait_for_available.assert_called_once()
        mock_client.do_get.assert_called_once()
        ticket = mock_client.do_get.call_args.args[0]
        payload = json.loads(ticket.ticket.decode("utf-8"))
        assert payload["sql"] == "SELECT 1"
        # The call is bounded — FlightCallOptions(timeout=120).
        options = mock_client.do_get.call_args.kwargs["options"]
        assert isinstance(options, flight.FlightCallOptions)

    def test_do_get_carries_timeout_call_options(self, tmp_path):
        """A real FlightCallOptions built with ``timeout=120`` is
        passed so a wedged-but-connected server cannot hang the app."""
        config = _config(tmp_path)
        table = pa.table({"Ticker": ["MSFT"]})
        mock_flight = _mock_flight(table)
        # Wrap the real class so constructor kwargs are recorded while
        # a genuine FlightCallOptions is still produced (the pyarrow
        # object exposes no readable .timeout attribute).
        mock_flight.FlightCallOptions = MagicMock(
            wraps=flight.FlightCallOptions
        )
        mock_client = mock_flight.FlightClient.return_value

        with patch.object(report_app, "flight", mock_flight):
            df = report_app.fetch_dataframe("SELECT 1", config)

        assert df is not None
        mock_flight.FlightCallOptions.assert_called_once_with(timeout=120)
        options = mock_client.do_get.call_args.kwargs["options"]
        assert isinstance(options, flight.FlightCallOptions)

    def test_flight_down_falls_back_to_direct_readonly_db(self, tmp_path):
        config = _config(tmp_path)
        db_path = config.db_path
        _seed_daily_close(
            db_path, [("2026-09-01", "AAA", 1.0, 1.0, 1.0, 1.0, 10.0)]
        )

        mock_flight = _mock_flight()
        mock_flight.FlightClient.return_value.wait_for_available.side_effect = (
            flight.FlightUnavailableError("connection refused")
        )
        mock_duckdb = MagicMock()
        mock_duckdb.connect = MagicMock(wraps=duckdb.connect)

        with patch.object(
            report_app, "flight", mock_flight
        ), patch.object(report_app, "duckdb", mock_duckdb):
            df = report_app.fetch_dataframe(
                "SELECT Ticker, Volume FROM daily_close", config
            )

        # The fallback opened the DB read-only.
        mock_duckdb.connect.assert_called_once_with(
            db_path, read_only=True
        )
        assert list(df["Ticker"]) == ["AAA"]

    def test_both_paths_fail_returns_none(self, tmp_path):
        """Flight unreachable AND read-only connect fails (write lock
        held) -> None; the UI renders st.error."""
        config = _config(tmp_path)

        mock_flight = _mock_flight()
        mock_flight.FlightClient.return_value.wait_for_available.side_effect = (
            flight.FlightUnavailableError("connection refused")
        )
        mock_duckdb = MagicMock()
        mock_duckdb.connect.side_effect = duckdb.IOException(
            "Cannot open file: it is being used by another process"
        )

        with patch.object(
            report_app, "flight", mock_flight
        ), patch.object(report_app, "duckdb", mock_duckdb):
            df = report_app.fetch_dataframe("SELECT 1", config)

        assert df is None

    def test_flight_query_error_also_falls_back(self, tmp_path):
        """A mid-call Flight failure (not just connect) takes the
        fallback path too."""
        config = _config(tmp_path)
        db_path = config.db_path
        _seed_daily_close(
            db_path, [("2026-09-01", "AAA", 1.0, 1.0, 1.0, 1.0, 10.0)]
        )

        mock_flight = _mock_flight()
        mock_flight.FlightClient.return_value.do_get.side_effect = (
            flight.FlightServerError("query exploded")
        )

        with patch.object(report_app, "flight", mock_flight):
            df = report_app.fetch_dataframe(
                "SELECT Ticker FROM daily_close", config
            )

        assert list(df["Ticker"]) == ["AAA"]


class TestFindThresholdInversions:
    """Module-level helper behind the per-tab ``st.warning`` calls —
    pure function, exercised here without streamlit."""

    def test_accel_pair_inverted(self):
        """strong_accel < accel — the live-verified footgun: the CASE
        checks strong first so rvol ~1.0 names get 'Strong
        Acceleration' and tab B's filter silently becomes
        rvol >= strong_accel."""
        msgs = report_app.find_threshold_inversions(
            accel=2.0, strong_accel=1.0
        )
        assert len(msgs) == 1
        assert "Inverted" in msgs[0]
        assert "Strong Acceleration" in msgs[0]
        # Both bounds appear so the user sees the misordering.
        assert "rvol >= 1" in msgs[0]
        assert "rvol >= 2" in msgs[0]

    def test_decel_pair_inverted(self):
        """strong_decel > decel — the mirror-image misordering on the
        deceleration side (tab A)."""
        msgs = report_app.find_threshold_inversions(
            decel=0.5, strong_decel=0.9
        )
        assert len(msgs) == 1
        assert "Inverted" in msgs[0]
        assert "Strong Deceleration" in msgs[0]
        assert "rvol <= 0.9" in msgs[0]
        assert "rvol <= 0.5" in msgs[0]

    def test_both_pairs_inverted_reports_two(self):
        msgs = report_app.find_threshold_inversions(
            accel=2.0, strong_accel=1.0, decel=0.5, strong_decel=0.9
        )
        assert len(msgs) == 2
        joined = " ".join(msgs)
        assert "Strong Acceleration" in joined
        assert "Strong Deceleration" in joined

    def test_defaults_not_inverted(self):
        assert (
            report_app.find_threshold_inversions(
                accel=report_app.DEFAULT_ACCEL,
                strong_accel=report_app.DEFAULT_STRONG_ACCEL,
                decel=report_app.DEFAULT_DECEL,
                strong_decel=report_app.DEFAULT_STRONG_DECEL,
            )
            == []
        )

    def test_equal_bounds_not_inverted(self):
        """Equal thresholds are not *inverted* — the strong label
        legitimately absorbs the boundary."""
        assert (
            report_app.find_threshold_inversions(
                accel=1.8, strong_accel=1.8, decel=0.8, strong_decel=0.8
            )
            == []
        )

    def test_none_pairs_skipped(self):
        """Tab B only has the accel pair — a ``None`` decel pair must
        produce no warning."""
        assert (
            report_app.find_threshold_inversions(
                accel=1.8, strong_accel=2.5
            )
            == []
        )
        assert report_app.find_threshold_inversions() == []


class _FakeCacheData:
    """Minimal stand-in for the ``st`` object's ``cache_data``.

    Implements the kwargs-decorator form (``@st.cache_data(ttl=...)``)
    as an in-memory dict keyed on the wrapped function's positional
    args — enough to prove the cache key is SQL-dependent — and records
    the decorator kwargs so the TTL is assertable.  Exposes ``.clear()``
    on the wrapper like the real ``st.cache_data`` function.
    """

    def __init__(self):
        self.decorator_kwargs = []
        self.store = {}

    def cache_data(self, **kwargs):
        self.decorator_kwargs.append(kwargs)
        store = self.store

        def decorator(fn):
            def wrapper(*args):
                if args not in store:
                    store[args] = fn(*args)
                return store[args]

            def clear():
                store.clear()

            wrapper.clear = clear
            return wrapper

        return decorator


class TestCachedFetch:
    """The ``st.cache_data`` layer — built lazily via
    ``_build_cached_fetch(st)`` / ``_get_cached_fetch(st)`` so it is
    exercisable here with a fake ``st`` (no streamlit import)."""

    @pytest.fixture(autouse=True)
    def _reset_cached_fetch_singleton(self):
        saved = report_app._CACHED_FETCH
        report_app._CACHED_FETCH = None
        yield
        report_app._CACHED_FETCH = saved

    def test_cache_key_is_sql_dependent(self, tmp_path):
        """Same SQL -> one underlying fetch; changed SQL (a widget
        rerun that altered the query) -> re-fetch."""
        fake_st = _FakeCacheData()
        cached = report_app._build_cached_fetch(fake_st)
        df = pd.DataFrame({"x": [1]})
        with patch.object(
            report_app, "_fetch_dataframe_uncached", return_value=df
        ) as mock_fetch:
            first = cached("SELECT 1", "grpc://h:1", "db1")
            second = cached("SELECT 1", "grpc://h:1", "db1")
            third = cached("SELECT 2", "grpc://h:1", "db1")

        assert mock_fetch.call_count == 2
        # Cache hit returns the stored object for the repeated key.
        assert first is second
        assert third is df
        # The two underlying calls differ only in the SQL text.
        sqls = [call.args[0] for call in mock_fetch.call_args_list]
        assert sqls == ["SELECT 1", "SELECT 2"]

    def test_cache_key_includes_connection_targets(self):
        """Same SQL against a different Flight location or DB path is
        a different cache entry."""
        fake_st = _FakeCacheData()
        cached = report_app._build_cached_fetch(fake_st)
        with patch.object(
            report_app,
            "_fetch_dataframe_uncached",
            return_value=pd.DataFrame({"x": [1]}),
        ) as mock_fetch:
            cached("SELECT 1", "grpc://a:1", "db1")
            cached("SELECT 1", "grpc://b:1", "db1")
            cached("SELECT 1", "grpc://a:1", "db2")
            cached("SELECT 1", "grpc://a:1", "db1")

        assert mock_fetch.call_count == 3

    def test_ttl_is_configured(self):
        fake_st = _FakeCacheData()
        report_app._build_cached_fetch(fake_st)
        assert len(fake_st.decorator_kwargs) == 1
        assert (
            fake_st.decorator_kwargs[0]["ttl"]
            == report_app._CACHE_TTL_S
        )
        assert report_app._CACHE_TTL_S > 0

    def test_clear_forces_refetch(self):
        """The "Refresh data" buttons call ``.clear()`` — after it,
        the same SQL is fetched again."""
        fake_st = _FakeCacheData()
        cached = report_app._build_cached_fetch(fake_st)
        with patch.object(
            report_app,
            "_fetch_dataframe_uncached",
            return_value=pd.DataFrame({"x": [1]}),
        ) as mock_fetch:
            cached("SELECT 1", "grpc://h:1", "db1")
            cached.clear()
            cached("SELECT 1", "grpc://h:1", "db1")

        assert mock_fetch.call_count == 2

    def test_cached_fetch_dataframe_derives_key_from_config(
        self, tmp_path
    ):
        """``cached_fetch_dataframe`` maps config -> (location,
        db_path) so the cache key is (sql, grpc target, db file)."""
        config = _config(tmp_path)
        fake_st = _FakeCacheData()
        df = pd.DataFrame({"x": [1]})
        with patch.object(
            report_app, "_fetch_dataframe_uncached", return_value=df
        ) as mock_fetch:
            report_app.cached_fetch_dataframe(fake_st, "SELECT 1", config)
            report_app.cached_fetch_dataframe(fake_st, "SELECT 1", config)

        assert mock_fetch.call_count == 1
        args = mock_fetch.call_args.args
        assert args[0] == "SELECT 1"
        assert args[1] == (
            f"grpc://{config.server.flight_host}:"
            f"{config.server.flight_port}"
        )
        assert args[2] == config.db_path

    def test_get_cached_fetch_builds_singleton_once(self):
        """The st.cache_data wrapper is built once and reused — a
        fresh wrapper per rerun would never hit the cache."""
        fake_st = _FakeCacheData()
        first = report_app._get_cached_fetch(fake_st)
        second = report_app._get_cached_fetch(fake_st)
        assert first is second
        assert len(fake_st.decorator_kwargs) == 1


class TestDataframeToXlsxBytes:
    """In-memory xlsx serializer used by st.download_button."""

    def test_produces_readable_workbook_with_widths(self):
        df = pd.DataFrame(
            {
                "Ticker": ["MSFT", "AAPL"],
                "rvol": [2.1, 1.9],
                "latest_dollar_volume": [1234.5, 678.9],
            }
        )
        blob = report_app.dataframe_to_xlsx_bytes(df, sheet_name="Report")
        assert isinstance(blob, bytes) and len(blob) > 0

        wb = openpyxl.load_workbook(io.BytesIO(blob))
        ws = wb["Report"]
        headers = [cell.value for cell in ws[1]]
        assert headers == ["Ticker", "rvol", "latest_dollar_volume"]
        data_rows = list(ws.iter_rows(min_row=2, values_only=True))
        assert data_rows[0][0] == "MSFT"
        assert data_rows[1][0] == "AAPL"
        # Auto-adjusted widths: each column dimension is set.
        for col_letter, header in zip(("A", "B", "C"), headers):
            width = ws.column_dimensions[col_letter].width
            assert width is not None
            assert width >= len(header)
        wb.close()

    def test_empty_dataframe_still_produces_workbook(self):
        df = pd.DataFrame({"Ticker": [], "rvol": []})
        blob = report_app.dataframe_to_xlsx_bytes(df, sheet_name="Empty")
        wb = openpyxl.load_workbook(io.BytesIO(blob))
        ws = wb["Empty"]
        assert [cell.value for cell in ws[1]] == ["Ticker", "rvol"]
        wb.close()


class TestMarketBreadthTab:
    """The "Market Breadth" tab — 6th tab, added after the two daily
    tabs (F11-T1).  The heavy lifting lives in
    ``scripts/breadth_dashboard.py``; here we only check the tab is
    wired into ``main()`` and that the render path drives the shared
    cached fetch + ``st.plotly_chart``."""

    def test_tab_label_in_main_tab_list(self):
        import inspect

        src = inspect.getsource(report_app.main)
        assert '"Market Breadth"' in src
        # Sixth and last tab — after the two daily tabs.
        assert src.index('"Daily — Acceleration Candidates"') < src.index(
            '"Market Breadth"'
        )

    @staticmethod
    def _synthetic_frames():
        import breadth_dashboard

        dates = pd.date_range("2025-01-01", periods=80, freq="B")
        n = len(dates)
        breadth = pd.DataFrame(
            {
                "Date": dates,
                "Total_Stocks": [100] * n,
                "Advances": [60] * n,
                "Declines": [30] * n,
                "Above_SMA_50": [55] * n,
                "Above_SMA_200": [45] * n,
                "Adv_Volume": [6000.0] * n,
                "Dec_Volume": [3000.0] * n,
                "New_Highs": [12] * n,
                "New_Lows": [4] * n,
            }
        )
        spy = pd.DataFrame(
            {
                "Date": dates,
                "Open": [100.0] * n,
                "High": [101.0] * n,
                "Low": [99.0] * n,
                "Close": [100.5 + i for i in range(n)],
                "Volume": [1_000_000.0] * n,
            }
        )
        return breadth, spy

    def test_render_drives_fetch_and_plotly_chart(
        self, tmp_path, monkeypatch
    ):
        breadth, spy = self._synthetic_frames()

        def fake_cached_fetch(st, sql, config):
            if "Ticker = 'SPY'" in sql:
                return spy
            return breadth

        monkeypatch.setattr(
            report_app, "cached_fetch_dataframe", fake_cached_fetch
        )
        st = MagicMock()
        st.number_input.return_value = 0
        st.button.return_value = False

        report_app._render_market_breadth_tab(st, _config(tmp_path))

        st.plotly_chart.assert_called_once()
        # Params expander + interpretation-guide expander used.
        assert st.expander.call_count >= 2
        # The verbatim HTML guide renders via st.html (streamlit 1.58)
        # or unsafe markdown — one of them must have been called with it.
        guide_calls = (
            getattr(st.html, "call_args_list", [])
            + getattr(st.markdown, "call_args_list", [])
        )
        assert any(
            "Complete Interpretation Guide" in str(c)
            for c in guide_calls
        )

    def test_render_errors_when_fetch_returns_none(
        self, tmp_path, monkeypatch
    ):
        monkeypatch.setattr(
            report_app,
            "cached_fetch_dataframe",
            lambda st, sql, config: None,
        )
        st = MagicMock()
        st.number_input.return_value = 0
        st.button.return_value = False

        report_app._render_market_breadth_tab(st, _config(tmp_path))

        st.error.assert_called_once()
        st.plotly_chart.assert_not_called()


class TestRrgTab:
    """The "Relative Rotation (RRG)" tab — 7th tab, after Market
    Breadth (F13-T1).  The heavy lifting lives in
    ``scripts/rrg_dashboard.py``; here we only check the tab is wired
    into ``main()`` and that the render path drives the shared cached
    fetch + ``st.plotly_chart``."""

    def test_tab_label_in_main_tab_list(self):
        import inspect

        src = inspect.getsource(report_app.main)
        assert '"Relative Rotation (RRG)"' in src
        # Seventh and last tab — after Market Breadth.
        assert src.index('"Market Breadth"') < src.index(
            '"Relative Rotation (RRG)"'
        )

    @staticmethod
    def _weekly_long_df(n: int = 60, tickers=("XLK", "DIA")):
        """Long-format weekly_close rows covering ``tickers`` + the
        SPY benchmark."""
        import numpy as np

        dates = pd.date_range(
            end=pd.Timestamp.today().normalize(), periods=n, freq="W-FRI"
        )
        i = np.arange(n)
        closes = {
            t: 100.0 * (1.03 + 0.02 * k) ** i
            for k, t in enumerate(tickers)
        }
        closes["SPY"] = np.full(n, 100.0)
        recs = [
            {"Date": d, "Ticker": t, "Close": c}
            for t, series in closes.items()
            for d, c in zip(dates, series)
        ]
        return pd.DataFrame.from_records(recs)

    @staticmethod
    def _keyed_text_input(custom_value):
        """``st.text_input`` side_effect — the benchmark widget and the
        Custom tickers widget share one MagicMock; dispatch on the
        widget ``key``."""

        def fake_text_input(label, *args, **kwargs):
            if kwargs.get("key") == "rrg_custom_tickers":
                return custom_value
            return "SPY"

        return fake_text_input

    def test_render_drives_fetch_and_plotly_chart(
        self, tmp_path, monkeypatch
    ):
        pytest.importorskip("scipy")
        pytest.importorskip("plotly")
        df = self._weekly_long_df()
        monkeypatch.setattr(
            report_app,
            "cached_fetch_dataframe",
            lambda st, sql, config: df,
        )
        st = MagicMock()
        st.selectbox.return_value = "sector3"
        # One MagicMock return feeds the period/trail/lookback inputs —
        # 14 each leaves 60 - (2*14+4) = 28 clean rows > trail 14.
        st.number_input.return_value = 14
        st.text_input.return_value = "SPY"
        st.button.return_value = False
        st.columns.return_value = (MagicMock(), MagicMock())

        report_app._render_rrg_tab(st, _config(tmp_path))

        st.plotly_chart.assert_called_once()
        st.error.assert_not_called()
        # Latest-position stats table + JSON download button rendered.
        st.dataframe.assert_called_once()
        st.download_button.assert_called_once()
        assert (
            st.download_button.call_args.kwargs["file_name"]
            == "Relative_Rotation_Dashboard_sector3_analysis_prompt.json"
        )

    def test_render_errors_when_fetch_returns_none(
        self, tmp_path, monkeypatch
    ):
        monkeypatch.setattr(
            report_app,
            "cached_fetch_dataframe",
            lambda st, sql, config: None,
        )
        st = MagicMock()
        st.selectbox.return_value = "sector3"
        st.number_input.return_value = 14
        st.text_input.return_value = "SPY"
        st.button.return_value = False
        st.columns.return_value = (MagicMock(), MagicMock())

        report_app._render_rrg_tab(st, _config(tmp_path))

        st.error.assert_called_once()
        st.plotly_chart.assert_not_called()

    def test_render_custom_normalizes_tickers_into_sql(
        self, tmp_path, monkeypatch
    ):
        """Custom mode: "XLK, dia ,xlf" is stripped / uppercased /
        deduped into the SQL IN-list and the render completes."""
        pytest.importorskip("scipy")
        pytest.importorskip("plotly")
        df = self._weekly_long_df(tickers=("XLK", "DIA", "XLF"))
        seen_sql = []

        def fake_fetch(st, sql, config):
            seen_sql.append(sql)
            return df

        monkeypatch.setattr(
            report_app, "cached_fetch_dataframe", fake_fetch
        )
        st = MagicMock()
        st.selectbox.return_value = "Custom"
        st.number_input.return_value = 14
        st.text_input.side_effect = self._keyed_text_input(
            "XLK, dia ,xlf"
        )
        st.button.return_value = False
        st.columns.return_value = (MagicMock(), MagicMock())

        report_app._render_rrg_tab(st, _config(tmp_path))

        # Normalized + benchmark appended: IN ('XLK','DIA','XLF','SPY').
        assert len(seen_sql) == 1
        assert "'XLK','DIA','XLF'" in seen_sql[0]
        st.plotly_chart.assert_called_once()
        st.error.assert_not_called()
        # All requested tickers present in the data -> no drop warning.
        st.warning.assert_not_called()
        # Custom has no preset prefix — fixed ad-hoc filename.
        assert (
            st.download_button.call_args.kwargs["file_name"]
            == "RRG_Custom_analysis_prompt.json"
        )

    def test_render_custom_empty_input_errors(
        self, tmp_path, monkeypatch
    ):
        """Whitespace-only Custom input errors out before any fetch."""
        fetch = MagicMock()
        monkeypatch.setattr(
            report_app, "cached_fetch_dataframe", fetch
        )
        st = MagicMock()
        st.selectbox.return_value = "Custom"
        st.number_input.return_value = 14
        st.text_input.side_effect = self._keyed_text_input("   , ,")
        st.button.return_value = False
        st.columns.return_value = (MagicMock(), MagicMock())

        report_app._render_rrg_tab(st, _config(tmp_path))

        st.error.assert_called_once()
        fetch.assert_not_called()
        st.plotly_chart.assert_not_called()

    def test_render_custom_invalid_symbol_errors(
        self, tmp_path, monkeypatch
    ):
        """A symbol that fails build_rrg_sql's regex surfaces as
        st.error and never reaches the fetch."""
        fetch = MagicMock()
        monkeypatch.setattr(
            report_app, "cached_fetch_dataframe", fetch
        )
        st = MagicMock()
        st.selectbox.return_value = "Custom"
        st.number_input.return_value = 14
        st.text_input.side_effect = self._keyed_text_input(
            "X;DROP TABLE"
        )
        st.button.return_value = False
        st.columns.return_value = (MagicMock(), MagicMock())

        report_app._render_rrg_tab(st, _config(tmp_path))

        st.error.assert_called_once()
        fetch.assert_not_called()
        st.plotly_chart.assert_not_called()

    def test_render_custom_dropped_ticker_warns(
        self, tmp_path, monkeypatch
    ):
        """Fetch returning a subset (XLF absent) -> st.warning names
        the dropped symbol while the survivors still render."""
        pytest.importorskip("scipy")
        pytest.importorskip("plotly")
        df = self._weekly_long_df(tickers=("XLK", "DIA"))  # no XLF
        monkeypatch.setattr(
            report_app,
            "cached_fetch_dataframe",
            lambda st, sql, config: df,
        )
        st = MagicMock()
        st.selectbox.return_value = "Custom"
        st.number_input.return_value = 14
        st.text_input.side_effect = self._keyed_text_input(
            "XLK,DIA,XLF"
        )
        st.button.return_value = False
        st.columns.return_value = (MagicMock(), MagicMock())

        report_app._render_rrg_tab(st, _config(tmp_path))

        st.warning.assert_called_once()
        assert "XLF" in str(st.warning.call_args)
        st.plotly_chart.assert_called_once()


class TestFearGreedTab:
    """The "Fear & Greed" tab — 8th tab, after RRG (F18-T1).  The
    heavy lifting lives in ``scripts/fear_greed_dashboard.py``; here
    we only check the tab is wired into ``main()`` and that the
    render path drives the shared cached fetch + three
    ``st.plotly_chart`` calls (gauge + history + heatmap)."""

    def test_tab_label_in_main_tab_list(self):
        import inspect

        src = inspect.getsource(report_app.main)
        assert '"Fear & Greed"' in src
        # Eighth and last tab — after Relative Rotation.
        assert src.index('"Relative Rotation (RRG)"') < src.index(
            '"Fear & Greed"'
        )
        assert src.index("_render_fear_greed_tab") > src.index(
            '"Fear & Greed"'
        )

    @staticmethod
    def _fg_df(n: int = 60) -> pd.DataFrame:
        """Synthetic ``fear_greed_history`` frame — all 21 display
        columns, business-day dates."""
        import fear_greed_dashboard as fgd

        dates = pd.date_range(end="2026-09-18", periods=n, freq="B")
        data = {
            "date": dates,
            "fg_score": [30.0 + i * 0.25 for i in range(n)],
            "fg_rating": ["fear"] * n,
        }
        for key, _label in fgd.INDICATORS:
            data[key] = [float(i % 10) for i in range(n)]
            data[f"{key}_rating"] = ["neutral"] * n
        return pd.DataFrame(data)

    def test_render_drives_fetch_and_three_charts(
        self, tmp_path, monkeypatch
    ):
        pytest.importorskip("plotly")
        df = self._fg_df()
        seen_sql = []

        def fake_fetch(st, sql, config):
            seen_sql.append(sql)
            return df

        monkeypatch.setattr(
            report_app, "cached_fetch_dataframe", fake_fetch
        )
        st = MagicMock()
        st.radio.return_value = "1Y"
        st.button.return_value = False
        st.checkbox.return_value = True

        report_app._render_fear_greed_tab(st, _config(tmp_path))

        # Exactly one fetch through the shared cached path.
        assert len(seen_sql) == 1
        assert "FROM fear_greed_history" in seen_sql[0]
        # Gauge + history + heatmap.
        assert st.plotly_chart.call_count == 3
        st.error.assert_not_called()
        # Window radio defaults to 1Y among the seven presets.
        radio_kwargs = st.radio.call_args.kwargs
        assert (
            radio_kwargs["options"][radio_kwargs["index"]] == "1Y"
        )
        # MA checkbox on -> the history figure carries 3 traces
        # (score + MA20 + MA50).
        history_fig = st.plotly_chart.call_args_list[1].args[0]
        assert len(history_fig.data) == 3
        # The heatmap figure is the third chart, 9 rows.
        heatmap_fig = st.plotly_chart.call_args_list[2].args[0]
        assert len(heatmap_fig.data[0].z) == 9

    def test_render_errors_when_fetch_returns_none(
        self, tmp_path, monkeypatch
    ):
        monkeypatch.setattr(
            report_app,
            "cached_fetch_dataframe",
            lambda st, sql, config: None,
        )
        st = MagicMock()
        st.radio.return_value = "1Y"
        st.button.return_value = False

        report_app._render_fear_greed_tab(st, _config(tmp_path))

        st.error.assert_called_once()
        assert "Flight server" in str(st.error.call_args)
        st.plotly_chart.assert_not_called()

    def test_render_errors_when_frame_empty(
        self, tmp_path, monkeypatch
    ):
        monkeypatch.setattr(
            report_app,
            "cached_fetch_dataframe",
            lambda st, sql, config: pd.DataFrame(),
        )
        st = MagicMock()
        st.radio.return_value = "1Y"
        st.button.return_value = False

        report_app._render_fear_greed_tab(st, _config(tmp_path))

        st.error.assert_called_once()
        assert "fear_greed_history" in str(st.error.call_args)
        st.plotly_chart.assert_not_called()

    def test_refresh_button_clears_cached_fetch(
        self, tmp_path, monkeypatch
    ):
        """The "Refresh data" button calls
        ``_get_cached_fetch(st).clear()``."""
        pytest.importorskip("plotly")
        df = self._fg_df(5)
        monkeypatch.setattr(
            report_app,
            "cached_fetch_dataframe",
            lambda st, sql, config: df,
        )
        sentinel = MagicMock()
        monkeypatch.setattr(
            report_app, "_get_cached_fetch", lambda st: sentinel
        )
        st = MagicMock()
        st.radio.return_value = "Max"
        st.button.return_value = True
        st.checkbox.return_value = False

        report_app._render_fear_greed_tab(st, _config(tmp_path))

        sentinel.clear.assert_called_once()
        assert st.plotly_chart.call_count == 3
