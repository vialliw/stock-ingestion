"""Interactive Streamlit report app with two tabs:

* **Tab A — "Top 25 by Dollar Volume"**: the top-N tickers ranked by
  latest (most recent trading day) dollar volume, enriched with the
  RVOL / regime edge metrics (latest dollar volume, relative volume,
  pct change, volume regime classification, share of total average
  dollar volume).  The query is the one inside
  ``docs/report_idea/report_Acceleration Candidates (RVOL significantly
  above norm).md`` — note the two md filenames are *swapped* vs their
  contents; tabs are labelled by intent, not filename.
* **Tab B — "Acceleration Candidates"**: filters ``rvol >= threshold``
  among the top-K most-liquid names by latest dollar volume, ordered
  by ``rvol`` descending.  The query is the one inside
  ``docs/report_idea/report_Top 25 by Approximate Dollar Volume.md``.

All interactive widget values are numeric (or drawn from the fixed
``VOLUME_REGIMES`` whitelist), so they are safely formatted into the
SQL text — no quoting/escaping concerns.

Data access mirrors ``scripts/report_top25_active.py``: the running
Arrow Flight service is tried first (``do_get`` with a JSON
``{"sql": ...}`` ticket on
``grpc://{server.flight_host}:{server.flight_port}``, a 5-second
``wait_for_available`` probe, and a 120-second ``FlightCallOptions``
call bound), and any Flight-path failure falls back to a direct
*read-only* DuckDB connection.  When both fail the tab shows
``st.error``.

The module is importable and unit-testable *without* the Streamlit
runtime: the SQL builder functions (:func:`build_top25_sql`,
:func:`build_accel_sql`), the data fetch (:func:`fetch_dataframe`) and
the xlsx serializer (:func:`dataframe_to_xlsx_bytes`) are plain
functions; ``streamlit`` is only imported inside the UI code paths.

Launch with:

    streamlit run scripts/report_app.py
"""

from __future__ import annotations

import io
import json
import logging
import os
import sys
from typing import Optional, Sequence

# Ensure src/ is importable regardless of the caller's working
# directory (mirrors scripts/daily_load_once.py /
# scripts/report_top25_active.py).
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _REPO_ROOT)

_CONFIG_PATH = os.path.join(_REPO_ROOT, "src", "config.yaml")

import duckdb
import pandas as pd
import pyarrow.flight as flight

from src.config import PipelineConfig

logger = logging.getLogger(__name__)

# Short probe answering "is the Flight service up?" — kept small so a
# stopped service does not stall the app before the direct read-only
# fallback takes over (mirrors report_top25_active).
_FLIGHT_CONNECT_TIMEOUT_S = 5

# Bound on the do_get call itself so a wedged-but-connected server
# cannot hang the app forever.  120 s mirrors the server's own
# query_timeout_seconds default (src/config.py ServerConfig).
_FLIGHT_GET_TIMEOUT_S = 120

# Padding added to each auto-sized xlsx column so content is not flush
# against the cell edge in Excel.
_WIDTH_PADDING = 2

# ------------------------------------------------------------------
# Report parameter defaults (tunable via the Streamlit widgets).
# ------------------------------------------------------------------
DEFAULT_LOOKBACK = 30          # trailing trading periods per ticker
DEFAULT_LIMIT = 25             # top-N rows for tab A
DEFAULT_STRONG_ACCEL = 2.5     # rvol >= -> 'Strong Acceleration'
DEFAULT_ACCEL = 1.8            # rvol >= -> 'Acceleration'
DEFAULT_DECEL = 0.8            # rvol <= -> 'Deceleration'
DEFAULT_STRONG_DECEL = 0.6     # rvol <= -> 'Strong Deceleration'
DEFAULT_TOP_K = 50             # liquidity filter for tab B

# Fixed whitelist of volume_regime labels the SQL CASE can emit.  The
# tab-A multiselect is restricted to these values and the builder
# validates against them before formatting into SQL text.
VOLUME_REGIMES = (
    "Strong Acceleration",
    "Acceleration",
    "Normal",
    "Deceleration",
    "Strong Deceleration",
)

_TAB_A_SHEET = "Top 25 by Dollar Volume"
_TAB_A_FILENAME = "top25_by_dollar_volume.xlsx"
_TAB_B_SHEET = "Acceleration Candidates"
_TAB_B_FILENAME = "acceleration_candidates.xlsx"


# ------------------------------------------------------------------
# SQL builders (pure functions — unit-testable without Streamlit).
# ------------------------------------------------------------------


def _require_number(name: str, value) -> float:
    """Return ``value`` as a float after rejecting non-numerics.

    Widget values are formatted directly into the SQL text, so only
    real numbers may pass — ``bool`` and non-numeric types raise
    ``ValueError``/``TypeError`` here instead of reaching the query.
    """
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be a number, got {type(value).__name__}")
    return value


def _regime_in_clause(regimes: Optional[Sequence[str]]) -> str:
    """Build an optional ``c.volume_regime IN (...)`` WHERE clause.

    Every label is validated against the fixed ``VOLUME_REGIMES``
    whitelist so only known-safe literals reach the SQL text.  ``None``
    or an empty selection means "no filter" (all regimes shown).
    """
    if not regimes:
        return ""
    unknown = [r for r in regimes if r not in VOLUME_REGIMES]
    if unknown:
        raise ValueError(
            f"Unknown volume_regime value(s): {unknown!r}; "
            f"must be drawn from {list(VOLUME_REGIMES)!r}"
        )
    labels = ", ".join(f"'{r}'" for r in regimes)
    return f"WHERE c.volume_regime IN ({labels})\n"


def build_top25_sql(
    lookback: int = DEFAULT_LOOKBACK,
    limit: int = DEFAULT_LIMIT,
    strong_accel: float = DEFAULT_STRONG_ACCEL,
    accel: float = DEFAULT_ACCEL,
    decel: float = DEFAULT_DECEL,
    strong_decel: float = DEFAULT_STRONG_DECEL,
    regimes: Optional[Sequence[str]] = None,
) -> str:
    """Tab A query — top ``limit`` tickers by latest dollar volume plus
    the RVOL / regime edge metrics.

    Adapted from ``docs/report_idea/report_Acceleration Candidates
    (RVOL significantly above norm).md`` (the file whose *name* says
    "Acceleration Candidates" but whose content is the top-25 report).

    ``lookback`` sets the trailing window used for the per-ticker
    averages; ``strong_accel``/``accel``/``decel``/``strong_decel`` are
    the RVOL regime thresholds; ``regimes`` optionally restricts the
    output to selected ``volume_regime`` labels.
    """
    lookback = int(_require_number("lookback", lookback))
    limit = int(_require_number("limit", limit))
    strong_accel = float(_require_number("strong_accel", strong_accel))
    accel = float(_require_number("accel", accel))
    decel = float(_require_number("decel", decel))
    strong_decel = float(_require_number("strong_decel", strong_decel))
    regime_where = _regime_in_clause(regimes)

    return f"""
WITH trade_money AS (
  SELECT
    Ticker,
    Date,
    Open,
    High,
    Low,
    "Close",
    Volume,
    ((Open + High + Low + "Close") / 4.0) * Volume AS trade_money,
    (("Close" - LAG("Close") OVER (PARTITION BY Ticker ORDER BY Date))
      / NULLIF(LAG("Close") OVER (PARTITION BY Ticker ORDER BY Date), 0)) * 100 AS pct_change
  FROM daily_close
),

-- Last N trading periods per ticker (for averages)
last_n_periods AS (
  SELECT
    Ticker,
    Date,
    trade_money,
    Volume,
    ROW_NUMBER() OVER (PARTITION BY Ticker ORDER BY Date DESC) AS period_rank
  FROM trade_money
),

avg_metrics AS (
  SELECT
    Ticker,
    AVG(trade_money) AS avg_trade_money,
    AVG(Volume)      AS avg_volume
  FROM last_n_periods
  WHERE period_rank <= {lookback}
  GROUP BY Ticker
),

latest_day AS (
  SELECT
    t.Ticker,
    t.Date AS latest_date,
    t."Close" AS latest_close,
    t.Volume AS latest_volume,
    t.trade_money AS latest_trade_money,
    t.pct_change AS latest_pct_change,
    ROW_NUMBER() OVER (PARTITION BY t.Ticker ORDER BY t.Date DESC) AS rn
  FROM trade_money t
),

combined AS (
  SELECT
    l.Ticker,
    l.latest_date,
    l.latest_close,
    ROUND(l.latest_pct_change, 2) AS pct_change,
    l.latest_volume,
    a.avg_volume,
    ROUND(l.latest_volume * 1.0 / NULLIF(a.avg_volume, 0), 2) AS rvol,
    l.latest_trade_money,
    a.avg_trade_money,
    ROUND(l.latest_trade_money * 1.0 / NULLIF(a.avg_trade_money, 0), 2) AS relative_dollar_vol,
    CASE
      WHEN l.latest_volume * 1.0 / NULLIF(a.avg_volume, 0) >= {strong_accel} THEN 'Strong Acceleration'
      WHEN l.latest_volume * 1.0 / NULLIF(a.avg_volume, 0) >= {accel} THEN 'Acceleration'
      WHEN l.latest_volume * 1.0 / NULLIF(a.avg_volume, 0) <= {strong_decel} THEN 'Strong Deceleration'
      WHEN l.latest_volume * 1.0 / NULLIF(a.avg_volume, 0) <= {decel} THEN 'Deceleration'
      ELSE 'Normal'
    END AS volume_regime
  FROM latest_day l
  JOIN avg_metrics a ON l.Ticker = a.Ticker
  WHERE l.rn = 1
),

total_avg AS (
  SELECT SUM(avg_trade_money) AS total_avg_trade_money
  FROM avg_metrics
)

SELECT
  c.Ticker,
  c.latest_date,
  c.latest_close,
  c.pct_change,
  c.latest_volume,
  c.avg_volume,
  c.rvol,
  ROUND(c.latest_trade_money, 0) AS latest_dollar_volume,
  ROUND(c.avg_trade_money, 0) AS avg_dollar_volume,
  c.relative_dollar_vol,
  c.volume_regime,
  ROUND((c.avg_trade_money / t.total_avg_trade_money) * 100, 2) AS pct_of_total_avg_dollar
FROM combined c
CROSS JOIN total_avg t
{regime_where}ORDER BY c.latest_trade_money DESC
LIMIT {limit}
"""


def build_accel_sql(
    lookback: int = DEFAULT_LOOKBACK,
    accel: float = DEFAULT_ACCEL,
    strong_accel: float = DEFAULT_STRONG_ACCEL,
    top_k: int = DEFAULT_TOP_K,
) -> str:
    """Tab B query — acceleration candidates only.

    Adapted from ``docs/report_idea/report_Top 25 by Approximate Dollar
    Volume.md`` (the file whose *name* says "Top 25" but whose content
    is the acceleration-candidates screen): ranks every ticker by
    latest dollar volume, keeps the ``top_k`` most-liquid names, then
    keeps only ``rvol >= accel`` (labelled 'Acceleration', or 'Strong
    Acceleration' when ``rvol >= strong_accel``), ordered by ``rvol``
    descending.
    """
    lookback = int(_require_number("lookback", lookback))
    accel = float(_require_number("accel", accel))
    strong_accel = float(_require_number("strong_accel", strong_accel))
    top_k = int(_require_number("top_k", top_k))

    return f"""
WITH trade_money AS (
  SELECT
    Ticker,
    Date,
    Open,
    High,
    Low,
    "Close",
    Volume,
    ((Open + High + Low + "Close") / 4.0) * Volume AS trade_money,
    (("Close" - LAG("Close") OVER (PARTITION BY Ticker ORDER BY Date))
      / NULLIF(LAG("Close") OVER (PARTITION BY Ticker ORDER BY Date), 0)) * 100 AS pct_change
  FROM daily_close
),

last_n_periods AS (
  SELECT
    Ticker,
    Date,
    trade_money,
    Volume,
    ROW_NUMBER() OVER (PARTITION BY Ticker ORDER BY Date DESC) AS period_rank
  FROM trade_money
),

avg_metrics AS (
  SELECT
    Ticker,
    AVG(trade_money) AS avg_trade_money,
    AVG(Volume)      AS avg_volume
  FROM last_n_periods
  WHERE period_rank <= {lookback}
  GROUP BY Ticker
),

latest_day AS (
  SELECT
    t.Ticker,
    t.Date AS latest_date,
    t."Close" AS latest_close,
    t.Volume AS latest_volume,
    t.trade_money AS latest_trade_money,
    t.pct_change AS latest_pct_change,
    ROW_NUMBER() OVER (PARTITION BY t.Ticker ORDER BY t.Date DESC) AS rn
  FROM trade_money t
),

combined AS (
  SELECT
    l.Ticker,
    l.latest_date,
    l.latest_close,
    ROUND(l.latest_pct_change, 2) AS pct_change,
    l.latest_volume,
    a.avg_volume,
    ROUND(l.latest_volume * 1.0 / NULLIF(a.avg_volume, 0), 2) AS rvol,
    l.latest_trade_money,
    a.avg_trade_money,
    ROUND(l.latest_trade_money * 1.0 / NULLIF(a.avg_trade_money, 0), 2) AS relative_dollar_vol,
    CASE
      WHEN l.latest_volume * 1.0 / NULLIF(a.avg_volume, 0) >= {strong_accel} THEN 'Strong Acceleration'
      WHEN l.latest_volume * 1.0 / NULLIF(a.avg_volume, 0) >= {accel} THEN 'Acceleration'
      ELSE 'Other'
    END AS volume_regime
  FROM latest_day l
  JOIN avg_metrics a ON l.Ticker = a.Ticker
  WHERE l.rn = 1
),

-- Rank all stocks by latest dollar volume first (to keep only liquid names)
ranked_by_dollar AS (
  SELECT
    *,
    ROW_NUMBER() OVER (ORDER BY latest_trade_money DESC) AS dollar_rank
  FROM combined
)

SELECT
  Ticker,
  latest_date,
  latest_close,
  pct_change,
  latest_volume,
  avg_volume,
  rvol,
  ROUND(latest_trade_money, 0) AS latest_dollar_volume,
  ROUND(avg_trade_money, 0) AS avg_dollar_volume,
  relative_dollar_vol,
  volume_regime
FROM ranked_by_dollar
WHERE volume_regime IN ('Acceleration', 'Strong Acceleration')
  AND dollar_rank <= {top_k}
ORDER BY rvol DESC, latest_trade_money DESC
"""


# ------------------------------------------------------------------
# Data access — Flight first, direct read-only DuckDB fallback.
# ------------------------------------------------------------------


def _query_via_flight(location: str, sql: str) -> pd.DataFrame:
    """Run ``sql`` through the Flight service's ``do_get``.

    Sends a JSON ``{"sql": ...}`` ticket — the same contract the
    Flight server's ``do_get`` implements — after a short
    ``wait_for_available`` probe.  The call is bounded by
    ``_FLIGHT_GET_TIMEOUT_S`` so a wedged server cannot hang the app.
    Raises on any failure; the caller decides whether to fall back.
    """
    client = flight.FlightClient(location)
    client.wait_for_available(timeout=_FLIGHT_CONNECT_TIMEOUT_S)
    ticket = flight.Ticket(json.dumps({"sql": sql}).encode("utf-8"))
    options = flight.FlightCallOptions(timeout=_FLIGHT_GET_TIMEOUT_S)
    reader = client.do_get(ticket, options=options)
    return reader.read_all().to_pandas()


def _query_via_duckdb(db_path: str, sql: str) -> pd.DataFrame:
    """Run ``sql`` on a direct *read-only* DuckDB connection.

    Only usable when no process holds the database write lock (i.e.
    the Flight service / ingestion daemon is not running).
    """
    conn = duckdb.connect(db_path, read_only=True)
    try:
        return conn.execute(sql).df()
    finally:
        conn.close()


def fetch_dataframe(
    sql: str, config: PipelineConfig
) -> Optional[pd.DataFrame]:
    """Fetch report rows: Flight first, direct read-only DB second.

    Returns ``None`` when both paths fail (each failure is logged);
    the UI turns that into ``st.error``.
    """
    location = (
        f"grpc://{config.server.flight_host}:{config.server.flight_port}"
    )
    try:
        return _query_via_flight(location, sql)
    except Exception as exc:
        logger.warning(
            "Flight server at %s unreachable or query failed (%s); "
            "falling back to a direct read-only DuckDB query.",
            location,
            exc,
        )

    try:
        return _query_via_duckdb(config.db_path, sql)
    except Exception:
        logger.exception(
            "Direct read-only DuckDB query also failed "
            "(write lock held or database unavailable)."
        )
        return None


# ------------------------------------------------------------------
# xlsx export (in-memory, for st.download_button).
# ------------------------------------------------------------------


def dataframe_to_xlsx_bytes(
    df: pd.DataFrame, sheet_name: str = "Report"
) -> bytes:
    """Serialize ``df`` to xlsx bytes with auto-adjusted column widths.

    Each column's width is set to the maximum rendered content length
    (header included) plus ``_WIDTH_PADDING`` — same convention as
    ``scripts/report_top25_active.py``.
    """
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
        worksheet = writer.sheets[sheet_name]
        # justified: openpyxl exposes no vectorized API for per-column
        # width sizing — each column's cells must be measured one
        # column at a time (small report sheets, negligible cost).
        for column_cells in worksheet.columns:
            max_len = max(
                len(str(cell.value)) if cell.value is not None else 0
                for cell in column_cells
            )
            letter = column_cells[0].column_letter
            worksheet.column_dimensions[letter].width = (
                max_len + _WIDTH_PADDING
            )
    return buffer.getvalue()


# ------------------------------------------------------------------
# Streamlit UI (streamlit is imported lazily so this module stays
# importable and testable without the Streamlit runtime).
# ------------------------------------------------------------------


def _render_top25_tab(st, config: PipelineConfig) -> None:
    """Tab A — Top 25 by Dollar Volume (+ RVOL/regime edge metrics)."""
    with st.expander("Parameters", expanded=True):
        col1, col2, col3 = st.columns(3)
        with col1:
            lookback = st.number_input(
                "Lookback periods",
                min_value=2,
                max_value=500,
                value=DEFAULT_LOOKBACK,
                step=1,
                key="a_lookback",
            )
            limit = st.number_input(
                "Top-N limit",
                min_value=1,
                max_value=500,
                value=DEFAULT_LIMIT,
                step=1,
                key="a_limit",
            )
        with col2:
            strong_accel = st.number_input(
                "Strong Acceleration RVOL >=",
                min_value=0.0,
                value=DEFAULT_STRONG_ACCEL,
                step=0.1,
                key="a_strong_accel",
            )
            accel = st.number_input(
                "Acceleration RVOL >=",
                min_value=0.0,
                value=DEFAULT_ACCEL,
                step=0.1,
                key="a_accel",
            )
        with col3:
            decel = st.number_input(
                "Deceleration RVOL <=",
                min_value=0.0,
                value=DEFAULT_DECEL,
                step=0.1,
                key="a_decel",
            )
            strong_decel = st.number_input(
                "Strong Deceleration RVOL <=",
                min_value=0.0,
                value=DEFAULT_STRONG_DECEL,
                step=0.1,
                key="a_strong_decel",
            )
        regimes = st.multiselect(
            "Volume regime filter (empty = all)",
            options=list(VOLUME_REGIMES),
            default=list(VOLUME_REGIMES),
            key="a_regimes",
        )

    try:
        sql = build_top25_sql(
            lookback=lookback,
            limit=limit,
            strong_accel=strong_accel,
            accel=accel,
            decel=decel,
            strong_decel=strong_decel,
            regimes=regimes,
        )
    except (TypeError, ValueError) as exc:
        st.error(f"Invalid parameters: {exc}")
        return

    df = fetch_dataframe(sql, config)
    if df is None:
        st.error(
            "Could not fetch report data — the Flight server is "
            "unreachable and the direct read-only DuckDB fallback "
            "failed (the database write lock is probably held by "
            "another process, or the database file is unavailable)."
        )
        return

    st.caption(f"{len(df)} row(s)")
    st.dataframe(df, width="stretch")
    if not df.empty and "latest_dollar_volume" in df.columns:
        st.bar_chart(df, x="Ticker", y="latest_dollar_volume")
    st.download_button(
        label="Download as Excel",
        data=dataframe_to_xlsx_bytes(df, sheet_name=_TAB_A_SHEET),
        file_name=_TAB_A_FILENAME,
        mime=(
            "application/vnd.openxmlformats-officedocument."
            "spreadsheetml.sheet"
        ),
        key="a_download",
    )


def _render_accel_tab(st, config: PipelineConfig) -> None:
    """Tab B — Acceleration Candidates (rvol >= threshold among the
    top-K most-liquid names, ordered by rvol descending)."""
    with st.expander("Parameters", expanded=True):
        col1, col2, col3 = st.columns(3)
        with col1:
            lookback = st.number_input(
                "Lookback periods",
                min_value=2,
                max_value=500,
                value=DEFAULT_LOOKBACK,
                step=1,
                key="b_lookback",
            )
            top_k = st.number_input(
                "Liquidity top-K",
                min_value=1,
                max_value=2000,
                value=DEFAULT_TOP_K,
                step=1,
                key="b_top_k",
            )
        with col2:
            accel = st.number_input(
                "Acceleration RVOL >=",
                min_value=0.0,
                value=DEFAULT_ACCEL,
                step=0.1,
                key="b_accel",
            )
        with col3:
            strong_accel = st.number_input(
                "Strong Acceleration RVOL >=",
                min_value=0.0,
                value=DEFAULT_STRONG_ACCEL,
                step=0.1,
                key="b_strong_accel",
            )

    try:
        sql = build_accel_sql(
            lookback=lookback,
            accel=accel,
            strong_accel=strong_accel,
            top_k=top_k,
        )
    except (TypeError, ValueError) as exc:
        st.error(f"Invalid parameters: {exc}")
        return

    df = fetch_dataframe(sql, config)
    if df is None:
        st.error(
            "Could not fetch report data — the Flight server is "
            "unreachable and the direct read-only DuckDB fallback "
            "failed (the database write lock is probably held by "
            "another process, or the database file is unavailable)."
        )
        return

    st.caption(f"{len(df)} row(s)")
    st.dataframe(df, width="stretch")
    if not df.empty and "rvol" in df.columns:
        st.bar_chart(df, x="Ticker", y="rvol")
    st.download_button(
        label="Download as Excel",
        data=dataframe_to_xlsx_bytes(df, sheet_name=_TAB_B_SHEET),
        file_name=_TAB_B_FILENAME,
        mime=(
            "application/vnd.openxmlformats-officedocument."
            "spreadsheetml.sheet"
        ),
        key="b_download",
    )


def main() -> None:
    """Streamlit entrypoint — run via ``streamlit run scripts/report_app.py``."""
    import streamlit as st

    st.set_page_config(page_title="Stock Reports", layout="wide")
    st.title("Stock Reports")

    try:
        config = PipelineConfig.from_yaml(_CONFIG_PATH)
    except Exception as exc:
        st.error(f"Failed to load configuration ({_CONFIG_PATH}): {exc}")
        return

    tab_top25, tab_accel = st.tabs(
        ["Top 25 by Dollar Volume", "Acceleration Candidates"]
    )
    with tab_top25:
        _render_top25_tab(st, config)
    with tab_accel:
        _render_accel_tab(st, config)


# ``streamlit run`` executes this file as __main__, while unit tests
# import it as a plain module — the guard keeps imports side-effect
# free (no widgets rendered, no data fetched during test collection).
if __name__ == "__main__":
    main()
