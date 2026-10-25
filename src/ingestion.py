"""YFinance ingestion engine for the stock-ingestion pipeline.

Implements ``IngestionEngine`` per spec §10 (reference implementation):
batch download via ``yf.download``, per-ticker split detection with
DELETE + re-download, dataframe normalization, and the three entry
points (``run_initial_load``, ``run_daily_load``, ``run_on_request_load``).

Spec reference: docs/tech_spec/gemini_tech_spec.1.0.2.md §10.
"""

from __future__ import annotations

import logging
import os
import queue
import re
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import duckdb
import numpy as np
import pandas as pd
import pandas_market_calendars as mcal
import requests
import yfinance as yf

from src import client_dml, signal_analysis, signal_pipeline
from src.config import PipelineConfig
from src.db import DatabaseManager
from src.r2_sync import R2SyncManager

logger = logging.getLogger(__name__)


# =====================================================================
# CNN Fear & Greed index (F16-T1)
# =====================================================================
#
# Live API facts (verified 2026-09-20) — the reference doc
# docs/enhancements/2260920.md uses stale key names that do NOT exist
# in the real payload (stock_price_momentum, market_volatility).
#
# URL: https://production.dataviz.cnn.io/index/fearandgreed/graphdata/
#      {start_date} — returns HTTP 418 without browser headers.
# Retention (verified 2026-09-20): CNN keeps only ~6 years of history —
# the dated endpoint returns HTTP 500 for start_dates behind the
# retention floor (e.g. 2020-07-01 fails while 2020-08-01 works), and
# the undated ``graphdata`` endpoint returns a rolling ~1 year window.
# run_fear_greed_load therefore falls back to the undated URL when the
# dated request raises an HTTPError.
# Payload top-level keys: ``fear_and_greed`` (current snapshot),
# ``fear_and_greed_historical`` ({data: [{x: ms-epoch float,
# y: 0-100 score, rating: str}]}) and one block per sub-indicator
# ({timestamp, score, rating, data: [{x, y, rating}]}) — where ``y`` is
# the RAW metric (VIX level, put/call ratio, SPX price, breadth
# oscillator), not a 0-100 score.

FEAR_GREED_BASE_URL = (
    "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
)
FEAR_GREED_URL_TEMPLATE = FEAR_GREED_BASE_URL + "/{}"

FEAR_GREED_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/126.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.cnn.com/markets/fear-and-greed",
    "Origin": "https://www.cnn.com",
    "Accept": "application/json",
}

# The 9 CNN sub-indicator payload keys mapped to their column prefix on
# fear_greed_history (the CNN key verbatim — each yields a
# ``<key> DOUBLE`` + ``<key>_rating VARCHAR`` column pair).
FEAR_GREED_SERIES: Dict[str, str] = {
    "stock_price_strength": "stock_price_strength",
    "stock_price_breadth": "stock_price_breadth",
    "market_momentum_sp500": "market_momentum_sp500",
    "market_momentum_sp125": "market_momentum_sp125",
    "market_volatility_vix": "market_volatility_vix",
    "market_volatility_vix_50": "market_volatility_vix_50",
    "put_call_options": "put_call_options",
    "junk_bond_demand": "junk_bond_demand",
    "safe_haven_demand": "safe_haven_demand",
}

# Static wide-frame column list: date, fg_score, fg_rating, then each
# series' <prefix> / <prefix>_rating pair.  Mirrors
# db.FEAR_GREED_UPSERT_COLUMNS.
FEAR_GREED_COLUMNS: List[str] = (
    ["date", "fg_score", "fg_rating"]
    + [c for prefix in FEAR_GREED_SERIES.values() for c in (prefix, f"{prefix}_rating")]
)

_FEAR_GREED_DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")


class IngestionEngine:
    """YFinance ingestion engine backed by a ``DatabaseManager``.

    Coordinates batch pricing downloads, per-ticker split detection
    (delete + re-download), dataframe normalization, and the three
    operational entry points: initial load, daily load, and on-request
    load.
    """

    #: Seconds to wait before the second-pass retry download, letting a
    #: yfinance rate limit (HTTP 429) cool down.  Mockable in tests via
    #: ``time.sleep``.
    _RETRY_DELAY_S: float = 15.0

    def __init__(self, db_mgr: DatabaseManager, cfg: PipelineConfig):
        self.db = db_mgr
        self.cfg = cfg

    # ------------------------------------------------------------------
    # Split detection (per-ticker — no batch API exists)
    # ------------------------------------------------------------------

    def _check_and_apply_splits(self, tickers: List[str]) -> None:
        """Per-ticker split detection, batched re-download.

        For each ticker, query ``yf.Ticker(t).splits`` (no batch API).
        Splits dated before the ticker's earliest stored row are ignored:
        they were already baked into the ``auto_adjust=True`` prices at
        download time.  Tickers with no stored data are skipped entirely.

        All tickers with unprocessed splits are re-downloaded together in a
        single ``yf.download`` batch from the minimum earliest date across
        the affected tickers.  This avoids one API call per split ticker.

        The re-download happens BEFORE any destructive writes: only tickers
        that actually returned rows have their old data deleted, the new
        data upserted, and their splits recorded as processed.  A ticker
        missing from the re-download keeps its existing data and its
        splits stay unprocessed so a later run retries them — a failed or
        empty re-download can therefore never wipe stored history.
        """
        # Phase 1 — collect affected tickers and their unprocessed splits.
        # justified: yfinance has no batch split API — yf.Ticker(t).splits
        # must be called per-ticker. This is an external API constraint,
        # not a choice we can vectorize away.
        affected: List[Tuple[str, str, List[Tuple[str, float]]]] = []
        for t in tickers:
            try:
                earliest_date = self.db.get_ticker_earliest_date(t)
                if earliest_date is None:
                    # No stored data: adjusted prices downloaded from now on
                    # already reflect every historical split.
                    continue

                stock = yf.Ticker(t)
                splits = stock.splits
                if splits is None or splits.empty:
                    continue

                # Find unprocessed splits on/after the ticker's earliest row
                unprocessed: List[Tuple[str, float]] = []
                for split_dt, factor in splits.sort_index().items():
                    split_date = split_dt.strftime("%Y-%m-%d")
                    if split_date < earliest_date:
                        continue
                    if not self.db.is_split_processed(t, split_date):
                        unprocessed.append((split_date, float(factor)))

                if unprocessed:
                    affected.append((t, earliest_date, unprocessed))

            except Exception as e:
                logger.error("Failed checking splits for ticker '%s': %s", t, e)

        if not affected:
            return

        # Phase 2 — single batched re-download for all affected tickers,
        # performed BEFORE any destructive writes.  The previous ordering
        # (record splits -> delete history -> re-download) permanently lost
        # data when the re-download failed or returned empty (production
        # incident: BKNG, BWA, FNF, FTAI, LOGI, RLI).
        min_start = min(earliest for _, earliest, _ in affected)
        today = datetime.now().strftime("%Y-%m-%d")
        affected_tickers = [t for t, _, _ in affected]

        logger.warning(
            "RUNTIME EVENT: Stock split(s) detected for %d ticker(s): %s. "
            "Re-downloading from %s to %s in one batch before replacing data.",
            len(affected_tickers),
            affected_tickers,
            min_start,
            today,
        )

        try:
            df_redownload = self._batch_download(affected_tickers, min_start, today)
            df_norm, redl_ok, _redl_failed, _redl_errors = self._normalize_batch_df(
                df_redownload, affected_tickers
            )
        except Exception as e:
            logger.error(
                "Failed batch re-download for split tickers %s: %s. "
                "Existing data preserved; splits left unprocessed for retry.",
                affected_tickers,
                e,
            )
            return

        # Phase 3 — replace data only for tickers that actually returned
        # rows in the re-download.  Tickers missing from the re-download
        # keep their existing data and their splits stay unprocessed so a
        # later run retries them.
        redownloaded = set(redl_ok)
        missing = [t for t, _, _ in affected if t not in redownloaded]
        if missing:
            logger.warning(
                "RUNTIME EVENT: Re-download returned no rows for %d of %d "
                "split ticker(s): %s. Existing data kept; their splits stay "
                "unprocessed and will be retried on a later run.",
                len(missing),
                len(affected_tickers),
                missing,
            )

        if not redownloaded:
            return

        # justified: delete_ticker_data is the existing per-ticker DB API;
        # deletes are fast SQL and keeping the per-ticker call avoids
        # breaking tests/downstream code that mock delete_ticker_data.
        for t, _earliest, _unprocessed in affected:
            if t not in redownloaded:
                continue
            self.db.delete_ticker_data(t)

        self.db.upsert_dataframe(df_norm)

        # Record splits ONLY for tickers whose data was actually replaced;
        # splits for missing tickers remain unprocessed for the next run.
        split_records = [
            (t, split_date, factor)
            for t, _earliest, unprocessed in affected
            if t in redownloaded
            for split_date, factor in unprocessed
        ]
        if split_records:
            self.db.batch_record_splits(split_records)

        logger.info(
            "Re-download complete for %d ticker(s): %s",
            len(redownloaded),
            sorted(redownloaded),
        )

    def _record_historical_splits(self, tickers: List[str]) -> None:
        """Record all historical splits for ``tickers`` as processed.

        After a full historical download (initial load or backfill), the
        data already reflects every historical split via
        ``auto_adjust=True``.  Recording those splits in
        ``splits_processed`` prevents the next incremental run from
        treating them as new and re-downloading them unnecessarily.

        Only splits on/after the ticker's earliest stored date are
        recorded; earlier splits are already baked into the adjusted
        prices and are irrelevant for the stored window.
        """
        all_split_records: List[Tuple[str, str, float]] = []
        for t in tickers:
            # justified: yfinance has no batch split API — yf.Ticker(t).splits
            # must be called per-ticker. This is an external API constraint.
            try:
                earliest_date = self.db.get_ticker_earliest_date(t)
                if earliest_date is None:
                    continue

                stock = yf.Ticker(t)
                splits = stock.splits
                if splits is None or splits.empty:
                    continue

                for split_dt, factor in splits.sort_index().items():
                    split_date = split_dt.strftime("%Y-%m-%d")
                    if split_date < earliest_date:
                        continue
                    all_split_records.append((t, split_date, float(factor)))

            except Exception as e:
                logger.error("Failed recording historical splits for '%s': %s", t, e)

        if all_split_records:
            self.db.batch_record_splits(all_split_records)
            logger.info(
                "Recorded %d historical split(s) for %d ticker(s) as processed.",
                len(all_split_records),
                len({t for t, _, _ in all_split_records}),
            )

    # ------------------------------------------------------------------
    # Batch download
    # ------------------------------------------------------------------

    def _batch_download(
        self,
        tickers: List[str],
        start_date: str,
        end_date: str,
        threads: bool = True,
    ) -> pd.DataFrame:
        """Batch download via ``yf.download()``.

        ``end_date`` is treated as inclusive, so it is adjusted +1 day
        internally to satisfy yfinance's exclusive ``end`` parameter.
        Downloads use ``auto_adjust=False`` (R1A-T1: one batch pull
        returns both the raw ``Close`` — stored as ``RawClose`` — and
        ``Adj Close``, whose per-row ratio reconstructs the historical
        adjusted OHLC contract), ``interval="1d"``,
        ``group_by='ticker'`` and ``threads=True`` — pass
        ``threads=False`` for gentler pacing (the second-pass retry
        does this after a rate-limit cool-down).  ``progress=False``
        disables the tqdm progress bar: the Flight server runs headless
        under Windows Task Scheduler (no console, ``sys.stderr`` is
        ``None``), where the bar's finalizer crashes on
        ``sys.stderr.flush()`` *after* the download completes — losing
        the whole load.  The bar is meaningless in a service anyway.
        """
        end_dt = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)
        df = yf.download(
            tickers=tickers,
            start=start_date,
            end=end_dt.strftime("%Y-%m-%d"),
            interval="1d",
            auto_adjust=False,
            group_by="ticker",
            threads=threads,
            progress=False,
        )
        return df

    # ------------------------------------------------------------------
    # Normalization
    # ------------------------------------------------------------------

    def _confirm_delisted(self, ticker: str) -> bool:
        """Cheap independent confirmation that ``ticker`` has no data.

        Runs a small single-ticker ``yf.download`` over a recent short
        window.  Returns ``True`` only when that second, independent
        request also comes back empty — i.e. the delisting is confirmed
        by two empty responses.  Returned rows OR an unexpected error
        both mean *unconfirmed*: yfinance under rate-limiting reports
        valid tickers as "possibly delisted", and a throttle must never
        wipe real history, so the caller must then skip ``purge_ticker``.
        """
        try:
            recent = yf.download(
                tickers=ticker,
                period="1mo",
                interval="1d",
                auto_adjust=True,
                progress=False,
            )
        except Exception as e:
            logger.warning(
                "Delisting check for '%s' errored (%s) — delisting "
                "unconfirmed; skipping purge.",
                ticker,
                e,
            )
            return False
        if recent is not None and not recent.empty:
            logger.warning(
                "Delisting check for '%s' returned data — delisting "
                "unconfirmed; skipping purge.",
                ticker,
            )
            return False
        return True

    def _normalize_batch_df(
        self, df: pd.DataFrame, tickers: List[str]
    ) -> Tuple[pd.DataFrame, List[str], List[str], Dict[str, str]]:
        """Normalize batch ``yf.download`` output to the flat schema.

        Handles both multi-ticker (``columns.nlevels == 2``, in either
        ``(ticker, field)`` or ``(field, ticker)`` level order) and
        single-ticker output.  Drops rows with NaN OHLC.  R1A-T1: the
        raw ``Close`` becomes ``RawClose``; when the ``Adj Close``
        field is present the per-row ``Adj Close / Close`` ratio
        scales OHLC and ``Close`` back to the historical
        ``auto_adjust=True`` contract, and rows that cannot produce
        both an adjusted Close and a finite RawClose are dropped rather
        than stored with NULL. Zero and negative raw market values are
        valid (for example ^IRX and WTI).  An empty
        ticker that is in ``exclude_tickers`` is purged from the DB
        only after ``_confirm_delisted`` independently verifies the
        delisting (a rate-limit throttle must never wipe real history);
        an empty ticker not in ``exclude_tickers`` produces a warning
        log only (no purge).  Returns
        ``(df, succeeded, failed, errors)``.
        """
        records: List[pd.DataFrame] = []
        succeeded: List[str] = []
        failed: List[str] = []
        errors: Dict[str, str] = {}

        for t in tickers:
            # justified: yf.download returns MultiIndex columns per ticker.
            # Each ticker's sub-frame needs individual dropna + schema
            # normalization. pandas doesn't support vectorized per-key
            # dropna on a MultiIndex DataFrame.
            try:
                if df.columns.nlevels == 2:
                    # Multi-ticker MultiIndex — historically
                    # (ticker, field) via group_by='ticker', but current
                    # yfinance may emit (field, ticker); support both.
                    if t in set(df.columns.get_level_values(0)):
                        ticker_df = df[t].copy()
                    else:
                        ticker_df = df.xs(t, axis=1, level=1).copy()
                else:
                    # Single ticker: columns are flat
                    ticker_df = df.copy()

                ticker_df = ticker_df.dropna(subset=["Open", "High", "Low", "Close"])
                if ticker_df.empty:
                    if t in self.cfg.ingestion.exclude_tickers:
                        if self._confirm_delisted(t):
                            logger.warning(
                                "RUNTIME EVENT: Ticker '%s' returned empty "
                                "dataset; delisting independently confirmed. "
                                "Matches exclusion criteria.",
                                t,
                            )
                            self.db.purge_ticker(t)
                        else:
                            logger.warning(
                                "RUNTIME EVENT: Ticker '%s' returned empty "
                                "dataset and matches exclusion criteria, but "
                                "delisting unconfirmed — NOT purging.",
                                t,
                            )
                    else:
                        logger.warning(
                            "RUNTIME EVENT: Ticker '%s' returned empty dataset. "
                            "Not in exclude list — skipped (no purge). "
                            "This may indicate a temporary data outage.",
                            t,
                        )
                    failed.append(t)
                    errors[t] = "Empty dataset"
                    continue

                # R1A-T1: the downloaded ``Close`` is RAW (batch pulls
                # run auto_adjust=False).  Derive the per-row
                # adjustment ratio ``Adj Close / Close`` and scale OHLC
                # so Close/Open/High/Low keep the existing adjusted
                # (total-return) contract while ``RawClose`` carries
                # the unadjusted close.  Rows lacking a finite raw close
                # or usable adjustment are dropped — never stored with a
                # NULL RawClose. Zero/negative market values remain valid.
                raw_close = pd.to_numeric(ticker_df["Close"], errors="coerce")
                if "Adj Close" in ticker_df.columns:
                    adj_close = pd.to_numeric(
                        ticker_df["Adj Close"], errors="coerce"
                    )
                    with np.errstate(divide="ignore", invalid="ignore"):
                        ratio = adj_close / raw_close
                    # A zero raw close cannot yield a ratio. Only a
                    # finite zero/zero market bar (^IRX) has a defined
                    # identity adjustment; zero raw with nonzero adjusted
                    # Close is contradictory and must be dropped.
                    zero_pair = (raw_close == 0) & (adj_close == 0)
                    ratio = ratio.mask(zero_pair, 1.0)
                    valid = (
                        np.isfinite(raw_close) & np.isfinite(adj_close)
                        & np.isfinite(ratio)
                    )
                else:
                    # No Adj Close field in the frame (pre-adjusted or
                    # foreign-sourced pull): implicit ratio of 1.
                    adj_close = None
                    ratio = pd.Series(1.0, index=ticker_df.index)
                    valid = np.isfinite(raw_close)
                ticker_df = ticker_df.loc[valid].copy()
                if ticker_df.empty:
                    logger.warning(
                        "RUNTIME EVENT: Ticker '%s' produced no rows with a "
                        "finite raw close and valid adjustment — "
                        "skipped (nothing stored).",
                        t,
                    )
                    failed.append(t)
                    errors[t] = "No rows with valid RawClose/adjustment"
                    continue
                ratio = ratio.loc[ticker_df.index]
                raw_close = raw_close.loc[ticker_df.index]
                ticker_df[["Open", "High", "Low"]] = ticker_df[
                    ["Open", "High", "Low"]
                ].mul(ratio, axis=0)
                ticker_df["Close"] = (
                    adj_close.loc[ticker_df.index]
                    if adj_close is not None
                    else raw_close
                )
                ticker_df["RawClose"] = raw_close

                ticker_df = ticker_df.reset_index()
                ticker_df["Ticker"] = t
                ticker_df["Date"] = pd.to_datetime(ticker_df["Date"]).dt.date
                records.append(
                    ticker_df[
                        ["Date", "Ticker", "Open", "High", "Low", "Close",
                         "Volume", "RawClose"]
                    ]
                )
                succeeded.append(t)
            except Exception as e:
                logger.error("Failed processing ingestion for ticker '%s': %s", t, e)
                failed.append(t)
                errors[t] = str(e)
                if t in self.cfg.ingestion.exclude_tickers:
                    # Same safeguard as the empty-data path: only purge
                    # when an independent check also finds no data.
                    if self._confirm_delisted(t):
                        self.db.purge_ticker(t)
                    else:
                        logger.warning(
                            "RUNTIME EVENT: Ticker '%s' failed processing "
                            "and matches exclusion criteria, but delisting "
                            "unconfirmed — NOT purging.",
                            t,
                        )

        if records:
            out = pd.concat(records, ignore_index=True)
            return self._apply_exclude_rows(out), succeeded, failed, errors
        return pd.DataFrame(), succeeded, failed, errors

    def _apply_exclude_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        """Drop normalized rows matching ``ingestion.exclude_rows``.

        INC-T6 enforcement: vendor artifact bars (delisting-terminal
        placeholders) must never reach ``daily_close``.  Applied here —
        the single choke point every download path (initial / daily /
        on_request, retry, and split re-download) flows through before
        ``upsert_dataframe``.  The ``Date`` column may hold
        ``datetime.date`` objects or strings depending on the stage, so
        both sides are normalized to ``YYYY-MM-DD`` for the (Ticker,
        Date) match.  Logs a RUNTIME EVENT with the dropped count when
        >0 and returns the filtered frame.
        """
        ingestion_cfg = getattr(self.cfg, "ingestion", None)
        pairs = getattr(ingestion_cfg, "exclude_rows", None)
        if not isinstance(pairs, (list, tuple)) or not pairs or df.empty:
            return df
        keys = {
            f"{str(e.get('ticker', '')).strip().upper()}|"
            f"{str(e.get('date', '')).strip()}"
            for e in pairs
            if isinstance(e, dict)
        }
        if not keys:
            return df
        date_str = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")
        row_keys = (
            df["Ticker"].astype(str).str.strip().str.upper() + "|" + date_str
        )
        mask = row_keys.isin(keys)
        dropped = int(mask.sum())
        if not dropped:
            return df
        logger.warning(
            "RUNTIME EVENT: exclude_rows dropped %d row(s) matching "
            "configured (ticker, date) exclusions: %s",
            dropped,
            sorted(set(row_keys[mask])),
        )
        return df.loc[~mask].reset_index(drop=True)

    # ------------------------------------------------------------------
    # Second-pass retry for transient empty-dataset failures
    # ------------------------------------------------------------------

    def _retry_failed_tickers(
        self, failed: List[str], start_date: str, end_date: str
    ) -> Tuple[pd.DataFrame, List[str], List[str], Dict[str, str]]:
        """One-shot second-pass download for empty-dataset failures.

        yfinance under rate-limiting (HTTP 429) returns "possibly
        delisted; no price data found" for *valid* tickers — observed
        live during the 1210-ticker 10-year load and in daily loads.
        A single follow-up download after a short cool-down recovers
        most of these transient failures.  Only the failed tickers are
        re-requested, with ``threads=False`` for gentler pacing.

        Returns a ``(df, succeeded, failed, errors)`` tuple for the
        retried tickers only, in the same shape as
        ``_normalize_batch_df``.
        """
        logger.warning(
            "RUNTIME EVENT: %d ticker(s) returned empty datasets: %s. "
            "Retrying once after a %.0fs rate-limit cool-down.",
            len(failed),
            failed,
            self._RETRY_DELAY_S,
        )
        time.sleep(self._RETRY_DELAY_S)
        try:
            retry_raw = self._batch_download(
                failed, start_date, end_date, threads=False
            )
        except Exception as e:
            logger.error(
                "Retry download raised for %s: %s — tickers stay failed.",
                failed,
                e,
            )
            return (
                pd.DataFrame(),
                [],
                list(failed),
                {t: "Empty dataset" for t in failed},
            )
        if retry_raw.empty:
            logger.warning(
                "RUNTIME EVENT: Retry download returned no data for %d "
                "ticker(s): %s — they stay failed.",
                len(failed),
                failed,
            )
            return (
                pd.DataFrame(),
                [],
                list(failed),
                {t: "Empty dataset" for t in failed},
            )
        return self._normalize_batch_df(retry_raw, failed)

    # ------------------------------------------------------------------
    # Fetch + clean (split check + batch download + normalize)
    # ------------------------------------------------------------------

    def fetch_and_clean(
        self, tickers: List[str], start_date: str, end_date: str
    ) -> Tuple[pd.DataFrame, List[str], List[str], Dict[str, str]]:
        """Fetch pricing data (batch) and check splits (per-ticker).

        ``exclude_tickers`` are filtered out at the earliest stage —
        they are never downloaded, never split-checked, and never
        normalised.  Any excluded ticker that still has data in the
        DB is purged.  Returns ``(df, succeeded, failed, errors)``.
        """
        if not tickers:
            return pd.DataFrame(), [], [], {}

        # --- Earliest-stage exclusion filter + purge of stale data ---
        excluded = set(self.cfg.ingestion.exclude_tickers)
        effective_tickers = [t for t in tickers if t not in excluded]
        skipped = [t for t in tickers if t in excluded]
        if skipped:
            logger.info(
                "RUNTIME EVENT: Excluding tickers from download (in exclude_tickers): %s",
                skipped,
            )
            # Bulk-purge any excluded tickers that still have data in the DB
            existing = self.db.get_existing_tickers()
            stale = list(set(skipped) & existing)
            if stale:
                logger.warning(
                    "RUNTIME EVENT: Purging stale data for excluded tickers: %s",
                    stale,
                )
                self.db.purge_tickers(stale)
        if not effective_tickers:
            return pd.DataFrame(), [], [], {}

        # --- Split detection (per-ticker, no batch API) ---
        self._check_and_apply_splits(effective_tickers)

        # --- Batch pricing download ---
        raw_df = self._batch_download(effective_tickers, start_date, end_date)
        if raw_df.empty:
            # All tickers returned nothing — vectorized dict comprehension
            df = pd.DataFrame()
            succeeded: List[str] = []
            failed: List[str] = list(effective_tickers)
            errors: Dict[str, str] = {
                t: "Empty dataset from yf.download" for t in effective_tickers
            }
        else:
            df, succeeded, failed, errors = self._normalize_batch_df(
                raw_df, effective_tickers
            )

        # --- Second pass: retry transient empty-dataset failures once ---
        # yfinance rate-limiting makes valid tickers report "possibly
        # delisted; no price data found".  Only empty-dataset failures
        # are retried — a per-ticker processing exception is
        # deterministic and re-downloading would not fix it.
        retry_candidates = [
            t for t in failed if errors.get(t, "").startswith("Empty dataset")
        ]
        if retry_candidates:
            retry_df, retry_ok, _retry_fail, retry_err = (
                self._retry_failed_tickers(
                    retry_candidates, start_date, end_date
                )
            )
            recovered = set(retry_ok)
            if recovered:
                logger.info(
                    "Retry recovered %d ticker(s): %s",
                    len(retry_ok),
                    retry_ok,
                )
                df = (
                    retry_df
                    if df.empty
                    else pd.concat([df, retry_df], ignore_index=True)
                )
                succeeded = succeeded + retry_ok
            failed = [t for t in failed if t not in recovered]
            errors = {t: e for t, e in errors.items() if t not in recovered}
            errors.update(retry_err)

        return df, succeeded, failed, errors

    # ------------------------------------------------------------------
    # Company metadata load (F10-T1) — rate-limit safe
    # ------------------------------------------------------------------

    # SEC-T3: the four company_metadata columns an ``overrides`` entry
    # may set (ticker is the key, never an overridable field).
    _METADATA_OVERRIDE_FIELDS = (
        "company_name",
        "sector",
        "industry",
        "quote_type",
    )

    @classmethod
    def _normalize_metadata_overrides(
        cls, overrides: Optional[List[Dict[str, Any]]]
    ) -> List[Dict[str, Any]]:
        """Validate + normalize a ``metadata_load`` ``overrides`` payload.

        ``overrides`` is a list of dicts; each must carry a ``ticker``
        string (non-empty after strip — normalized to upper case, same
        as the ``tickers`` argument) plus any subset of the four
        metadata fields as strings.  A field present with a ``None``
        value is treated as ABSENT (merge semantics preserve the stored
        value — only non-null provided fields overwrite).  Unknown keys,
        non-dict items, empty/non-string tickers, and non-string field
        values raise ``ValueError`` — validation runs before any write,
        so a malformed payload can never leave a half-applied
        correction.  Duplicate tickers collapse last-wins so the final
        correction in the list is the one applied.  Returns the
        normalized, deduped list (never ``None``).
        """
        if overrides is None:
            return []
        if not isinstance(overrides, list):
            raise ValueError(
                f"overrides must be a list of dicts, got {overrides!r}"
            )
        normalized: List[Dict[str, Any]] = []
        for i, item in enumerate(overrides):
            if not isinstance(item, dict):
                raise ValueError(
                    f"overrides[{i}] must be a dict, got {item!r}"
                )
            unknown = set(item) - {
                "ticker",
                *cls._METADATA_OVERRIDE_FIELDS,
            }
            if unknown:
                raise ValueError(
                    f"overrides[{i}] has unknown field(s): "
                    f"{sorted(unknown)} — allowed: ticker, "
                    f"{list(cls._METADATA_OVERRIDE_FIELDS)}"
                )
            ticker = item.get("ticker")
            if not isinstance(ticker, str) or not ticker.strip():
                raise ValueError(
                    f"overrides[{i}].ticker must be a non-empty string, "
                    f"got {ticker!r}"
                )
            entry: Dict[str, Any] = {"ticker": ticker.strip().upper()}
            for field in (
                "company_name",
                "sector",
                "industry",
                "quote_type",
            ):
                value = item.get(field)
                if value is None:
                    # Absent OR explicit null -> merge semantics keep
                    # the stored value; never an overwrite.
                    continue
                if not isinstance(value, str):
                    raise ValueError(
                        f"overrides[{i}].{field} must be a string or "
                        f"null, got {value!r}"
                    )
                entry[field] = value
            normalized.append(entry)
        # Last-wins dedup keyed on the normalized ticker.
        deduped = {e["ticker"]: e for e in normalized}
        return list(deduped.values())

    @staticmethod
    def _merge_metadata_override(
        override: Dict[str, Any], stored_row: Optional[tuple]
    ) -> Dict[str, Any]:
        """Fill absent override fields from the stored metadata row.

        ``stored_row`` is ``(ticker, company_name, sector, industry,
        quote_type)`` or ``None`` (new ticker -> NULLs for every field
        not provided).  Provided override fields always win.
        """
        row = {
            "ticker": override["ticker"],
            "company_name": stored_row[1] if stored_row else None,
            "sector": stored_row[2] if stored_row else None,
            "industry": stored_row[3] if stored_row else None,
            "quote_type": stored_row[4] if stored_row else None,
        }
        row.update({k: v for k, v in override.items() if k != "ticker"})
        return row

    def _apply_metadata_overrides(
        self, normalized: List[Dict[str, Any]]
    ) -> Tuple[int, List[Dict[str, Any]]]:
        """Merge + upsert validated overrides; return (applied, report).

        The merge is a read-then-write: existing rows are fetched for
        the override tickers, absent fields are filled from the stored
        values, and the merged rows go through
        ``db.upsert_company_metadata`` (the single write path — a blind
        upsert would NULL out untouched fields because the ON CONFLICT
        clause replaces all four columns).  The per-override report
        records which fields were applied and whether the row was
        created, so corrections are auditable.
        """
        tickers = [o["ticker"] for o in normalized]
        stored = {
            r[0]: r
            for r in self.db.conn.execute(
                "SELECT ticker, company_name, sector, industry, "
                "quote_type FROM company_metadata "
                "WHERE ticker IN (SELECT unnest(?))",
                [tickers],
            ).fetchall()
        }
        merged = [
            self._merge_metadata_override(o, stored.get(o["ticker"]))
            for o in normalized
        ]
        applied = self.db.upsert_company_metadata(merged)
        report = [
            {
                "ticker": o["ticker"],
                "applied_fields": [k for k in o if k != "ticker"],
                "created": o["ticker"] not in stored,
            }
            for o in normalized
        ]
        return applied, report

    def run_metadata_load(
        self,
        tickers: Optional[List[str]] = None,
        refresh: bool = False,
        delay_s: float = 0.5,
        max_consecutive_failures: int = 15,
        max_tickers: Optional[int] = None,
        batch_size: int = 50,
        overrides: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """Fetch ``yf.Ticker(t).info`` metadata into ``company_metadata``.

        Port of the legacy ``load_company_metadata_to_duckdb3b.py`` job
        (longName/sector/industry/quoteType, skip-existing, ON CONFLICT
        upsert) with deliberate rate-limit hardening:

        * ``tickers=None`` uses all configured tickers (minus
          ``exclude_tickers``); an explicit list is normalized
          (strip/upper/dedup) and taken as-is.
        * ``refresh=False`` fetches only tickers missing from
          ``company_metadata`` (skip-existing); ``refresh=True``
          re-fetches every given ticker.
        * ``delay_s`` paces the per-ticker ``.info`` calls — yfinance has
          no batch API for them, and unpaced bursts trigger HTTP 429.
        * A per-ticker EXCEPTION adds the ticker to ``tickers_failed``,
          increments a consecutive-failure counter, and writes NO row —
          a deliberate deviation from the legacy job (which wrote a
          NULL-field row on exception): a throttled ticker must stay
          "missing" so the next run retries it instead of being
          poisoned by a permanent NULL placeholder.  A *successful* but
          empty/sparse ``.info`` response DOES write a NULL-field row,
          matching the legacy behaviour for genuinely unknown symbols.
        * ``max_consecutive_failures`` is a circuit breaker: that many
          fetch exceptions in a row aborts the run early with
          ``aborted=True`` and ``reason`` set; unprocessed tickers stay
          missing for a later retry.
        * Rows are upserted in ``batch_size`` chunks DURING the loop plus
          a final flush on abort/return, so an aborted run keeps the
          progress it already flushed.
        * ``overrides`` (SEC-T3) is an optional list of per-ticker
          correction dicts — the manual correction channel for
          ``company_metadata`` (the client-DML surface cannot write to
          that table).  Each dict requires ``ticker`` plus any subset of
          ``company_name``/``sector``/``industry``/``quote_type``
          strings; provided non-null fields overwrite the stored row
          while fields ABSENT from the dict preserve the stored value
          (merge semantics — a partial override cannot NULL out
          untouched fields; a new ticker stores NULL for unprovided
          fields).  Validation happens up front, so a malformed payload
          raises ``ValueError`` before any write.  Ordering when both a
          fetch and overrides run in one call: the yfinance fetch runs
          FIRST, then overrides are applied — so a correction always
          wins over a same-call fetch (e.g. ``refresh=True`` on the
          same ticker).  Overrides work with or without ``tickers``/
          ``refresh`` — pass ``tickers=[]`` for an overrides-only call
          with zero fetches.

        Returns ``{tickers_fetched, tickers_skipped, tickers_failed,
        rows_upserted, aborted, reason, errors, duration_ms,
        overrides_applied, overrides_report}``.
        """
        if (
            isinstance(delay_s, bool)
            or not isinstance(delay_s, (int, float))
            or delay_s < 0
        ):
            raise ValueError(
                f"delay_s must be a number >= 0, got {delay_s!r}"
            )
        if (
            not isinstance(max_consecutive_failures, int)
            or isinstance(max_consecutive_failures, bool)
            or max_consecutive_failures < 1
        ):
            raise ValueError(
                "max_consecutive_failures must be an integer >= 1, "
                f"got {max_consecutive_failures!r}"
            )
        if max_tickers is not None and (
            not isinstance(max_tickers, int)
            or isinstance(max_tickers, bool)
            or max_tickers < 1
        ):
            raise ValueError(
                f"max_tickers must be an integer >= 1, got {max_tickers!r}"
            )
        if (
            not isinstance(batch_size, int)
            or isinstance(batch_size, bool)
            or batch_size < 1
        ):
            raise ValueError(
                f"batch_size must be an integer >= 1, got {batch_size!r}"
            )
        if not isinstance(refresh, bool):
            raise ValueError(f"refresh must be a bool, got {refresh!r}")
        # Overrides are validated BEFORE any fetch or write — a malformed
        # payload must fail without touching company_metadata.
        normalized_overrides = self._normalize_metadata_overrides(overrides)

        start_time = datetime.now()

        if tickers is None:
            excluded = set(self.cfg.ingestion.exclude_tickers)
            requested = [
                t for t in self.cfg.ingestion.tickers if t not in excluded
            ]
        else:
            requested = list(
                dict.fromkeys(
                    t.strip().upper()
                    for t in tickers
                    if isinstance(t, str) and t.strip()
                )
            )

        if refresh:
            to_fetch = list(requested)
            skipped = 0
        else:
            to_fetch = self.db.get_missing_metadata_tickers(requested)
            skipped = len(requested) - len(to_fetch)
        if max_tickers is not None and len(to_fetch) > max_tickers:
            skipped += len(to_fetch) - max_tickers
            to_fetch = to_fetch[:max_tickers]

        fetched = 0
        failed: List[str] = []
        errors: Dict[str, str] = {}
        rows: List[Dict[str, Any]] = []
        rows_upserted = 0
        consecutive_failures = 0
        aborted = False
        reason: Optional[str] = None

        def _flush() -> None:
            """Upsert the buffered rows; a flush failure is logged and
            recorded in ``errors`` but never propagated."""
            nonlocal rows_upserted, rows
            if not rows:
                return
            try:
                rows_upserted += self.db.upsert_company_metadata(rows)
            except Exception as exc:
                logger.error(
                    "company_metadata upsert flush failed (%d rows): %s",
                    len(rows),
                    exc,
                )
                errors["__upsert__"] = str(exc)
            rows = []

        # justified: yfinance exposes no batch API for Ticker.info — each
        # ticker needs its own .info call, paced by delay_s to stay under
        # the rate limit.  The per-ticker try/except and the incremental
        # batch flush cannot be expressed in SQL or a vectorized op.
        for i, t in enumerate(to_fetch):
            if i:
                time.sleep(delay_s)
            try:
                info = yf.Ticker(t).info
            except Exception as e:
                consecutive_failures += 1
                failed.append(t)
                errors[t] = str(e)
                logger.warning(
                    "Metadata fetch failed for '%s' (%d consecutive): %s",
                    t,
                    consecutive_failures,
                    e,
                )
                if consecutive_failures >= max_consecutive_failures:
                    aborted = True
                    skipped += len(to_fetch) - (i + 1)
                    reason = (
                        f"circuit breaker: {consecutive_failures} "
                        f"consecutive fetch failures "
                        f"(>= {max_consecutive_failures}) — aborting "
                        f"early; {skipped} ticker(s) left unfetched and "
                        f"still missing for the next run"
                    )
                    logger.error("RUNTIME EVENT: Metadata load aborted — %s", reason)
                    break
                continue
            consecutive_failures = 0
            if not isinstance(info, dict):
                # A successful call returning a non-dict (e.g. None) is
                # treated like an empty dict — write the NULL-field row.
                info = {}
            rows.append(
                {
                    "ticker": t,
                    "company_name": info.get("longName"),
                    "sector": info.get("sector"),
                    "industry": info.get("industry"),
                    "quote_type": info.get("quoteType"),
                }
            )
            fetched += 1
            if len(rows) >= batch_size:
                _flush()

        _flush()  # final flush — keeps flushed progress on abort/return

        # SEC-T3: apply overrides AFTER the fetch loop's final flush so
        # a correction always wins over a same-call fetch (merge reads
        # the just-refreshed rows, then overwrite).  A merge/upsert
        # failure is logged and recorded in ``errors`` but never
        # propagates — same contract as ``_flush``.
        overrides_applied = 0
        overrides_report: List[Dict[str, Any]] = []
        if normalized_overrides:
            try:
                overrides_applied, overrides_report = (
                    self._apply_metadata_overrides(normalized_overrides)
                )
            except Exception as exc:
                logger.error(
                    "company_metadata overrides apply failed "
                    "(%d override(s)): %s",
                    len(normalized_overrides),
                    exc,
                )
                errors["__overrides__"] = str(exc)

        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        result = {
            "tickers_fetched": fetched,
            "tickers_skipped": skipped,
            "tickers_failed": failed,
            "rows_upserted": rows_upserted,
            "aborted": aborted,
            "reason": reason,
            "errors": errors,
            "duration_ms": duration_ms,
            "overrides_applied": overrides_applied,
            "overrides_report": overrides_report,
        }
        if failed:
            logger.warning(
                "Metadata load finished with %d fetch failure(s): %s",
                len(failed),
                failed,
            )
        logger.info(
            "Metadata load complete. Fetched: %s, skipped: %s, "
            "rows upserted: %s, overrides applied: %s, aborted: %s.",
            fetched,
            skipped,
            rows_upserted,
            overrides_applied,
            aborted,
        )
        return result

    def _metadata_bridge(self) -> Dict[str, Any]:
        """Best-effort company-metadata top-up after a load; NEVER raises.

        Mirrors the report-only ``health`` embedding in the Flight
        layer: the outcome is merged under a ``'metadata'`` key on
        SUCCESS/PARTIAL load results, and any failure is logged as a
        warning and surfaced as ``{'status': 'FAILED', 'error': ...}``
        so it can never break a completed load.
        """
        try:
            excluded = set(self.cfg.ingestion.exclude_tickers)
            configured = [
                t for t in self.cfg.ingestion.tickers if t not in excluded
            ]
            missing = self.db.get_missing_metadata_tickers(configured)
            if not missing:
                return {
                    "status": "NOT_NEEDED",
                    "tickers_fetched": 0,
                    "tickers_skipped": len(configured),
                    "tickers_failed": [],
                    "rows_upserted": 0,
                    "aborted": False,
                    "reason": None,
                    "errors": {},
                    "duration_ms": 0,
                    "overrides_applied": 0,
                    "overrides_report": [],
                }
            logger.info(
                "Metadata bridge: %d configured ticker(s) missing from "
                "company_metadata — fetching them now.",
                len(missing),
            )
            meta = self.run_metadata_load(tickers=missing)
            status = (
                "ABORTED"
                if meta.get("aborted")
                else "PARTIAL" if meta.get("tickers_failed") else "SUCCESS"
            )
            return {"status": status, **meta}
        except Exception as exc:
            logger.warning(
                "Metadata bridge failed (load unaffected): %s", exc
            )
            return {"status": "FAILED", "error": str(exc)}

    # ------------------------------------------------------------------
    # Weekly layer refresh (F12-T1)
    # ------------------------------------------------------------------

    def run_weekly_refresh(self) -> Dict[str, Any]:
        """Rebuild the weekly layer: ``weekly_close`` then
        ``weekly_indicators``.

        Order matters — the indicators query reads ``weekly_close``.
        Both rebuilds are pure SQL on the DB side (no yfinance calls);
        returns ``{status, weekly_close_rows, weekly_indicators_rows,
        duration_ms}``.
        """
        start_time = datetime.now()
        logger.info("Refreshing weekly_close + weekly_indicators...")
        close_rows = self.db.rebuild_weekly_close()
        indicator_rows = self.db.rebuild_weekly_indicators()
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        logger.info(
            "Weekly refresh complete. weekly_close rows: %d, "
            "weekly_indicators rows: %d.",
            close_rows,
            indicator_rows,
        )
        return {
            "status": "SUCCESS",
            "weekly_close_rows": close_rows,
            "weekly_indicators_rows": indicator_rows,
            "duration_ms": duration_ms,
        }

    def _weekly_bridge(self) -> Dict[str, Any]:
        """Best-effort weekly-layer refresh after a load; NEVER raises.

        Mirrors ``_metadata_bridge``: the outcome is merged under a
        ``'weekly'`` key on the load result, and any failure is logged
        as a warning and surfaced as ``{'status': 'FAILED', 'error':
        ...}`` so it can never break a completed load.
        """
        try:
            return self.run_weekly_refresh()
        except Exception as exc:
            logger.warning(
                "Weekly bridge failed (load unaffected): %s", exc
            )
            return {"status": "FAILED", "error": str(exc)}

    # ------------------------------------------------------------------
    # CNN Fear & Greed index (F16-T1)
    # ------------------------------------------------------------------

    @staticmethod
    def _fear_greed_series_points(block: Any) -> Dict[Any, Tuple[Any, Any]]:
        """Parse one CNN series block into ``{date: (value, rating)}``.

        ``block`` is a payload sub-object with a ``data`` array of
        ``{x: ms-epoch float, y: value, rating: str}`` points.  ``x`` is
        converted to a UTC date; a missing/None ``y`` or ``rating``
        lands as NULL.  Points without a usable ``x`` are skipped, and
        duplicate dates keep the LAST point in the array.  A
        non-dict block or a missing/empty ``data`` array yields ``{}``
        (the caller treats that as a missing series — never a KeyError).
        """
        if not isinstance(block, dict):
            return {}
        points = block.get("data")
        if not isinstance(points, (list, tuple)):
            return {}
        return {
            datetime.fromtimestamp(float(p["x"]) / 1000, tz=timezone.utc).date(): (
                p.get("y"),
                p.get("rating"),
            )
            for p in points
            if isinstance(p, dict)
            and p.get("x") is not None
            and isinstance(p.get("x"), (int, float))
        }

    def run_fear_greed_load(
        self, start_date: Optional[str] = None
    ) -> Dict[str, Any]:
        """Fetch the CNN Fear & Greed payload and upsert it into
        ``fear_greed_history``.

        One GET on ``graphdata/{start_date}`` returns the composite
        ``fear_and_greed_historical`` series plus the 9 sub-indicator
        blocks listed in ``FEAR_GREED_SERIES``.  Each series' ``data``
        array is parsed to per-date rows (``x`` ms-epoch -> UTC date,
        ``y`` -> DOUBLE column, ``rating`` -> VARCHAR column; missing
        ``y``/``rating`` -> NULL; duplicate dates keep the last point),
        then outer-merged on ``date`` into one wide frame carrying every
        expected column (absent -> None) so the upsert uses a static
        column list.

        Retention/fallback: CNN retains only ~6 years of history, so a
        ``start_date`` behind the advancing retention floor makes the
        dated endpoint return HTTP 500.  On ``requests.HTTPError`` ONLY
        the request is retried once against the undated
        ``FEAR_GREED_BASE_URL`` (rolling ~1 year of data); timeouts and
        connection errors do NOT fall back (same host — pointless), and
        a failing retry propagates (the bridge converts it to FAILED).

        Resilience contract: a missing/empty
        ``fear_and_greed_historical`` raises ``ValueError`` naming the
        key; a missing/empty sub-indicator block contributes NULL
        columns, a ``missing_series`` entry and a warning — never a
        KeyError; unknown payload keys are ignored; a fully-empty frame
        raises.

        Returns ``{status: 'SUCCESS', rows_upserted, latest_date,
        fg_score, fg_rating, alert_triggered, alert_detail,
        missing_series, fetched_url, start_date_fallback,
        duration_ms}`` where ``alert_triggered`` is set
        when the latest ``fg_score`` is <= ``fear_greed.alert_threshold``
        and ``alert_detail`` lists sub-indicator keys whose current
        rating is ``'extreme fear'``.  ``fetched_url`` is the URL that
        actually returned data and ``start_date_fallback`` is True when
        the undated-URL fallback was used.
        """
        if start_date is None:
            start_date = self.cfg.fear_greed.start_date
        if not isinstance(start_date, str) or not _FEAR_GREED_DATE_RE.fullmatch(
            start_date
        ):
            raise ValueError(
                f"start_date must be a YYYY-MM-DD string, got {start_date!r}"
            )
        try:
            datetime.strptime(start_date, "%Y-%m-%d")
        except ValueError:
            raise ValueError(
                f"start_date is not a real calendar date, got {start_date!r}"
            )

        start_time = datetime.now()
        url = FEAR_GREED_URL_TEMPLATE.format(start_date)
        logger.info("Fetching CNN Fear & Greed data from %s ...", url)
        try:
            resp = requests.get(
                url,
                headers=FEAR_GREED_HEADERS,
                timeout=self.cfg.fear_greed.timeout_seconds,
            )
            resp.raise_for_status()
            start_date_fallback = False
        except requests.HTTPError:
            # CNN only retains ~6 years of history — a start_date behind
            # the advancing retention floor makes the dated endpoint
            # return HTTP 500.  Retry ONCE with the undated base URL
            # (rolling ~1 year).  Only HTTPError falls back: timeouts
            # and connection errors would hit the same host again
            # pointlessly.  A failing retry propagates (the bridge
            # converts it to FAILED).
            logger.warning(
                "CNN Fear & Greed request to %s returned an HTTP error "
                "(start_date %s is likely beyond CNN's ~6-year "
                "retention floor) — retrying with the undated URL %s "
                "...",
                url,
                start_date,
                FEAR_GREED_BASE_URL,
            )
            url = FEAR_GREED_BASE_URL
            resp = requests.get(
                url,
                headers=FEAR_GREED_HEADERS,
                timeout=self.cfg.fear_greed.timeout_seconds,
            )
            resp.raise_for_status()
            start_date_fallback = True
        payload = resp.json()

        # Required core series — missing/empty is a hard error naming
        # the key (never a bare KeyError).
        hist_block = payload.get("fear_and_greed_historical")
        hist_rows = self._fear_greed_series_points(hist_block)
        if not hist_rows:
            raise ValueError(
                "CNN payload missing required key "
                "'fear_and_greed_historical' (or its 'data' array is "
                "empty/unusable)"
            )

        # Optional sub-indicator series — each maps CNN key -> column
        # prefix verbatim; missing/empty blocks contribute NULL columns.
        series_rows = {
            key: self._fear_greed_series_points(payload.get(key))
            for key in FEAR_GREED_SERIES
        }
        missing_series = [k for k, rows in series_rows.items() if not rows]
        if missing_series:
            logger.warning(
                "Fear & Greed payload missing/empty sub-indicator "
                "series %s — their columns will be NULL.",
                missing_series,
            )

        # Union of every series' date keys (dicts iterate their keys).
        all_dates = set(hist_rows).union(*series_rows.values())
        # Outer-merge on date into the static wide column list.
        wide_rows = [
            {
                "date": d,
                "fg_score": hist_rows.get(d, (None, None))[0],
                "fg_rating": hist_rows.get(d, (None, None))[1],
                **{
                    prefix: series_rows[key].get(d, (None, None))[0]
                    for key, prefix in FEAR_GREED_SERIES.items()
                },
                **{
                    f"{prefix}_rating": series_rows[key].get(d, (None, None))[1]
                    for key, prefix in FEAR_GREED_SERIES.items()
                },
            }
            for d in sorted(all_dates)
        ]
        wide = pd.DataFrame(wide_rows, columns=FEAR_GREED_COLUMNS)
        if wide.empty:
            raise ValueError(
                "CNN Fear & Greed payload produced no rows to upsert"
            )

        rows_upserted = self.db.upsert_fear_greed(wide)
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)

        latest_date = max(all_dates)
        fg_score = hist_rows.get(latest_date, (None, None))[0]
        fg_rating = hist_rows.get(latest_date, (None, None))[1]
        fg_score_f = (
            float(fg_score)
            if fg_score is not None and not pd.isna(fg_score)
            else None
        )
        threshold = self.cfg.fear_greed.alert_threshold
        alert_triggered = fg_score_f is not None and fg_score_f <= threshold
        alert_detail = [
            key
            for key in FEAR_GREED_SERIES
            if isinstance(payload.get(key), dict)
            and str(payload[key].get("rating") or "").lower()
            == "extreme fear"
        ]

        result = {
            "status": "SUCCESS",
            "rows_upserted": rows_upserted,
            "latest_date": latest_date.isoformat(),
            "fg_score": fg_score_f,
            "fg_rating": fg_rating,
            "alert_triggered": bool(alert_triggered),
            "alert_detail": alert_detail,
            "missing_series": missing_series,
            "fetched_url": url,
            "start_date_fallback": start_date_fallback,
            "duration_ms": duration_ms,
        }
        if alert_triggered:
            logger.warning(
                "RUNTIME EVENT: Fear & Greed alert — latest fg_score "
                "%s <= threshold %s (rating: %s; extreme-fear "
                "sub-indicators: %s)",
                fg_score_f,
                threshold,
                fg_rating,
                alert_detail,
            )
        logger.info(
            "Fear & Greed load complete. Rows upserted: %s, latest "
            "date: %s, fg_score: %s (%s).",
            rows_upserted,
            latest_date,
            fg_score_f,
            fg_rating,
        )
        return result

    def _fear_greed_bridge(self) -> Dict[str, Any]:
        """Best-effort Fear & Greed refresh after a load; NEVER raises.

        Mirrors ``_weekly_bridge`` exactly: honors
        ``cfg.fear_greed.enabled`` (``{'status': 'DISABLED'}`` when
        off), otherwise runs ``run_fear_greed_load`` and surfaces any
        failure as ``{'status': 'FAILED', 'error': ...}`` so it can
        never break a completed load.
        """
        try:
            if not getattr(
                getattr(self.cfg, "fear_greed", None), "enabled", False
            ):
                return {"status": "DISABLED"}
            return self.run_fear_greed_load()
        except Exception as exc:
            logger.warning(
                "Fear & Greed bridge failed (load unaffected): %s", exc
            )
            return {"status": "FAILED", "error": str(exc)}

    # ------------------------------------------------------------------
    # Signal pipeline (F24-T1) — server-side port of
    # quant_strategy_building/signal_input/orchestrator.py __main__
    # ------------------------------------------------------------------

    def run_signal_pipeline(
        self,
        folder_path: Optional[str] = None,
        backdate_days: Optional[int] = None,
        max_holding_days: Optional[int] = None,
        overwrite_existing: Optional[bool] = None,
        min_reward_risk: Optional[float] = None,
        min_target_pct: Optional[float] = None,
        exclude_clusters: Optional[List[int]] = None,
        exclude_models: Optional[List[str]] = None,
        filter_below_open: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """Run the six-step signal pipeline on the service DB.

        Reproduces the legacy ``orchestrator.py`` ``__main__`` step
        order exactly:

        1. ingest — ``*_active_signal_combined.xlsx`` files modified
           within ``backdate_days`` -> ``signal_records`` (vectorized
           ``INSERT ... ON CONFLICT DO NOTHING``)
        2. dedupe — keep the latest ``Signal_File`` per
           ``(Trading_Date, Model, Ticker)``
        3. repair — fill NULL ``Close`` from ``daily_close`` (in-DB
           join; the legacy ``ATTACH``ed price DB is obsolete)
        4. prune — delete signals failing the OR-filter
           (``Reward_Risk_Ratio``/``Target_Percent`` thresholds,
           cluster/model exclusions, optional below-open); the pruned
           rows are RETURNED (the legacy log file is obsolete)
        5. performance — forward-return outcomes into
           ``signal_performance`` (verbatim CTE; ``overwrite_existing``
           switches ``INSERT OR REPLACE`` vs ``ON CONFLICT DO NOTHING``)
        6. terminal safeguard — silent repair sweep + NULL-close
           verification, then ``cluster_metadata`` INSERT OR REPLACE

        Every ``None`` parameter falls back to the ``signals:`` config
        block; the ``signal_pipeline`` Flight action validates payload
        overrides before calling this.  Each step's writes run on
        ``self.db.conn`` under ``self.db._write_lock`` — the same
        per-statement interleaving model as ``run_daily_load``.

        Returns ``{status, steps: {ingest, dedupe, repair, prune,
        performance, repair_sweep, null_verification,
        cluster_metadata}, duration_ms}``.
        """
        sig = self.cfg.signals
        folder_path = folder_path if folder_path is not None else sig.folder_path
        backdate_days = (
            backdate_days if backdate_days is not None else sig.backdate_days
        )
        max_holding_days = (
            max_holding_days
            if max_holding_days is not None
            else sig.max_holding_days
        )
        overwrite_existing = (
            overwrite_existing
            if overwrite_existing is not None
            else sig.overwrite_existing
        )
        min_reward_risk = (
            min_reward_risk
            if min_reward_risk is not None
            else sig.min_reward_risk
        )
        min_target_pct = (
            min_target_pct
            if min_target_pct is not None
            else sig.min_target_pct
        )
        exclude_clusters = (
            exclude_clusters
            if exclude_clusters is not None
            else sig.exclude_clusters
        )
        exclude_models = (
            exclude_models
            if exclude_models is not None
            else sig.exclude_models
        )
        filter_below_open = (
            filter_below_open
            if filter_below_open is not None
            else sig.filter_below_open
        )

        start_time = datetime.now()
        con = self.db.conn
        steps: Dict[str, Any] = {}
        logger.info(
            "Starting signal pipeline (folder=%s, backdate=%dd, "
            "max_holding=%dd, overwrite=%s, min_rr=%s, "
            "min_target_pct=%s, exclude_clusters=%s, "
            "exclude_models=%s, filter_below_open=%s)...",
            folder_path,
            backdate_days,
            max_holding_days,
            overwrite_existing,
            min_reward_risk,
            min_target_pct,
            exclude_clusters,
            exclude_models,
            filter_below_open,
        )

        # 1. Ingest files into DB
        with self.db._write_lock:
            steps["ingest"] = signal_pipeline.ingest_signal_folder(
                con, folder_path, backdate_days
            )

        # 2. Deduplicate older runs
        with self.db._write_lock:
            steps["dedupe"] = signal_pipeline.delete_older_signal_runs(con)

        # 3. Primary data repair
        with self.db._write_lock:
            steps["repair"] = signal_pipeline.repair_missing_close_prices(
                con
            )

        # 4. Filter generation metrics
        with self.db._write_lock:
            steps["prune"] = signal_pipeline.prune_unwanted_signals(
                con,
                min_reward_risk=min_reward_risk,
                min_target_pct=min_target_pct,
                exclude_clusters=exclude_clusters,
                exclude_models=exclude_models,
                filter_below_open=filter_below_open,
            )

        # 5. Process performance metrics
        with self.db._write_lock:
            steps["performance"] = (
                signal_pipeline.calculate_and_save_performance(
                    con,
                    max_holding_days=max_holding_days,
                    overwrite_existing=overwrite_existing,
                )
            )

        # Terminal integrity safeguard: silent repair sweep + NULL
        # verification (legacy __main__ lines 189-195)
        with self.db._write_lock:
            steps["repair_sweep"] = (
                signal_pipeline.repair_missing_close_prices(
                    con, silent=True
                )
            )
            final_nulls = con.execute(
                "SELECT COUNT(1) FROM signal_records WHERE Close IS NULL"
            ).fetchone()[0]
            steps["null_verification"] = {
                "null_close_remaining": int(final_nulls)
            }
            logger.info(
                "Verification Check: remaining un-repairable NULL "
                "Close entries: %d",
                final_nulls,
            )

        # 6. Finalize meta properties
        with self.db._write_lock:
            steps["cluster_metadata"] = (
                signal_pipeline.setup_cluster_metadata(con)
            )

        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        logger.info("Signal pipeline execution completed successfully.")
        return {
            "status": "SUCCESS",
            "steps": steps,
            "duration_ms": duration_ms,
        }

    # ------------------------------------------------------------------
    # Signal analysis (F25-T1) — server-side port of
    # quant_strategy_building/signal_input2 (orchestrator_generic +
    # process_mutli_sets_signals multi-set loop)
    # ------------------------------------------------------------------

    def run_signal_analysis(
        self,
        set_name: Optional[str] = None,
        backdate_days: Optional[int] = None,
        max_holding_days: Optional[int] = None,
        mc_iterations: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Run the five-phase signal-analysis job over the configured
        engine sets.

        Reproduces the ``process_mutli_sets_signals.py`` loop: every
        configured ``signal_analysis.sets`` entry (or the single set
        named by ``set_name``) gets its own schema ``signals_<name>``
        (``CREATE SCHEMA IF NOT EXISTS`` — runtime, SCHEMA_VERSION stays
        7) and runs the phases in the legacy order:

        1. ingest — ``*_active_signal_combined.xlsx`` (all sheets) ->
           ``{schema}.aly_signal_records_raw`` (DROP+recreate per run,
           ``Trading_Date >= today - backdate_days`` cutoff)
        2. dedupe — keep the latest ``Signal_File`` per
           ``(Trading_Date, Model, Ticker)``
        3. repair — fill NULL/NaN ``Close`` from ``daily_close`` (in-DB
           join; no ATTACH)
        4. views — the four ``aly_v_signals_*`` views (verbatim
           predicates)
        5. backtest — ``aly_signal_performance`` (15 cols,
           stop-wins-same-day tie-break, Execution_Status 1/2/3) +
           integrity check + ``RPT_signal_performance.xlsx`` + per-view
           scorecard + ``SignalEdgeValidator`` metrics
        6. phase5 — matrix analytics -> 3 heatmap PNGs +
           ``RPT_phase5_analytics_matrix.xlsx`` under
           ``reports.report_dir/signal_analysis/<name>/``

        ``None`` parameters fall back to the ``signal_analysis:`` config
        block; the ``signal_analysis`` Flight action validates payload
        overrides before calling this.  Each step's writes run on
        ``self.db.conn`` under ``self.db._write_lock``.

        Returns ``{status, sets: {<name>: {status, schema, folder,
        report_dir, steps: {...}, duration_ms}}, duration_ms}`` — status
        is SUCCESS when every set succeeded, PARTIAL when some failed,
        FAILED when all failed.  An unknown ``set_name`` raises
        ``ValueError``; an empty ``sets`` config returns SUCCESS with an
        empty ``sets`` map.
        """
        acfg = self.cfg.signal_analysis
        backdate_days = (
            backdate_days if backdate_days is not None else acfg.backdate_days
        )
        max_holding_days = (
            max_holding_days
            if max_holding_days is not None
            else acfg.max_holding_days
        )
        mc_iterations = (
            mc_iterations
            if mc_iterations is not None
            else acfg.mc_iterations
        )
        benchmark = acfg.benchmark

        configured = {s.name: s for s in acfg.sets}
        if set_name is not None:
            if set_name not in configured:
                raise ValueError(
                    f"unknown signal_analysis set {set_name!r}; "
                    f"configured: {sorted(configured)}"
                )
            target_sets = [configured[set_name]]
        else:
            target_sets = list(acfg.sets)

        start_time = datetime.now()
        con = self.db.conn
        sets_result: Dict[str, Any] = {}
        logger.info(
            "Starting signal analysis (sets=%s, backdate=%dd, "
            "max_holding=%dd, mc_iterations=%d, benchmark=%s)...",
            [s.name for s in target_sets],
            backdate_days,
            max_holding_days,
            mc_iterations,
            benchmark,
        )

        # justified: per-set pipeline iteration — each configured engine
        # set runs the full 5-phase job against its own schema (mirrors
        # the legacy process_mutli_sets_signals folders_and_dbs loop);
        # sets are config entries, not data rows.
        for sig_set in target_sets:
            schema = signal_analysis.schema_for_set(sig_set.name)
            report_dir = os.path.join(
                self.cfg.reports.report_dir, "signal_analysis", sig_set.name
            )
            os.makedirs(report_dir, exist_ok=True)
            set_start = datetime.now()
            steps: Dict[str, Any] = {}
            set_status = "SUCCESS"
            try:
                with self.db._write_lock:
                    con.execute(
                        f"CREATE SCHEMA IF NOT EXISTS {schema}"
                    )
                    steps["ingest"] = signal_analysis.ingest_signals(
                        con, schema, sig_set.folder, backdate_days
                    )
                    raw_exists = signal_analysis._table_exists(
                        con, schema, signal_analysis.RAW_TABLE
                    )

                if not raw_exists:
                    # Graceful: nothing ingested and no leftover raw
                    # table — the downstream phases have no inputs.
                    # justified: emitting the same SKIPPED marker per
                    # remaining step is reporting, not data work.
                    for step_name in (
                        "dedupe",
                        "repair",
                        "views",
                        "performance",
                        "phase5",
                    ):
                        steps[step_name] = {
                            "status": "SKIPPED",
                            "reason": (
                                "aly_signal_records_raw absent "
                                "(no signal files ingested)"
                            ),
                        }
                else:
                    with self.db._write_lock:
                        steps["dedupe"] = (
                            signal_analysis.deduplicate_signals(
                                con, schema
                            )
                        )
                    with self.db._write_lock:
                        steps["repair"] = (
                            signal_analysis.repair_missing_close_prices(
                                con, schema
                            )
                        )
                    with self.db._write_lock:
                        steps["views"] = signal_analysis.build_views(
                            con, schema
                        )
                    with self.db._write_lock:
                        steps["performance"] = (
                            signal_analysis.run_backtest(
                                con,
                                schema,
                                report_dir,
                                max_holding_days=max_holding_days,
                                benchmark=benchmark,
                                mc_iterations=mc_iterations,
                            )
                        )
                    with self.db._write_lock:
                        steps["phase5"] = signal_analysis.run_phase5(
                            con, schema, report_dir
                        )
            except Exception as exc:
                logger.error(
                    "signal_analysis set %s failed: %s", sig_set.name, exc
                )
                steps["error"] = str(exc)
                set_status = "FAILED"

            sets_result[sig_set.name] = {
                "status": set_status,
                "schema": schema,
                "folder": sig_set.folder,
                "report_dir": report_dir,
                "steps": steps,
                "duration_ms": int(
                    (datetime.now() - set_start).total_seconds() * 1000
                ),
            }

        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        succeeded = sum(
            1 for s in sets_result.values() if s["status"] == "SUCCESS"
        )
        if not sets_result or succeeded == len(sets_result):
            status = "SUCCESS"
        elif succeeded == 0:
            status = "FAILED"
        else:
            status = "PARTIAL"
        logger.info(
            "Signal analysis completed (%s): %d/%d sets succeeded.",
            status,
            succeeded,
            len(sets_result),
        )
        return {
            "status": status,
            "sets": sets_result,
            "duration_ms": duration_ms,
        }

    # ------------------------------------------------------------------
    # Client-issued DML (F26-T1) — the ``pivot`` namespace contract.
    #
    # Orchestration for the ``dml_admin`` Flight action and the
    # generalized ``do_put`` descriptor path.  Enforcement boundary:
    # regexes + closed enums + the server-owned ``table_registry`` —
    # no SQL parsing of write targets (see src/client_dml.py).
    #
    # Lock discipline mirrors every other writer: a dedicated cursor on
    # ``self.db.conn`` executes the write unit under
    # ``self.db._write_lock``; the heavy ``create_as`` source scan runs
    # on a read cursor OUTSIDE the lock (snapshot -> Arrow -> locked
    # materialization, the PM's accepted lock mitigation).  Timeout is
    # the worker+Timer+interrupt() pattern from the study (§4.4) —
    # F26-T2/F3: the snapshot read goes through the SAME timeout worker
    # (it previously ran synchronously outside any deadline, so a
    # pathological source could pin the action thread unboundedly).
    # ------------------------------------------------------------------

    def _execute_dml_with_timeout(self, fn, timeout_seconds: float):
        """Run ``fn(cursor)`` on a worker thread with a hard deadline.

        The cursor is a duplicate of ``self.db.conn`` — its own
        transaction context and its own ``interrupt()`` target, so the
        timer can cancel exactly this op's statements without touching
        the shared connection.  The lock is acquired *inside* ``fn``
        (timeout wraps the whole unit; ``_write_lock`` wraps only the
        statements — same structure as ``_execute_query_with_timeout``).
        """
        result_queue: "queue.Queue[Any]" = queue.Queue()
        cur = self.db.conn.cursor()
        done = threading.Event()

        def _worker():
            try:
                result_queue.put(fn(cur))
            except Exception as exc:
                result_queue.put(exc)
            finally:
                done.set()

        def _timeout():
            if not done.is_set():
                try:
                    cur.interrupt()
                except Exception:
                    pass
            result_queue.put(
                TimeoutError(
                    f"DML op exceeded {timeout_seconds} second timeout"
                )
            )

        worker = threading.Thread(target=_worker, daemon=True)
        timer = threading.Timer(timeout_seconds, _timeout)
        worker.start()
        timer.start()
        try:
            result = result_queue.get()
        finally:
            timer.cancel()
            try:
                cur.close()
            except Exception:
                pass
        if isinstance(result, Exception):
            raise result
        return result

    @staticmethod
    def _dml_audit_target(payload: Dict[str, Any]) -> str:
        """Best-effort audit ``target`` when validation hasn't run yet."""
        table = payload.get("table")
        namespace = payload.get("namespace")
        if isinstance(table, str) and table:
            if isinstance(namespace, str) and namespace and "." not in table:
                return f"{namespace}.{table}"
            return table
        if isinstance(namespace, str) and namespace:
            return namespace
        return "-"

    def _check_stream_columns(
        self, cur, namespace: str, table: str, arrow_table
    ) -> None:
        """Reject a streamed frame whose columns don't all exist on the
        registered target (``schema mismatch`` per the contract).

        Case-insensitive subset check — DuckDB identifiers are
        case-insensitive and ``BY NAME`` fills absent columns with
        defaults/NULL (constraints still fail loudly on violations).
        """
        target_cols = {
            c.lower()
            for c in client_dml.catalog_column_names(cur, namespace, table)
        }
        if not target_cols:
            raise client_dml.DmlRejected(
                f"{namespace}.{table} has no catalog columns — "
                f"is it a real table?"
            )
        extra = [
            name
            for name in arrow_table.schema.names
            if name.lower() not in target_cols
        ]
        if extra:
            raise client_dml.DmlRejected(
                f"schema mismatch: streamed columns {extra} not present "
                f"in {namespace}.{table} {sorted(target_cols)}"
            )
        if not arrow_table.schema.names:
            raise client_dml.DmlRejected(
                "streamed frame has no columns"
            )

    # ----- dml_admin op handlers --------------------------------------

    def _dml_op_register_namespace(
        self, payload: Dict[str, Any], settings: Dict[str, Any], client_tag
    ) -> Dict[str, Any]:
        namespace = client_dml.validate_name(
            payload.get("namespace"), "namespace"
        )
        if namespace not in settings["allowed_namespaces"]:
            raise client_dml.DmlRejected(
                f"namespace {namespace!r} is not in client_dml."
                f"allowed_namespaces {settings['allowed_namespaces']}"
            )

        def unit(cur):
            with self.db._write_lock:
                cur.execute(
                    f"CREATE SCHEMA IF NOT EXISTS "
                    f"{client_dml.quote_ident(namespace)}"
                )
            return {"namespace": namespace}

        return self._execute_dml_with_timeout(
            unit, settings["dml_timeout_seconds"]
        )

    def _dml_pinned_flag(self, payload: Dict[str, Any]) -> bool:
        pinned = payload.get("pinned", True)
        if not isinstance(pinned, bool):
            raise client_dml.DmlRejected(
                f"pinned must be a bool, got {pinned!r}"
            )
        return pinned

    def _dml_op_create_table(
        self, payload: Dict[str, Any], settings: Dict[str, Any], client_tag
    ) -> Dict[str, Any]:
        namespace, name = client_dml.parse_table_ref(
            payload, settings["allowed_namespaces"]
        )
        ddl, has_pk = client_dml.compose_create_table_ddl(
            namespace, name, payload.get("columns")
        )
        pinned = self._dml_pinned_flag(payload)
        client_dml.check_quota(
            settings, self.db.namespace_size_estimate_mb(namespace)
        )

        def unit(cur):
            with self.db._write_lock:
                cur.execute("BEGIN TRANSACTION")
                try:
                    if (
                        client_dml.catalog_object_kind(cur, namespace, name)
                        is not None
                    ):
                        raise client_dml.DmlRejected(
                            f"{namespace}.{name} already exists — "
                            f"drop_table first for a rebuild"
                        )
                    cur.execute(ddl)
                    # Post-write catalog assertion: the object must have
                    # landed in the claimed namespace.
                    if (
                        client_dml.catalog_object_kind(cur, namespace, name)
                        != "table"
                    ):
                        raise RuntimeError(
                            f"post-write catalog assertion failed: "
                            f"{namespace}.{name} not found in duckdb_tables()"
                        )
                    self.db.register_table(
                        namespace,
                        name,
                        owner=client_tag,
                        has_pk=has_pk,
                        pinned=pinned,
                        con=cur,
                    )
                    cur.execute("COMMIT")
                except Exception:
                    cur.execute("ROLLBACK")
                    raise
            return {"table": f"{namespace}.{name}", "has_pk": has_pk}

        return self._execute_dml_with_timeout(
            unit, settings["dml_timeout_seconds"]
        )

    def _dml_op_create_as(
        self, payload: Dict[str, Any], settings: Dict[str, Any], client_tag
    ) -> Dict[str, Any]:
        namespace, name = client_dml.parse_table_ref(
            payload, settings["allowed_namespaces"]
        )
        sql = client_dml.validate_source_sql(payload.get("sql"))
        existing = self.db.get_registered_table(namespace, name)
        if existing and existing.get("has_pk"):
            raise client_dml.DmlRejected(
                f"{namespace}.{name} is registered as PK-carrying — "
                f"create_as would drop the constraint; use "
                f"insert_select mode=replace or drop_table first"
            )
        pinned = self._dml_pinned_flag(payload)
        client_dml.check_quota(
            settings, self.db.namespace_size_estimate_mb(namespace)
        )
        qualified = client_dml.qualified_name(namespace, name)

        # Heavy-mitigation path (accepted in the survey exchange): run
        # the source SELECT on a snapshot read cursor OUTSIDE
        # ``_write_lock``, materialize from the in-process Arrow frame
        # under the lock.  If the snapshot cannot be materialized as
        # Arrow, degrade to a plain CTAS inside the locked unit.
        #
        # F26-T2/F3: the snapshot read goes through
        # ``_execute_dml_with_timeout`` (worker + Timer +
        # cursor.interrupt()) so it is bounded by
        # ``dml_timeout_seconds`` like every other op phase —
        # previously ``db.execute_query`` ran synchronously here with
        # no deadline and a heavy source pinned the thread.
        #
        # A timed-out/interrupted snapshot does NOT degrade: the
        # interrupt cancels the in-flight scan but leaves the cursor
        # usable (verified on duckdb 1.5.5 — interrupt() on an idle
        # cursor does not latch), so re-running the same source as
        # inline CTAS would re-pin a worker under _write_lock for a
        # second full timeout.  Fail loudly instead (FAILED + audit).
        def snapshot(cur):
            # Mirror ``_read_only_conn``: explicit READ ONLY txn —
            # defense-in-depth on top of validate_source_sql.
            cur.execute("BEGIN TRANSACTION READ ONLY")
            try:
                return cur.execute(sql).fetch_arrow_table()
            finally:
                try:
                    cur.execute("ROLLBACK")
                except Exception:
                    pass

        try:
            arrow = self._execute_dml_with_timeout(
                snapshot, settings["dml_timeout_seconds"]
            )
        except (TimeoutError, duckdb.InterruptException):
            raise
        except Exception as exc:
            logger.warning(
                "create_as snapshot read failed (%s); degrading to "
                "inline CTAS under _write_lock",
                exc,
            )
            arrow = None

        def unit(cur):
            with self.db._write_lock:
                cur.execute("BEGIN TRANSACTION")
                try:
                    if arrow is not None:
                        cur.register("dml_stage", arrow)
                        try:
                            cur.execute(
                                f"CREATE OR REPLACE TABLE {qualified} "
                                f"AS SELECT * FROM dml_stage"
                            )
                        finally:
                            cur.unregister("dml_stage")
                    else:
                        cur.execute(
                            f"CREATE OR REPLACE TABLE {qualified} AS {sql}"
                        )
                    if (
                        client_dml.catalog_object_kind(cur, namespace, name)
                        != "table"
                    ):
                        raise RuntimeError(
                            f"post-write catalog assertion failed: "
                            f"{namespace}.{name} not found in duckdb_tables()"
                        )
                    rows = int(
                        cur.execute(
                            f"SELECT COUNT(*) FROM {qualified}"
                        ).fetchone()[0]
                    )
                    self.db.register_table(
                        namespace,
                        name,
                        owner=client_tag,
                        has_pk=False,
                        pinned=pinned,
                        con=cur,
                    )
                    self.db.touch_table(
                        namespace, name, rows_est=rows, con=cur
                    )
                    cur.execute("COMMIT")
                except Exception:
                    cur.execute("ROLLBACK")
                    raise
            return {
                "table": f"{namespace}.{name}",
                "rows_written": rows,
                "materialized_via": (
                    "arrow_snapshot" if arrow is not None else "inline_ctas"
                ),
                "_sql_sha256": client_dml.sql_sha256(sql),
            }

        return self._execute_dml_with_timeout(
            unit, settings["dml_timeout_seconds"]
        )

    def _dml_op_create_view(
        self, payload: Dict[str, Any], settings: Dict[str, Any], client_tag
    ) -> Dict[str, Any]:
        namespace, name = client_dml.parse_table_ref(
            payload, settings["allowed_namespaces"]
        )
        sql = client_dml.validate_source_sql(payload.get("sql"))
        existing = self.db.get_registered_table(namespace, name)
        if existing and existing.get("has_pk"):
            raise client_dml.DmlRejected(
                f"{namespace}.{name} is registered as PK-carrying — "
                f"create_view would replace a table; drop_table first"
            )
        pinned = self._dml_pinned_flag(payload)
        qualified = client_dml.qualified_name(namespace, name)

        def unit(cur):
            with self.db._write_lock:
                cur.execute("BEGIN TRANSACTION")
                try:
                    # Bind-check the source before creating anything —
                    # a LIMIT 0 probe validates semantics without
                    # scanning (duckdb binds views lazily otherwise).
                    cur.execute(
                        f"SELECT * FROM ({sql}) AS _dml_src LIMIT 0"
                    )
                    cur.execute(
                        f"CREATE OR REPLACE VIEW {qualified} AS {sql}"
                    )
                    if (
                        client_dml.catalog_object_kind(cur, namespace, name)
                        != "view"
                    ):
                        raise RuntimeError(
                            f"post-write catalog assertion failed: "
                            f"{namespace}.{name} not found in duckdb_views()"
                        )
                    self.db.register_table(
                        namespace,
                        name,
                        owner=client_tag,
                        has_pk=False,
                        pinned=pinned,
                        con=cur,
                    )
                    cur.execute("COMMIT")
                except Exception:
                    cur.execute("ROLLBACK")
                    raise
            return {
                "table": f"{namespace}.{name}",
                "kind": "view",
                "_sql_sha256": client_dml.sql_sha256(sql),
            }

        return self._execute_dml_with_timeout(
            unit, settings["dml_timeout_seconds"]
        )

    def _dml_op_insert_select(
        self, payload: Dict[str, Any], settings: Dict[str, Any], client_tag
    ) -> Dict[str, Any]:
        namespace, name = client_dml.parse_table_ref(
            payload, settings["allowed_namespaces"]
        )
        if self.db.get_registered_table(namespace, name) is None:
            raise client_dml.DmlRejected(
                f"{namespace}.{name} is not registered — insert_select "
                f"requires a registered table (create_table/create_as "
                f"first)"
            )
        mode = payload.get("mode")
        if mode not in client_dml.INSERT_SELECT_MODES:
            raise client_dml.DmlRejected(
                f"mode must be one of "
                f"{sorted(client_dml.INSERT_SELECT_MODES)}, got {mode!r}"
            )
        sql = client_dml.validate_source_sql(payload.get("sql"))
        client_dml.check_quota(
            settings, self.db.namespace_size_estimate_mb(namespace)
        )
        qualified = client_dml.qualified_name(namespace, name)

        def unit(cur):
            with self.db._write_lock:
                # ONE server-side txn — replace is an atomic
                # DELETE FROM + INSERT INTO (client TRANSACTION verbs
                # never cross the wire; the server composes the txn).
                cur.execute("BEGIN TRANSACTION")
                try:
                    if mode == "replace":
                        cur.execute(f"DELETE FROM {qualified}")
                    count_row = cur.execute(
                        f"INSERT INTO {qualified} BY NAME {sql}"
                    ).fetchone()
                    rows_written = int(count_row[0]) if count_row else 0
                    rows_est = int(
                        cur.execute(
                            f"SELECT COUNT(*) FROM {qualified}"
                        ).fetchone()[0]
                    )
                    self.db.touch_table(
                        namespace, name, rows_est=rows_est, con=cur
                    )
                    cur.execute("COMMIT")
                except Exception:
                    cur.execute("ROLLBACK")
                    raise
            return {
                "table": f"{namespace}.{name}",
                "mode": mode,
                "rows_written": rows_written,
                "_sql_sha256": client_dml.sql_sha256(sql),
            }

        return self._execute_dml_with_timeout(
            unit, settings["dml_timeout_seconds"]
        )

    def _dml_op_drop_table(
        self, payload: Dict[str, Any], settings: Dict[str, Any], client_tag
    ) -> Dict[str, Any]:
        namespace, name = client_dml.parse_table_ref(
            payload, settings["allowed_namespaces"]
        )
        if self.db.get_registered_table(namespace, name) is None:
            raise client_dml.DmlRejected(
                f"{namespace}.{name} is not registered — drop_table "
                f"only removes registered objects"
            )
        qualified = client_dml.qualified_name(namespace, name)

        def unit(cur):
            with self.db._write_lock:
                cur.execute("BEGIN TRANSACTION")
                try:
                    kind = client_dml.catalog_object_kind(
                        cur, namespace, name
                    )
                    if kind == "view":
                        cur.execute(f"DROP VIEW IF EXISTS {qualified}")
                    elif kind == "table":
                        cur.execute(f"DROP TABLE IF EXISTS {qualified}")
                    # kind=None: catalog object already gone — the
                    # registry row is still cleaned below (idempotent).
                    self.db.unregister_table(namespace, name, con=cur)
                    cur.execute("COMMIT")
                except Exception:
                    cur.execute("ROLLBACK")
                    raise
            return {
                "table": f"{namespace}.{name}",
                "dropped": kind,
            }

        return self._execute_dml_with_timeout(
            unit, settings["dml_timeout_seconds"]
        )

    def _dml_op_list_tables(
        self, payload: Dict[str, Any], settings: Dict[str, Any], client_tag
    ) -> Dict[str, Any]:
        namespace = payload.get("namespace")
        if namespace is not None:
            namespace = client_dml.validate_name(namespace, "namespace")
            if namespace not in settings["allowed_namespaces"]:
                raise client_dml.DmlRejected(
                    f"namespace {namespace!r} is not in client_dml."
                    f"allowed_namespaces "
                    f"{settings['allowed_namespaces']}"
                )
        return {"tables": self.db.list_registered_tables(namespace)}

    def _dml_op_set_pin(
        self, payload: Dict[str, Any], settings: Dict[str, Any],
        client_tag, pinned: bool,
    ) -> Dict[str, Any]:
        namespace, name = client_dml.parse_table_ref(
            payload, settings["allowed_namespaces"]
        )
        if self.db.get_registered_table(namespace, name) is None:
            raise client_dml.DmlRejected(
                f"{namespace}.{name} is not registered — pin/unpin "
                f"only applies to registered objects"
            )

        def unit(cur):
            with self.db._write_lock:
                self.db.set_table_pinned(
                    namespace, name, pinned, con=cur
                )
            return {
                "table": f"{namespace}.{name}",
                "pinned": pinned,
            }

        return self._execute_dml_with_timeout(
            unit, settings["dml_timeout_seconds"]
        )

    def _dml_op_pin(self, payload, settings, client_tag):
        return self._dml_op_set_pin(payload, settings, client_tag, True)

    def _dml_op_unpin(self, payload, settings, client_tag):
        return self._dml_op_set_pin(payload, settings, client_tag, False)

    def _dml_op_dml_sweep(
        self, payload: Dict[str, Any], settings: Dict[str, Any], client_tag
    ) -> Dict[str, Any]:
        """TTL sweep — drops only ``pinned=false`` registry objects
        older than ``ttl_days`` (chosen over folding into
        ``data_health``: that action is documented report-only and
        smuggling destructive drops into a health read would violate
        its contract; an explicit op also gets its own audit row).
        """
        ttl = payload.get("ttl_days")
        if ttl is None:
            ttl = settings["ttl_days"]
        elif (
            isinstance(ttl, bool)
            or not isinstance(ttl, (int, float))
            or int(ttl) != ttl
            or ttl < 1
        ):
            raise client_dml.DmlRejected(
                f"ttl_days must be an integer >= 1, got {ttl!r}"
            )
        ttl = int(ttl)
        expired = self.db.expired_registrations(ttl)
        dropped: List[str] = []

        def unit(cur):
            # justified: each expired object needs its own
            # kind-aware DROP + registry cleanup — a per-object unit
            # keeps each lock hold short (interleave model).
            for row in expired:
                namespace = row["namespace"]
                name = row["table_name"]
                qualified = client_dml.qualified_name(namespace, name)
                with self.db._write_lock:
                    cur.execute("BEGIN TRANSACTION")
                    try:
                        kind = client_dml.catalog_object_kind(
                            cur, namespace, name
                        )
                        if kind == "view":
                            cur.execute(
                                f"DROP VIEW IF EXISTS {qualified}"
                            )
                        elif kind == "table":
                            cur.execute(
                                f"DROP TABLE IF EXISTS {qualified}"
                            )
                        self.db.unregister_table(
                            namespace, name, con=cur
                        )
                        cur.execute("COMMIT")
                    except Exception:
                        cur.execute("ROLLBACK")
                        raise
                dropped.append(row["full_name"])
            return dropped

        self._execute_dml_with_timeout(unit, settings["dml_timeout_seconds"])
        return {
            "ttl_days": ttl,
            "dropped": dropped,
            "rows_written": len(dropped),
        }

    def run_dml_admin(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Execute one ``dml_admin`` op; ALWAYS audited.

        Returns ``{status: SUCCESS|FAILED|REJECTED, op, ...}`` —
        validation failures are REJECTED (audited, not a transport
        error), execution failures are FAILED.  Raises ``ValueError``
        only when ``payload`` is not a JSON object at all (handled by
        the Flight layer like every other payload-type error).
        """
        if not isinstance(payload, dict):
            raise ValueError(
                "dml_admin payload must be a JSON object, "
                f"got {type(payload).__name__}"
            )
        settings = client_dml.resolve_settings(self.cfg)
        op = payload.get("op")
        target = self._dml_audit_target(payload)
        client_tag: Optional[str] = None
        start_time = datetime.now()
        status = "SUCCESS"
        result: Dict[str, Any] = {}
        try:
            client_tag = client_dml.check_client_tag(
                payload.get("client_tag")
            )
            if not isinstance(op, str) or op not in client_dml.DML_OPS:
                raise client_dml.DmlRejected(
                    f"unknown dml_admin op {op!r}; closed enum: "
                    f"{sorted(client_dml.DML_OPS)}"
                )
            handler = getattr(self, f"_dml_op_{op}")
            result = handler(payload, settings, client_tag)
        except client_dml.DmlRejected as exc:
            status = "REJECTED"
            result = {"error": str(exc)}
        except Exception as exc:
            logger.error("dml_admin op %r failed: %s", op, exc)
            status = "FAILED"
            result = {"error": str(exc)}
        duration_ms = int(
            (datetime.now() - start_time).total_seconds() * 1000
        )
        sql_sha = result.pop("_sql_sha256", None)
        self.db.record_dml_audit(
            client_tag=client_tag,
            op=op if isinstance(op, str) else repr(op),
            target=target,
            source_sql_sha256=sql_sha,
            rows_written=result.get("rows_written"),
            status=status,
            error=result.get("error"),
            duration_ms=duration_ms,
        )
        return {
            "status": status,
            "op": op,
            **result,
            "duration_ms": duration_ms,
        }

    # ----- generalized do_put (descriptor-driven) ----------------------

    def run_dml_put(self, descriptor: Dict[str, Any], arrow_table) -> Dict[str, Any]:
        """Execute a generalized ``do_put`` against an allowed namespace.

        ``descriptor`` is the parsed JSON command
        ``{table, mode, namespace?, pinned?, client_tag?}``;
        ``arrow_table`` is the fully-read ``pa.Table`` stream.  On any
        failure the op is audited (REJECTED or FAILED) and the
        exception re-raised so the Flight layer can surface it as a
        ``FlightServerError`` — ``do_put`` has no result channel.
        """
        settings = client_dml.resolve_settings(self.cfg)
        mode = descriptor.get("mode")
        op_label = f"do_put:{mode}" if isinstance(mode, str) else "do_put"
        target = self._dml_audit_target(descriptor)
        client_tag: Optional[str] = None
        start_time = datetime.now()
        rows_written: Optional[int] = None
        try:
            client_tag = client_dml.check_client_tag(
                descriptor.get("client_tag")
            )
            namespace, name = client_dml.parse_table_ref(
                descriptor, settings["allowed_namespaces"]
            )
            target = f"{namespace}.{name}"
            if mode not in client_dml.DO_PUT_MODES:
                raise client_dml.DmlRejected(
                    f"do_put mode must be one of "
                    f"{sorted(client_dml.DO_PUT_MODES)}, got {mode!r}"
                )
            registered = self.db.get_registered_table(namespace, name)
            if mode in ("append", "replace") and registered is None:
                raise client_dml.DmlRejected(
                    f"{namespace}.{name} is not registered — "
                    f"{mode} requires a registered table"
                )
            if (
                mode == "create_or_replace"
                and registered is not None
                and registered.get("has_pk")
            ):
                raise client_dml.DmlRejected(
                    f"{namespace}.{name} is registered as PK-carrying — "
                    f"create_or_replace would drop the constraint; use "
                    f"mode=replace or drop_table first"
                )
            pinned = self._dml_pinned_flag(descriptor)
            client_dml.check_quota(
                settings, self.db.namespace_size_estimate_mb(namespace)
            )
            qualified = client_dml.qualified_name(namespace, name)

            def unit(cur):
                nonlocal rows_written
                with self.db._write_lock:
                    cur.register("dml_put_stage", arrow_table)
                    try:
                        if mode == "create_or_replace":
                            cur.execute("BEGIN TRANSACTION")
                            try:
                                cur.execute(
                                    f"CREATE OR REPLACE TABLE {qualified} "
                                    f"AS SELECT * FROM dml_put_stage"
                                )
                                if (
                                    client_dml.catalog_object_kind(
                                        cur, namespace, name
                                    )
                                    != "table"
                                ):
                                    raise RuntimeError(
                                        "post-write catalog assertion "
                                        f"failed: {namespace}.{name} not "
                                        "found in duckdb_tables()"
                                    )
                                self.db.register_table(
                                    namespace,
                                    name,
                                    owner=client_tag,
                                    has_pk=False,
                                    pinned=pinned,
                                    con=cur,
                                )
                                cur.execute("COMMIT")
                            except Exception:
                                cur.execute("ROLLBACK")
                                raise
                        else:
                            # append/replace write into a registered,
                            # possibly-constrained table — column-set
                            # check first (schema mismatch -> REJECTED).
                            self._check_stream_columns(
                                cur, namespace, name, arrow_table
                            )
                            cur.execute("BEGIN TRANSACTION")
                            try:
                                if mode == "replace":
                                    cur.execute(
                                        f"DELETE FROM {qualified}"
                                    )
                                cur.execute(
                                    f"INSERT INTO {qualified} BY NAME "
                                    f"SELECT * FROM dml_put_stage"
                                )
                                rows_est = int(
                                    cur.execute(
                                        f"SELECT COUNT(*) FROM {qualified}"
                                    ).fetchone()[0]
                                )
                                self.db.touch_table(
                                    namespace,
                                    name,
                                    rows_est=rows_est,
                                    con=cur,
                                )
                                cur.execute("COMMIT")
                            except Exception:
                                cur.execute("ROLLBACK")
                                raise
                        rows_written = int(arrow_table.num_rows)
                    finally:
                        cur.unregister("dml_put_stage")
                return {
                    "table": f"{namespace}.{name}",
                    "mode": mode,
                    "rows_written": rows_written,
                }

            result = self._execute_dml_with_timeout(
                unit, settings["dml_timeout_seconds"]
            )
        except client_dml.DmlRejected as exc:
            duration_ms = int(
                (datetime.now() - start_time).total_seconds() * 1000
            )
            self.db.record_dml_audit(
                client_tag=client_tag,
                op=op_label,
                target=target,
                bytes_in=int(arrow_table.nbytes),
                status="REJECTED",
                error=str(exc),
                duration_ms=duration_ms,
            )
            raise
        except Exception as exc:
            duration_ms = int(
                (datetime.now() - start_time).total_seconds() * 1000
            )
            self.db.record_dml_audit(
                client_tag=client_tag,
                op=op_label,
                target=target,
                bytes_in=int(arrow_table.nbytes),
                status="FAILED",
                error=str(exc),
                duration_ms=duration_ms,
            )
            raise
        duration_ms = int(
            (datetime.now() - start_time).total_seconds() * 1000
        )
        self.db.record_dml_audit(
            client_tag=client_tag,
            op=op_label,
            target=target,
            bytes_in=int(arrow_table.nbytes),
            rows_written=rows_written,
            status="SUCCESS",
            duration_ms=duration_ms,
        )
        return {"status": "SUCCESS", **result, "duration_ms": duration_ms}

    # ------------------------------------------------------------------
    # Entry points
    # ------------------------------------------------------------------

    def run_initial_load(self, years: Optional[int] = None) -> Dict[str, Any]:
        """Initial historical load: ``years``-year download of all
        configured tickers, upserted into DuckDB.

        When ``years`` is ``None`` (or omitted) the configured
        ``ingestion.initial_load_years`` depth is used (default 6).
        The effective value — explicit arg or config-resolved — must
        be an ``int`` (bool rejected) with ``1 <= years <= 30``; a
        violation raises ``ValueError`` before any download so an
        invalid action payload can never silently produce an empty
        download.

        Returns a dict with ``rows_upserted``, ``tickers_succeeded``,
        ``tickers_failed``, ``errors`` and ``duration_ms``.
        """
        if years is None:
            years = self.cfg.ingestion.initial_load_years
        if (
            not isinstance(years, int)
            or isinstance(years, bool)
            or not 1 <= years <= 30
        ):
            raise ValueError(
                f"years must be an integer in the range [1, 30], got {years!r}"
            )
        start_time = datetime.now()
        logger.info("Initiating initial historical load (%s years)...", years)
        end_dt = datetime.now()
        start_dt = end_dt - timedelta(days=years * 365)
        df, succeeded, failed, errors = self.fetch_and_clean(
            self.cfg.ingestion.tickers,
            start_dt.strftime("%Y-%m-%d"),
            end_dt.strftime("%Y-%m-%d"),
        )
        rows = self.db.upsert_dataframe(df)
        self._record_historical_splits(succeeded)
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        result = {
            "rows_upserted": rows,
            "tickers_succeeded": succeeded,
            "tickers_failed": failed,
            "errors": errors if failed else {},
            "duration_ms": duration_ms,
        }
        self.db.record_ingestion_run("initial_load", result, started_at=start_time)
        # F10-T1: best-effort company-metadata top-up — merged under
        # 'metadata'; never fails the load (mirrors the 'health' embed).
        result["metadata"] = self._metadata_bridge()
        # F12-T1: best-effort weekly-layer refresh — merged under
        # 'weekly'; never fails the load.
        result["weekly"] = self._weekly_bridge()
        # F16-T1: best-effort Fear & Greed refresh — merged under
        # 'fear_greed'; never fails the load.
        result["fear_greed"] = self._fear_greed_bridge()
        if failed:
            logger.warning(
                "Initial load finished with %d failed tickers: %s",
                len(failed),
                failed,
            )
        logger.info("Initial load completed. Rows upserted: %s.", rows)
        return result

    def run_daily_load(
        self, lookback_days: int = 4, force: bool = False
    ) -> Dict[str, Any]:
        """Daily incremental load.

        ``lookback_days`` is the trailing calendar-day refresh window
        (must be >= 1).  ``force`` is an opt-in bypass of the NYSE
        market-closed gate: when True the load runs even on a
        market-closed day so a local machine that was off on a trading
        day catches up on the missed session(s).  The default False
        preserves the existing skip-on-market-closed behavior relied on
        by CI (``scripts/ci_daily_load.py``) and the APScheduler daemon.

        Checks the NYSE market calendar via
        ``pandas_market_calendars``; if the market is closed today the
        run is skipped unless ``force`` is set.  Otherwise: detect newly
        configured tickers and backfill them from the aligned start date
        (or perform a full initial load if the DB is empty), then run a
        trailing ``lookback_days``-day refresh for all configured
        tickers.

        Returns a dict with ``rows_upserted``, ``tickers_succeeded``,
        ``tickers_failed``, ``errors`` and ``duration_ms``.
        """
        if not isinstance(lookback_days, int) or isinstance(lookback_days, bool) or lookback_days < 1:
            raise ValueError(f"lookback_days must be an integer >= 1, got {lookback_days!r}")
        start_time = datetime.now()
        nyse = mcal.get_calendar("NYSE")
        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        sched = nyse.schedule(start_date=today_str, end_date=today_str)

        if sched.empty and not force:
            logger.info(
                "US Market is closed today (Holiday/Weekend). "
                "Daily load execution skipped."
            )
            result = {"status": "SKIPPED", "reason": "Market closed", "duration_ms": 0}
            self.db.record_ingestion_run("daily_load", result, started_at=start_time)
            return result

        if sched.empty:
            logger.info(
                "US Market is closed today but force=True — running "
                "daily load anyway to catch up on missed trading day(s)."
            )

        all_succeeded: List[str] = []
        all_failed: List[str] = []
        all_errors: Dict[str, str] = {}
        total_rows = 0

        # --- New ticker detection: backfill from aligned start date ---
        excluded = set(self.cfg.ingestion.exclude_tickers)
        effective_tickers = [t for t in self.cfg.ingestion.tickers if t not in excluded]
        new_tickers = self.db.detect_new_tickers(effective_tickers)
        if new_tickers:
            aligned_start = self.db.get_aligned_start_date()
            if aligned_start:
                logger.info(
                    "RUNTIME EVENT: New ticker(s) detected: %s. "
                    "Backfilling from aligned start date %s.",
                    new_tickers,
                    aligned_start,
                )
                backfill_end = datetime.now().strftime("%Y-%m-%d")
                df_new, succ, fail, err = self.fetch_and_clean(
                    new_tickers, aligned_start, backfill_end
                )
                rows = self.db.upsert_dataframe(df_new)
                self._record_historical_splits(succ)
                total_rows += rows
                all_succeeded.extend(succ)
                all_failed.extend(fail)
                all_errors.update(err)
                logger.info(
                    "Backfill complete for %s. Rows: %s.", new_tickers, rows
                )
            else:
                logger.info(
                    "RUNTIME EVENT: New ticker(s) detected: %s. "
                    "Database is empty, performing initial load "
                    "(%s years).",
                    new_tickers,
                    self.cfg.ingestion.initial_load_years,
                )
                result = self.run_initial_load()
                total_rows += result.get("rows_upserted", 0)
                all_succeeded.extend(result.get("tickers_succeeded", []))
                all_failed.extend(result.get("tickers_failed", []))
                all_errors.update(result.get("errors", {}))
                # Fall through to trailing refresh

        # --- Trailing refresh for all effective tickers ---
        logger.info(
            "US Market active. Executing daily load (trailing %d calendar days)...",
            lookback_days,
        )
        end_dt = datetime.now()
        start_dt = end_dt - timedelta(days=lookback_days)
        df, succ, fail, err = self.fetch_and_clean(
            effective_tickers,
            start_dt.strftime("%Y-%m-%d"),
            end_dt.strftime("%Y-%m-%d"),
        )
        rows = self.db.upsert_dataframe(df)
        total_rows += rows
        all_succeeded.extend(succ)
        all_failed.extend(fail)
        all_errors.update(err)

        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        status = "PARTIAL" if all_failed else "SUCCESS"
        result = {
            "rows_upserted": total_rows,
            "tickers_succeeded": list(set(all_succeeded)),
            "tickers_failed": list(set(all_failed)),
            "errors": all_errors if all_failed else {},
            "duration_ms": duration_ms,
            "status": status,
        }
        self.db.record_ingestion_run("daily_load", result, started_at=start_time)

        # Sync the canonical DuckDB file to R2 only after successful processing.
        if self.cfg.r2.enabled:
            try:
                sync_result = R2SyncManager(self.cfg).sync(self.cfg.db_path)
                result["r2_sync"] = sync_result
                logger.info("Daily load R2 sync result: %s", sync_result)
            except Exception as exc:
                result["r2_sync"] = {"status": "FAILED", "error": str(exc)}
                logger.error("Daily load R2 sync failed: %s", exc)

        # F10-T1: best-effort company-metadata top-up — merged under
        # 'metadata' on SUCCESS/PARTIAL only (the SKIPPED path above
        # returns early); never fails the load.
        result["metadata"] = self._metadata_bridge()

        # F12-T1: best-effort weekly-layer refresh — merged under
        # 'weekly' on SUCCESS/PARTIAL only; never fails the load.
        result["weekly"] = self._weekly_bridge()

        # F16-T1: best-effort Fear & Greed refresh — merged under
        # 'fear_greed' on SUCCESS/PARTIAL only; never fails the load.
        result["fear_greed"] = self._fear_greed_bridge()

        if all_failed:
            logger.warning(
                "Daily load finished with %d failed tickers: %s",
                len(result["tickers_failed"]),
                result["tickers_failed"],
            )
        logger.info("Daily load complete. Total rows upserted: %s.", total_rows)
        return result

    def run_on_request_load(
        self, tickers: List[str], start_date: str, end_date: str
    ) -> Dict[str, Any]:
        """On-request load: custom tickers and date range, upserted
        into DuckDB.

        Returns a dict with ``rows_upserted``, ``tickers_succeeded``,
        ``tickers_failed``, ``errors`` and ``duration_ms``.
        """
        start_time = datetime.now()
        logger.info(
            "Executing on-request load for tickers %s [%s to %s]...",
            tickers,
            start_date,
            end_date,
        )
        df, succeeded, failed, errors = self.fetch_and_clean(
            tickers, start_date, end_date
        )
        rows = self.db.upsert_dataframe(df)
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        result = {
            "rows_upserted": rows,
            "tickers_succeeded": succeeded,
            "tickers_failed": failed,
            "errors": errors if failed else {},
            "duration_ms": duration_ms,
        }
        self.db.record_ingestion_run(
            "on_request_load", result, started_at=start_time
        )
        # F12-T1: best-effort weekly-layer refresh — merged under
        # 'weekly'; never fails the load.
        result["weekly"] = self._weekly_bridge()
        # F16-T1: best-effort Fear & Greed refresh — merged under
        # 'fear_greed'; never fails the load.
        result["fear_greed"] = self._fear_greed_bridge()
        if failed:
            logger.warning(
                "On-request load finished with %d failed tickers: %s",
                len(failed),
                failed,
            )
        logger.info("On-request load complete. Rows upserted: %s.", rows)
        return result


__all__ = ["IngestionEngine"]
