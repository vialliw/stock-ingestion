"""YFinance ingestion engine for the stock-ingestion pipeline.

Implements ``IngestionEngine`` per spec §10 (reference implementation):
batch download via ``yf.download``, per-ticker split detection with
DELETE + re-download, dataframe normalization, and the three entry
points (``run_initial_load``, ``run_daily_load``, ``run_on_request_load``).

Spec reference: docs/tech_spec/gemini_tech_spec.1.0.2.md §10.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import pandas_market_calendars as mcal
import yfinance as yf

from src.config import PipelineConfig
from src.db import DatabaseManager
from src.r2_sync import R2SyncManager

logger = logging.getLogger(__name__)


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
        Downloads use ``auto_adjust=True``, ``interval="1d"``,
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
            auto_adjust=True,
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

        Handles both multi-ticker (``columns.nlevels == 2``) and
        single-ticker output.  Drops rows with NaN OHLC.  An empty
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
                    # Multi-ticker: columns are (ticker, field)
                    ticker_df = df[t].copy()
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

                ticker_df = ticker_df.reset_index()
                ticker_df["Ticker"] = t
                ticker_df["Date"] = pd.to_datetime(ticker_df["Date"]).dt.date
                records.append(
                    ticker_df[
                        ["Date", "Ticker", "Open", "High", "Low", "Close", "Volume"]
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
            return pd.concat(records, ignore_index=True), succeeded, failed, errors
        return pd.DataFrame(), succeeded, failed, errors

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

    def run_metadata_load(
        self,
        tickers: Optional[List[str]] = None,
        refresh: bool = False,
        delay_s: float = 0.5,
        max_consecutive_failures: int = 15,
        max_tickers: Optional[int] = None,
        batch_size: int = 50,
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

        Returns ``{tickers_fetched, tickers_skipped, tickers_failed,
        rows_upserted, aborted, reason, errors, duration_ms}``.
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
        }
        if failed:
            logger.warning(
                "Metadata load finished with %d fetch failure(s): %s",
                len(failed),
                failed,
            )
        logger.info(
            "Metadata load complete. Fetched: %s, skipped: %s, "
            "rows upserted: %s, aborted: %s.",
            fetched,
            skipped,
            rows_upserted,
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
        if failed:
            logger.warning(
                "On-request load finished with %d failed tickers: %s",
                len(failed),
                failed,
            )
        logger.info("On-request load complete. Rows upserted: %s.", rows)
        return result


__all__ = ["IngestionEngine"]
