"""YFinance ingestion engine for the stock-ingestion pipeline.

Implements ``IngestionEngine`` per spec §10 (reference implementation):
batch download via ``yf.download``, per-ticker split detection with
DELETE + re-download, dataframe normalization, and the three entry
points (``run_initial_load``, ``run_daily_load``, ``run_on_request_load``).

Spec reference: docs/tech_spec/gemini_tech_spec.1.0.2.md §10.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple

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

        All tickers with unprocessed splits have their splits recorded, old
        data deleted, and then are re-downloaded together in a single
        ``yf.download`` batch from the minimum earliest date across the
        affected tickers.  This avoids one API call per split ticker.
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

        # Phase 2 — batch SQL writes: record splits and delete old data.
        all_split_records = [
            (t, split_date, factor)
            for t, _earliest, unprocessed in affected
            for split_date, factor in unprocessed
        ]
        self.db.batch_record_splits(all_split_records)

        for t, _earliest, _unprocessed in affected:
            # justified: delete_ticker_data is the existing per-ticker DB API;
            # deletes are fast SQL and keeping the per-ticker call avoids
            # breaking tests/downstream code that mock delete_ticker_data.
            self.db.delete_ticker_data(t)

        # Phase 3 — single batched re-download for all affected tickers.
        min_start = min(earliest for _, earliest, _ in affected)
        today = datetime.now().strftime("%Y-%m-%d")
        affected_tickers = [t for t, _, _ in affected]

        logger.warning(
            "RUNTIME EVENT: Stock split(s) detected for %d ticker(s): %s. "
            "Deleted and re-downloading from %s to %s in one batch.",
            len(affected_tickers),
            affected_tickers,
            min_start,
            today,
        )

        try:
            df_redownload = self._batch_download(affected_tickers, min_start, today)
            df_norm, _, _, _ = self._normalize_batch_df(df_redownload, affected_tickers)
            self.db.upsert_dataframe(df_norm)
            logger.info(
                "Re-download complete for %d ticker(s): %s",
                len(affected_tickers),
                affected_tickers,
            )
        except Exception as e:
            logger.error(
                "Failed batch re-download for split tickers %s: %s",
                affected_tickers,
                e,
            )

    # ------------------------------------------------------------------
    # Batch download
    # ------------------------------------------------------------------

    def _batch_download(
        self, tickers: List[str], start_date: str, end_date: str
    ) -> pd.DataFrame:
        """Batch download via ``yf.download()``.

        ``end_date`` is treated as inclusive, so it is adjusted +1 day
        internally to satisfy yfinance's exclusive ``end`` parameter.
        Downloads use ``auto_adjust=True``, ``interval="1d"``,
        ``group_by='ticker'`` and ``threads=True``.
        """
        end_dt = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)
        df = yf.download(
            tickers=tickers,
            start=start_date,
            end=end_dt.strftime("%Y-%m-%d"),
            interval="1d",
            auto_adjust=True,
            group_by="ticker",
            threads=True,
        )
        return df

    # ------------------------------------------------------------------
    # Normalization
    # ------------------------------------------------------------------

    def _normalize_batch_df(
        self, df: pd.DataFrame, tickers: List[str]
    ) -> Tuple[pd.DataFrame, List[str], List[str], Dict[str, str]]:
        """Normalize batch ``yf.download`` output to the flat schema.

        Handles both multi-ticker (``columns.nlevels == 2``) and
        single-ticker output.  Drops rows with NaN OHLC.  An empty
        ticker that is in ``exclude_tickers`` is purged from the DB and
        logged; an empty ticker not in ``exclude_tickers`` produces a
        warning log only (no purge).  Returns
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
                        logger.warning(
                            "RUNTIME EVENT: Ticker '%s' returned empty dataset. "
                            "Matches exclusion criteria.",
                            t,
                        )
                        self.db.purge_ticker(t)
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
                    self.db.purge_ticker(t)

        if records:
            return pd.concat(records, ignore_index=True), succeeded, failed, errors
        return pd.DataFrame(), succeeded, failed, errors

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
            failed: List[str] = list(effective_tickers)
            errors: Dict[str, str] = {t: "Empty dataset from yf.download" for t in effective_tickers}
            return pd.DataFrame(), [], failed, errors

        return self._normalize_batch_df(raw_df, effective_tickers)

    # ------------------------------------------------------------------
    # Entry points
    # ------------------------------------------------------------------

    def run_initial_load(self, years: int = 6) -> Dict[str, Any]:
        """Initial historical load: ``years``-year download of all
        configured tickers, upserted into DuckDB.

        Returns a dict with ``rows_upserted``, ``tickers_succeeded``,
        ``tickers_failed``, ``errors`` and ``duration_ms``.
        """
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
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        result = {
            "rows_upserted": rows,
            "tickers_succeeded": succeeded,
            "tickers_failed": failed,
            "errors": errors if failed else {},
            "duration_ms": duration_ms,
        }
        self.db.record_ingestion_run("initial_load", result, started_at=start_time)
        logger.info("Initial load completed. Rows upserted: %s.", rows)
        return result

    def run_daily_load(self, lookback_days: int = 4) -> Dict[str, Any]:
        """Daily incremental load.

        ``lookback_days`` is the trailing calendar-day refresh window
        (must be >= 1).

        Checks the NYSE market calendar via
        ``pandas_market_calendars``; if the market is closed today the
        run is skipped.  Otherwise: detect newly configured tickers and
        backfill them from the aligned start date (or perform a full
        initial load if the DB is empty), then run a trailing
        ``lookback_days``-day refresh for all configured tickers.

        Returns a dict with ``rows_upserted``, ``tickers_succeeded``,
        ``tickers_failed``, ``errors`` and ``duration_ms``.
        """
        if not isinstance(lookback_days, int) or isinstance(lookback_days, bool) or lookback_days < 1:
            raise ValueError(f"lookback_days must be an integer >= 1, got {lookback_days!r}")
        start_time = datetime.now()
        nyse = mcal.get_calendar("NYSE")
        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        sched = nyse.schedule(start_date=today_str, end_date=today_str)

        if sched.empty:
            logger.info(
                "US Market is closed today (Holiday/Weekend). "
                "Daily load execution skipped."
            )
            result = {"status": "SKIPPED", "reason": "Market closed", "duration_ms": 0}
            self.db.record_ingestion_run("daily_load", result, started_at=start_time)
            return result

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
                    "Database is empty, performing initial load (6 years).",
                    new_tickers,
                )
                result = self.run_initial_load(years=6)
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
        logger.info("On-request load complete. Rows upserted: %s.", rows)
        return result


__all__ = ["IngestionEngine"]
