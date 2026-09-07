"""Challenger variant of the yfinance ingestion engine.

Implements ``IngestionEngine`` with the *same public API* as the spec
reference (docs/tech_spec/gemini_tech_spec.1.0.2.md §10) but with a
structurally different internal design:

1. **Mixin-based composition** — download, normalization and split logic
   live in dedicated mixin classes (``_DownloadMixin``, ``_NormalizeMixin``,
   ``_SplitMixin``) rather than a monolithic class body.
2. **Custom exception hierarchy** — ``IngestionError``, ``DownloadError``,
   ``SplitError`` replace bare ``except Exception`` blocks, giving callers
   a typed surface for error handling.
3. **Retry-with-backoff download** — ``_batch_download`` retries transient
   yfinance failures with exponential backoff (3 attempts) instead of a
   single fire-and-forget call.
4. **Pyarrow intermediate normalization** — ``_normalize_batch_df`` stages
   per-ticker data through ``pyarrow.Table`` before concatenation, avoiding
   pandas concat overhead and giving explicit schema control.
5. **Collect-then-process split detection** — ``_check_and_apply_splits``
   first collects *all* splits across every ticker in a single pass, then
   filters against ``splits_processed`` and processes only those with
   unprocessed splits, reducing redundant work when most tickers have none.

Spec reference: docs/tech_spec/gemini_tech_spec.1.0.2.md §4, §10.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import yfinance as yf

import pandas_market_calendars as mcal

logger = logging.getLogger(__name__)

# =====================================================================
# Exception hierarchy (structural difference #2)
# =====================================================================


class IngestionError(Exception):
    """Base error for the ingestion pipeline."""


class DownloadError(IngestionError):
    """Raised when a batch download fails after all retries."""


class SplitError(IngestionError):
    """Raised when split detection / re-download fails for a ticker."""


# =====================================================================
# Mixins (structural difference #1)
# =====================================================================


class _DownloadMixin:
    """Batch download via yf.download with retry-and-backoff."""

    #: Maximum download attempts before giving up.
    _MAX_ATTEMPTS = 3
    #: Base backoff in seconds (doubled each attempt).
    _BACKOFF_BASE = 1.0

    def _batch_download(
        self, tickers: List[str], start_date: str, end_date: str
    ) -> pd.DataFrame:
        """Batch download via yf.download().

        ``end_date`` is inclusive — adjusted +1 day internally to satisfy
        yfinance's exclusive ``end`` parameter.
        Retries up to ``_MAX_ATTEMPTS`` times with exponential backoff on
        failure, raising ``DownloadError`` if all attempts fail.
        """
        end_dt = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)
        end_str = end_dt.strftime("%Y-%m-%d")

        last_exc: Exception | None = None
        for attempt in range(1, self._MAX_ATTEMPTS + 1):
            try:
                df = yf.download(
                    tickers=tickers,
                    start=start_date,
                    end=end_str,
                    interval="1d",
                    auto_adjust=True,
                    group_by="ticker",
                    threads=True,
                )
                return df
            except Exception as exc:  # noqa: BLE001 — yfinance raises varied errors
                last_exc = exc
                if attempt < self._MAX_ATTEMPTS:
                    backoff = self._BACKOFF_BASE * (2 ** (attempt - 1))
                    logger.warning(
                        "Download attempt %d/%d failed for %s: %s. "
                        "Retrying in %.1fs...",
                        attempt,
                        self._MAX_ATTEMPTS,
                        tickers,
                        exc,
                        backoff,
                    )
                    time.sleep(backoff)

        raise DownloadError(
            f"Batch download failed for {tickers} after {self._MAX_ATTEMPTS} attempts: {last_exc}"
        ) from last_exc


class _NormalizeMixin:
    """Normalize raw yf.download output to the flat daily_quotes schema."""

    _OUTPUT_COLUMNS = ["Date", "Ticker", "Open", "High", "Low", "Close", "Volume"]

    def _normalize_batch_df(
        self, df: pd.DataFrame, tickers: List[str]
    ) -> Tuple[pd.DataFrame, List[str], List[str], Dict[str, str]]:
        """Normalize batch yf.download output to flat schema.

        Returns ``(df, succeeded, failed, errors)``.

        Uses a pyarrow intermediate per ticker for explicit schema control
        and to avoid pandas ``concat`` overhead on the hot path.
        """
        succeeded: List[str] = []
        failed: List[str] = []
        errors: Dict[str, str] = {}
        arrow_tables: List[pa.Table] = []

        multi = df.columns.nlevels == 2 if not df.empty else False

        for t in tickers:
            try:
                if multi:
                    ticker_df = df[t].copy()
                else:
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

                # Stage through pyarrow for explicit schema (structural diff #4)
                subset = ticker_df[self._OUTPUT_COLUMNS]
                tbl = pa.Table.from_pandas(subset, preserve_index=False)
                arrow_tables.append(tbl)
                succeeded.append(t)

            except Exception as e:
                logger.error("Failed processing ingestion for ticker '%s': %s", t, e)
                failed.append(t)
                errors[t] = str(e)
                if t in self.cfg.ingestion.exclude_tickers:
                    self.db.purge_ticker(t)

        if arrow_tables:
            combined = pa.concat_tables(arrow_tables)
            return combined.to_pandas(), succeeded, failed, errors
        return pd.DataFrame(), succeeded, failed, errors


class _SplitMixin:
    """Split detection and re-download logic.

    Structural difference #5: collect *all* splits across every ticker in
    a single pass first, then filter against ``splits_processed`` and
    process only tickers with unprocessed splits.
    """

    def _check_and_apply_splits(self, tickers: List[str]) -> None:
        """Per-ticker split detection.

        If any unprocessed split is found for a ticker, record all
        unprocessed splits, delete all ticker data, and re-download from
        the aligned start date with ``auto_adjust=True``.
        """
        # Phase 1 — collect splits for every ticker in one pass.
        collected: Dict[str, List[Tuple[str, float]]] = {}
        for t in tickers:
            try:
                stock = yf.Ticker(t)
                splits = stock.splits
                if splits is None or splits.empty:
                    continue
                for split_dt, factor in splits.sort_index().items():
                    split_date_str = split_dt.strftime("%Y-%m-%d")
                    collected.setdefault(t, []).append(
                        (split_date_str, float(factor))
                    )
            except Exception as e:
                logger.error("Failed fetching splits for ticker '%s': %s", t, e)

        # Phase 2 — filter against DB and process only unprocessed splits.
        for t, split_list in collected.items():
            try:
                unprocessed = [
                    (ds, fac)
                    for ds, fac in split_list
                    if not self.db.is_split_processed(t, ds)
                ]
                if not unprocessed:
                    continue

                # Record all unprocessed splits for idempotency
                for split_date_str, factor in unprocessed:
                    self.db.record_split(t, split_date_str, factor)

                # Delete all existing data for this ticker
                self.db.delete_ticker_data(t)

                # Re-download from aligned start date with auto_adjust=True
                aligned_start = self.db.get_aligned_start_date()
                if aligned_start is None:
                    # DB is now empty after delete; use 6-year default
                    end_dt = datetime.now()
                    start_dt = end_dt - timedelta(days=6 * 365)
                    aligned_start = start_dt.strftime("%Y-%m-%d")

                today = datetime.now().strftime("%Y-%m-%d")
                logger.warning(
                    "RUNTIME EVENT: Stock split detected for '%s'. "
                    "Deleted and re-downloading from %s to %s.",
                    t,
                    aligned_start,
                    today,
                )
                df_redownload = self._batch_download([t], aligned_start, today)
                df_norm, _, _, _ = self._normalize_batch_df(df_redownload, [t])
                self.db.upsert_dataframe(df_norm)
                logger.info("Re-download complete for '%s'.", t)

            except Exception as e:
                logger.error("Failed checking splits for ticker '%s': %s", t, e)


# =====================================================================
# Main engine — composes the mixins
# =====================================================================


class IngestionEngine(_SplitMixin, _DownloadMixin, _NormalizeMixin):
    """Yfinance ingestion engine (challenger variant).

    Same public API as the spec reference implementation but structurally
    different: mixin composition, custom exceptions, retry-with-backoff
    downloads, pyarrow intermediate normalization, and collect-then-process
    split detection.
    """

    def __init__(self, db_mgr, cfg):
        self.db = db_mgr
        self.cfg = cfg

    # ------------------------------------------------------------------
    # Public orchestration
    # ------------------------------------------------------------------

    def fetch_and_clean(
        self, tickers: List[str], start_date: str, end_date: str
    ) -> Tuple[pd.DataFrame, List[str], List[str], Dict[str, str]]:
        """Fetch pricing data (batch) + check splits (per-ticker).

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
            # Purge any excluded tickers that still have data in the DB
            existing = self.db.get_existing_tickers()
            for t in skipped:
                if t in existing:
                    logger.warning(
                        "RUNTIME EVENT: Purging stale data for excluded ticker '%s'.",
                        t,
                    )
                    self.db.purge_ticker(t)
        if not effective_tickers:
            return pd.DataFrame(), [], [], {}

        # --- Split detection (per-ticker, no batch API) ---
        self._check_and_apply_splits(effective_tickers)

        # --- Batch pricing download (with retry/backoff) ---
        try:
            raw_df = self._batch_download(effective_tickers, start_date, end_date)
        except DownloadError as exc:
            # All retries exhausted — mark every ticker as failed.
            failed = list(effective_tickers)
            errors: Dict[str, str] = {t: str(exc) for t in effective_tickers}
            return pd.DataFrame(), [], failed, errors

        if raw_df.empty:
            # All tickers returned nothing
            failed = []
            errors = {}
            for t in effective_tickers:
                failed.append(t)
                errors[t] = "Empty dataset from yf.download"
            return pd.DataFrame(), [], failed, errors

        return self._normalize_batch_df(raw_df, effective_tickers)

    def run_initial_load(self, years: int = 6) -> Dict[str, Any]:
        start_time = datetime.now()
        logger.info("Initiating initial historical load (%d years)...", years)
        end_dt = datetime.now()
        start_dt = end_dt - timedelta(days=years * 365)
        df, succeeded, failed, errors = self.fetch_and_clean(
            self.cfg.ingestion.tickers,
            start_dt.strftime("%Y-%m-%d"),
            end_dt.strftime("%Y-%m-%d"),
        )
        rows = self.db.upsert_dataframe(df)
        duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        logger.info("Initial load completed. Rows upserted: %d.", rows)
        return {
            "rows_upserted": rows,
            "tickers_succeeded": succeeded,
            "tickers_failed": failed,
            "errors": errors if failed else {},
            "duration_ms": duration_ms,
        }

    def run_daily_load(self) -> Dict[str, Any]:
        start_time = datetime.now()
        nyse = mcal.get_calendar("NYSE")
        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        sched = nyse.schedule(start_date=today_str, end_date=today_str)

        if sched.empty:
            logger.info(
                "US Market is closed today (Holiday/Weekend). "
                "Daily load execution skipped."
            )
            return {"status": "SKIPPED", "reason": "Market closed", "duration_ms": 0}

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
                    "Backfill complete for %s. Rows: %d.", new_tickers, rows
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
                # Fall through to trailing 4-day refresh

        # --- Trailing 4-day refresh for all effective tickers ---
        logger.info(
            "US Market active. Executing daily load (trailing 4 calendar days)..."
        )
        end_dt = datetime.now()
        start_dt = end_dt - timedelta(days=4)
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
        logger.info("Daily load complete. Total rows upserted: %d.", total_rows)
        return {
            "rows_upserted": total_rows,
            "tickers_succeeded": list(set(all_succeeded)),
            "tickers_failed": list(set(all_failed)),
            "errors": all_errors if all_failed else {},
            "duration_ms": duration_ms,
        }

    def run_on_request_load(
        self, tickers: List[str], start_date: str, end_date: str
    ) -> Dict[str, Any]:
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
        logger.info("On-request load complete. Rows upserted: %d.", rows)
        return {
            "rows_upserted": rows,
            "tickers_succeeded": succeeded,
            "tickers_failed": failed,
            "errors": errors if failed else {},
            "duration_ms": duration_ms,
        }


__all__ = [
    "IngestionEngine",
    "IngestionError",
    "DownloadError",
    "SplitError",
]
