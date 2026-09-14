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
from typing import Any, Dict, List, Optional, Tuple

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
    #: Cool-down (seconds) before the second-pass retry download for
    #: tickers that returned empty datasets.  Mockable via ``time.sleep``.
    _RETRY_DELAY_S: float = 15.0

    def _batch_download(
        self, tickers: List[str], start_date: str, end_date: str
    ) -> pd.DataFrame:
        """Batch download via yf.download().

        ``end_date`` is inclusive — adjusted +1 day internally to satisfy
        yfinance's exclusive ``end`` parameter.
        Retries up to ``_MAX_ATTEMPTS`` times with exponential backoff on
        failure, raising ``DownloadError`` if all attempts fail.
        ``progress=False`` disables the tqdm progress bar — the Flight
        server runs headless under Windows Task Scheduler (no console,
        ``sys.stderr`` is ``None``), where the bar's finalizer crashes
        on ``sys.stderr.flush()`` *after* the download completes.
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
                    progress=False,
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

    def _retry_failed_download(
        self, tickers: List[str], start_date: str, end_date: str
    ) -> pd.DataFrame:
        """Single follow-up ``yf.download`` for empty-dataset tickers.

        Unlike ``_batch_download`` this performs exactly one attempt —
        it is itself the second pass, run after a rate-limit cool-down —
        and uses ``threads=False`` for gentler pacing.  ``end_date`` is
        inclusive (+1 day internally), ``progress=False`` keeps the
        headless Task Scheduler service safe.
        """
        end_dt = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)
        return yf.download(
            tickers=tickers,
            start=start_date,
            end=end_dt.strftime("%Y-%m-%d"),
            interval="1d",
            auto_adjust=True,
            group_by="ticker",
            threads=False,
            progress=False,
        )


class _NormalizeMixin:
    """Normalize raw yf.download output to the flat daily_close schema."""

    _OUTPUT_COLUMNS = ["Date", "Ticker", "Open", "High", "Low", "Close", "Volume"]

    def _confirm_delisted(self, ticker: str) -> bool:
        """Cheap independent confirmation that ``ticker`` has no data.

        Runs a small single-ticker ``yf.download`` over a recent short
        window.  Returns ``True`` only when that second, independent
        request also comes back empty — the delisting is confirmed by
        two empty responses.  Returned rows OR an unexpected error both
        mean *unconfirmed*: yfinance under rate-limiting reports valid
        tickers as "possibly delisted", and a throttle must never wipe
        real history, so the caller must then skip ``purge_ticker``.
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
        """Normalize batch yf.download output to flat schema.

        Returns ``(df, succeeded, failed, errors)``.

        Uses a pyarrow intermediate per ticker for explicit schema control
        and to avoid pandas ``concat`` overhead on the hot path.  An
        empty ticker in ``exclude_tickers`` is purged only after
        ``_confirm_delisted`` independently verifies the delisting.
        """
        succeeded: List[str] = []
        failed: List[str] = []
        errors: Dict[str, str] = {}
        arrow_tables: List[pa.Table] = []

        multi = df.columns.nlevels == 2 if not df.empty else False

        for t in tickers:
            # justified: yf.download returns MultiIndex columns per ticker.
            # Each ticker's sub-frame needs individual dropna + schema
            # normalization. Cannot vectorize per-key dropna on MultiIndex.
            try:
                if multi:
                    ticker_df = df[t].copy()
                else:
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
        """Per-ticker split detection, batched re-download.

        For each ticker, query ``yf.Ticker(t).splits`` (no batch API).
        Splits are filtered against the global aligned start date and
        ``splits_processed``.  All affected tickers are re-downloaded in
        one batch from the aligned start date.

        The re-download happens BEFORE any destructive writes: only
        tickers that actually returned rows have their old data deleted,
        the new data upserted, and their splits recorded as processed.
        A ticker missing from the re-download keeps its existing data and
        its splits stay unprocessed so a later run retries them.
        """
        # Phase 1 — collect candidate splits for every ticker in one pass.
        # justified: yfinance has no batch split API — yf.Ticker(t).splits
        # must be called per-ticker. This is an external API constraint.
        aligned_start = self.db.get_aligned_start_date()
        if aligned_start is None:
            return

        collected: Dict[str, List[Tuple[str, float]]] = {}
        # justified: yfinance has no batch split API — yf.Ticker(t).splits
        # must be called per-ticker. This is an external API constraint.
        for t in tickers:
            try:
                stock = yf.Ticker(t)
                splits = stock.splits
                if splits is None or splits.empty:
                    continue
                for split_dt, factor in splits.sort_index().items():
                    split_date_str = split_dt.strftime("%Y-%m-%d")
                    if split_date_str < aligned_start:
                        continue
                    collected.setdefault(t, []).append(
                        (split_date_str, float(factor))
                    )
            except Exception as e:
                logger.error("Failed fetching splits for ticker '%s': %s", t, e)

        # Phase 2 — filter against DB and collect affected tickers.
        affected: List[Tuple[str, List[Tuple[str, float]]]] = []
        for t, split_list in collected.items():
            try:
                unprocessed = [
                    (ds, fac)
                    for ds, fac in split_list
                    if not self.db.is_split_processed(t, ds)
                ]
                if not unprocessed:
                    continue
                affected.append((t, unprocessed))
            except Exception as e:
                logger.error("Failed filtering splits for ticker '%s': %s", t, e)

        if not affected:
            return

        # Phase 3 — single batched re-download for all affected tickers,
        # performed BEFORE any destructive writes.  The previous ordering
        # (record splits -> delete history -> re-download) permanently lost
        # data when the re-download failed or returned empty.
        today = datetime.now().strftime("%Y-%m-%d")
        affected_tickers = [t for t, _ in affected]

        logger.warning(
            "RUNTIME EVENT: Stock split(s) detected for %d ticker(s): %s. "
            "Re-downloading from %s to %s in one batch before replacing data.",
            len(affected_tickers),
            affected_tickers,
            aligned_start,
            today,
        )

        try:
            df_redownload = self._batch_download(affected_tickers, aligned_start, today)
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

        # Phase 4 — replace data only for tickers that actually returned
        # rows in the re-download.  Tickers missing from the re-download
        # keep their existing data and their splits stay unprocessed so a
        # later run retries them.
        redownloaded = set(redl_ok)
        missing = [t for t, _ in affected if t not in redownloaded]
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
        # deletes are fast SQL and the per-ticker call is kept for
        # compatibility with tests/downstream mocks.
        for t, _unprocessed in affected:
            if t not in redownloaded:
                continue
            self.db.delete_ticker_data(t)

        self.db.upsert_dataframe(df_norm)

        # Record splits ONLY for tickers whose data was actually replaced;
        # splits for missing tickers remain unprocessed for the next run.
        split_records = [
            (t, split_date, factor)
            for t, unprocessed in affected
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

        After a full historical download, the data already reflects every
        historical split via ``auto_adjust=True``.  Recording those splits
        prevents the next incremental run from treating them as new.
        """
        aligned_start = self.db.get_aligned_start_date()
        if aligned_start is None:
            return

        all_split_records: List[Tuple[str, str, float]] = []
        for t in tickers:
            # justified: yfinance has no batch split API — yf.Ticker(t).splits
            # must be called per-ticker. This is an external API constraint.
            try:
                stock = yf.Ticker(t)
                splits = stock.splits
                if splits is None or splits.empty:
                    continue

                for split_dt, factor in splits.sort_index().items():
                    split_date = split_dt.strftime("%Y-%m-%d")
                    if split_date < aligned_start:
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

        # --- Batch pricing download (with retry/backoff) ---
        try:
            raw_df = self._batch_download(effective_tickers, start_date, end_date)
        except DownloadError as exc:
            # All retries exhausted — mark every ticker as failed.
            failed = list(effective_tickers)
            errors: Dict[str, str] = {t: str(exc) for t in effective_tickers}
            return pd.DataFrame(), [], failed, errors

        if raw_df.empty:
            # All tickers returned nothing — vectorized dict comprehension
            df = pd.DataFrame()
            succeeded: List[str] = []
            failed = list(effective_tickers)
            errors = {t: "Empty dataset from yf.download" for t in effective_tickers}
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
            df, succeeded, failed, errors = self._retry_empty_failures(
                df, succeeded, failed, errors, retry_candidates,
                start_date, end_date,
            )

        return df, succeeded, failed, errors

    def _retry_empty_failures(
        self,
        df: pd.DataFrame,
        succeeded: List[str],
        failed: List[str],
        errors: Dict[str, str],
        retry_candidates: List[str],
        start_date: str,
        end_date: str,
    ) -> Tuple[pd.DataFrame, List[str], List[str], Dict[str, str]]:
        """Second-pass retry + merge for empty-dataset failures.

        Sleeps ``_RETRY_DELAY_S`` to let a yfinance rate limit cool,
        re-downloads only the failed tickers via
        ``_retry_failed_download`` (one attempt, ``threads=False``),
        normalizes the result and merges recovered tickers back into
        the ``(df, succeeded, failed, errors)`` quadruple.
        """
        logger.warning(
            "RUNTIME EVENT: %d ticker(s) returned empty datasets: %s. "
            "Retrying once after a %.0fs rate-limit cool-down.",
            len(retry_candidates),
            retry_candidates,
            self._RETRY_DELAY_S,
        )
        time.sleep(self._RETRY_DELAY_S)
        try:
            retry_raw = self._retry_failed_download(
                retry_candidates, start_date, end_date
            )
        except Exception as exc:  # noqa: BLE001 — stay failed, never raise
            logger.error(
                "Retry download raised for %s: %s — tickers stay failed.",
                retry_candidates,
                exc,
            )
            return df, succeeded, failed, errors
        if retry_raw.empty:
            logger.warning(
                "RUNTIME EVENT: Retry download returned no data for %d "
                "ticker(s): %s — they stay failed.",
                len(retry_candidates),
                retry_candidates,
            )
            return df, succeeded, failed, errors

        retry_df, retry_ok, _retry_fail, retry_err = self._normalize_batch_df(
            retry_raw, retry_candidates
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
        logger.info("Initiating initial historical load (%d years)...", years)
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
        logger.info("Initial load completed. Rows upserted: %d.", rows)
        return {
            "rows_upserted": rows,
            "tickers_succeeded": succeeded,
            "tickers_failed": failed,
            "errors": errors if failed else {},
            "duration_ms": duration_ms,
        }

    def run_daily_load(
        self, lookback_days: int = 4, force: bool = False
    ) -> Dict[str, Any]:
        """Daily incremental load.

        ``lookback_days`` is the trailing calendar-day refresh window
        (must be >= 1).  ``force`` is an opt-in bypass of the NYSE
        market-closed gate: when True the load runs even on a
        market-closed day so a local machine that was off on a trading
        day catches up on the missed session(s).  The default False
        preserves the existing skip-on-market-closed behavior.
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
            return {"status": "SKIPPED", "reason": "Market closed", "duration_ms": 0}

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
                    "Backfill complete for %s. Rows: %d.", new_tickers, rows
                )
            else:
                logger.info(
                    "RUNTIME EVENT: New ticker(s) detected: %s. "
                    "Database is empty, performing initial load "
                    "(%d years).",
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
