RRG DASHBOARD: BACKDATED JSON SUPPORT
====================================

INSTALLATION (existing stock-ingestion repository)
1. Back up scripts/rrg_dashboard.py.
2. Replace it with scripts/rrg_dashboard.py from this archive.
3. Place tests/test_rrg_dashboard_backdate.py into your tests directory (optional).

EXAMPLES
  python scripts/rrg_dashboard.py --dashboard sector1 --as-of 2024-06-19 --json-only
  python scripts/rrg_dashboard.py --dashboard Semi --date 2024-06-19
  python scripts/rrg_dashboard.py --as-of 2024-06-19 --json-only
  python scripts/rrg_dashboard.py --dashboard Mag7

Python API:
  paths = generate_rrg_dashboard(config, 'Mag7', as_of='2024-06-19', json_only=True)

The historical filename uses the requested cutoff (2024-06-19), while the
market_snapshot.analysis_date field in JSON contains the most recent valid
weekly observation at or before the cutoff (possibly earlier than 2024-06-19).
market_snapshot.requested_as_of_date makes the distinction explicit.
Historical filenames use {prefix}_{YYYY-MM-DD}_analysis_prompt.json and
{prefix}_{YYYY-MM-DD}.html, so they do not overwrite latest-data outputs.

The query and in-memory filtering use the inclusive historical window.
Historical runs require at least 12 valid RRG observations to compute the
12-week persistence statistics honestly. No output is written on such failure.

VALIDATION
  python -m pytest -q tests/test_rrg_dashboard_backdate.py

16 tests pass using a mocked Flight data source, synthetic weekly prices, and
an actual Plotly render. No integration test against your deployed Flight
server or the production weekly_close table was possible from the upload.

LIMITATION
An as-of cutoff prevents post-cutoff observation dates from reaching the
calculations. It does NOT reconstruct past raw vendor revisions, delisted
stocks excluded from today's ticker preset, split-adjustment knowledge, or
other point-in-time states not retained in weekly_close. If Date is a
week-start label for end-of-week closes, validate that representation before
using these outputs in historical research.

KNOWN EXISTING WARNING
The pandas DataFrame.pct_change() call from the original RRG math emits a
FutureWarning on some versions of pandas. That calculation is left unchanged
to preserve previous RRG calculation parity.
