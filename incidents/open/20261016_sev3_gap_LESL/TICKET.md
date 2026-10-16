# SEV3 incident — gap

- Incident key: `20261016_sev3_gap_LESL`
- Severity: SEV3
- Check: gap
- State: open
- First seen: 2026-10-16T21:20:32+01:00
- Last seen: 2026-10-16T21:20:32+01:00
- Unacknowledged checks: 1

## Summary

LESL missing 1 session(s) in window 2026-09-03..2026-10-15

## Suggested action

Re-fetch the missing window — idempotent MERGE-upsert: daily_load {"tickers": ["LESL"]} or on_request_load for the missing date range.

## Evidence

See `report.json` (verbatim health-JSON fragment).

## Severity history

  - none
