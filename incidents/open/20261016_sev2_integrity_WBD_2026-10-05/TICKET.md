# SEV2 incident — integrity

- Incident key: `20261016_sev2_integrity_WBD_2026-10-05`
- Severity: SEV2
- Check: integrity
- State: open
- First seen: 2026-10-16T21:20:32+01:00
- Last seen: 2026-10-16T21:20:32+01:00
- Unacknowledged checks: 1

## Summary

WBD 2026-10-05: null_or_nonpositive_ohlc — recent corrupt row accepted by the loader

## Suggested action

Targeted re-fetch for that ticker/date via on_request_load — the loader accepted a bad row.

## Evidence

See `report.json` (verbatim health-JSON fragment).

## Severity history

  - none
