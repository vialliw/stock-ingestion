# SEV3 incident — stale

- Incident key: `20261016_sev3_stale_WBD`
- Severity: SEV3
- Check: stale
- State: open
- First seen: 2026-10-16T21:20:32+01:00
- Last seen: 2026-10-16T21:20:32+01:00
- Unacknowledged checks: 1

## Summary

WBD stale 8 sessions vs latest session 2026-10-15 (last data 2026-10-05) — suspected delisting/halt

## Suggested action

Suspected delisting/corporate action: verify ticker status (stale >=5 sessions with no integrity error).  If confirmed delisted, add the ticker to acknowledged_delisted in _config/suppressions.json to downgrade to SEV4.

## Evidence

See `report.json` (verbatim health-JSON fragment).

## Severity history

  - none
