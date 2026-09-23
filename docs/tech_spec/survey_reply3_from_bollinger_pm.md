| Their question | Our answer |
| --- | --- |
| 1. PKs vs create_as | The mix — create_table + insert_select for PK-carrying tables (week_calendar, weekly_bars, bb_events, bb_outcomes+flags, bb_features, run_config); constraint-free create_as/do_put for audit sinks and the view. PK violations should fail loudly at insert. Flagged one gap: persistent tables need rebuild semantics — asked for insert_select mode: append\\|replace or a truncate op, else every rerun doubles the table |
| 2. r4_class | Accepted value_text VARCHAR — categorical outcomes go there, numerics stay in value. Shape confirmed |
| 3. Column names | Reply DDL is canonical — verified our WIP store.py already uses check_name/med_vol_ratio/volume_verdict; whatever they compared against was stale |
| 4. Denylist | No impact; acknowledged it also closes the exfil-via-SELECT path |