# Native string workers: observation and complete delivery

Thirty frozen observations run fifteen declarations once with optional queue-wait
measurement disabled and once enabled. All complete values, schemas, source
identities, queue drains and final reservation refunds pass. Eight calls execute
partition work inline, sixteen use workers and six retain the existing
heavy-hitter worker decline. No capacity-blocked producer wait was observed;
no new finite feedback policy or performance gain is claimed.

The new delivery controls exercise real partition workers, retained output slices,
cancellation, consumer error, memory refusal, panic and once-only fanout to native
Vortex and JSONL. Credits remain until the last consumer owner releases them.
Panic preserves the existing poisoned-session contract and requires a fresh
session for retry. This is not a scheduler or runtime delivery change.

Focused checks, strict native lint and formatting pass. Complete native validation
uses the existing `ci-native` profile with checked arithmetic/debug assertions:
4,432 tests pass across 17 targets, with 41 intentionally ignored diagnostics.
The compiled unit-test inventory has 4,456 tests; all four CI partitions cover
that inventory exactly. Twenty inventory-helper tests pass. The earlier partial
unoptimized run remains failed/interrupted and receives no full-suite credit.

`observations.json.gz` preserves every original observation, fifteen guarded
check receipts/logs including five failed or interrupted attempts, exact measured
and final test source versions, protocols, source identities, cleanup receipts
and the explicit no-change feedback disposition. `validation.json` binds the
archive and records each outcome. Public-interface, installed-package and hosted
acceptance remain separate obligations.

Replay verifies every archived byte and reanalyzes the original complete results
in a new directory without executing native queries:

```sh
python3 replay.py --packet . --output /path/to/new-local-replay
```

Add `--repository /path/to/checkout` to require exact current test-source
identities. Old compiled binaries and benchmark payloads are not required for
replay. Measured engine totals are 0.109764999 seconds with wait measurement off
and 0.109081210 seconds with it on. These are single observations with common
capture overhead and uncontrolled cache state, not a stable overhead bound or
speedup claim.
