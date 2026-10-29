# Explicit decimal output acceptance — October 29, 2026

The explicit decimal unit of active queue item 20 adds caller-selected output
precision, scale and rounding to shared native scalar arithmetic, additive
Decimal128 aggregates and continuous quantiles, including admitted windows.
Existing declarations preserve their defaults. This is capability and ownership
acceptance; no benchmark was run and no performance benefit is claimed.

The [implementation contract](../architecture/native-decimal-output-2026-10-29.md)
defines the domain and provider decision. The
[public aggregate reference](../reference/native-analytical-aggregates.md) shows
SQL, DataFrame and typed Python declarations. Native Vortex arrays and existing
total, rank, distinct, grouping, window and spill owners remain the execution
foundation. No dependency, external execution provider or fallback is added.

## Complete-value acceptance

| Evidence | Result |
| --- | --- |
| Independent core reference | 3,462 deterministic integer/Fraction/Decimal cases covering all six modes, signs, ties, subnormal quantile probabilities, wide intermediate totals, cancellation and final overflow. |
| Public interfaces | Six tests, 250 executions and 2,695 complete rows through DataFrame, typed relation, native SQL, embedded workflow and CLI; no skips. Complete expected/actual types and values and all five reports are retained. |
| Persistence and ownership | Vortex, Parquet and Arrow IPC readback after source deletion; retained output after producer close and reuse as a new native source. |
| Streamed inputs | Collect, bounded batches, native write, empty input, late producer error and zero-limit late error; complete-input/close checks, no partial publication, plus lazy decimal division under conditional selection. |
| Composition | Complete three-row `explicit_decimal_output_workflow` fixture combines decimal arithmetic, grouped rounding, DISTINCT, median interpolation and cumulative windows. |
| Documentation and declarations | All three literal decimal examples execute with complete expected values and types; 14 source checks include 131 tests without skips and exact regeneration of the oracle data. |
| Default workspace | 3,285 tests pass without ignores; formatting and strict lint pass. |
| Native workspace | Full attempt: 6,460 pass, one stale conformance-count fixture fails, 35 existing opt-in tests ignored. The two expected count strings are corrected; all 58 tests in that native metadata target then pass. Strict native lint and the minimal native feature check pass. |
| Installed package | The exact frozen CLI/library is packaged, checked, installed into an isolated environment, imported and used by 223 integration tests without skips; uninstall verification passes. No publication occurs. |

The independent oracle covers exact, half-even, half-away, toward-zero, floor
and ceiling. Rounding uses the final rational value, including the original
Float64 probability ratio for interpolation. It does not round endpoints,
partial sums, spill records or removed window values. An exact widened integer
sum is requested through explicit `CAST(... AS DECIMAL(38,0))`; ordinary integer
SUM and floating AVG remain unchanged.

## Memory and failure observations

Both pressure cases compare every value against an ample-memory run. A 16 MiB
resident-only execution is denied, while explicit native spill completes under
the same grant. The grouped case produces 8,005 rows from 24,013 input rows with
620 runs and 309 merge passes; the window case produces all 24,013 rows with 260
runs and 124 merge passes. Peak reservations are 7,179,064 and 10,426,024 bytes,
and peak spill sizes are 227,257,044 and 112,818,964 bytes respectively.

These resource observations come from attempt `n019`, where both complete-value
pressure comparisons passed. Its separate failed-consumer fixture was denied
before reaching the callback because it requested the default output batch.
The fixture now uses the same bounded 127-row batches as successful delivery;
the final native suite passes its quota, cancellation, failed-consumer, cleanup
and retry checks. No resource limit or runtime behavior was changed for that
test correction.

## Durable evidence and remaining work

The [portable evidence packet](evidence/native-decimal-output-2026-10-29.json.xz)
retains numbered attempts and logs, independent references, complete public
comparisons and execution reports, conformance results, source snapshots,
build/product identities and installed-wheel proof. Its embedded verifier
replays source/product linkage, complete values, oracle arithmetic and failure
dispositions without invoking another execution engine. Corruption controls
exercise members, source links, result values/types, certificates, counts,
conformance output, oracle data and wheel identity.

Original compilation, lint, fixture and pressure failures remain recorded.
The full native attempt's document-count failure is represented as failed and
linked to its successful corrected-target rerun; it is not rewritten as a
wholly successful original run. Hosted acceptance is recorded separately when
available. The retained 347-query checkpoint is unchanged at **15.304317834
engine seconds**; this work supplies no new timing observation.

Non-finite/signed-zero policy, remaining composition and recursive execution,
application batch functions, diagnostics and combined item-20 acceptance remain
open. This unit does not close a competitive gate or authorize a release.
