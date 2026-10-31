# Single-predicate masks: complete experiment rejected

The single-comparison and null-test mask candidate is **dropped**. One execution
of every original query passes all complete-value, native-execution, resource
and ownership checks, but the summed engine time is higher. The frozen rule
requires a lower complete total; selected improvements do not override it.

| Observation | Complete queries | Complete result rows | Engine seconds |
| --- | ---: | ---: | ---: |
| Accepted bounded-ordering baseline | 347 | 12,565 | 12.055132613 |
| Single-predicate-mask candidate | 347 | 12,565 | 12.199579628 |

The candidate adds 0.144447015 engine seconds, approximately 1.20%. This is one
observation of each runtime composition, not a statistical estimate or isolated
causal attribution. The accepted baseline remains 12.055132613 seconds; the
under-ten-second objective remains unmet. No Full43 or external comparator ran.

## Candidate and actual reach

The [frozen contract](../architecture/native-single-predicate-masks-2026-10-30.md)
extends the existing native predicate program to one I64 column/literal
comparison in either order and direct I64 IS NULL/IS NOT NULL. It reuses the
current numeric validation owner and credited Mask/BitBuffer representation.
It retains projected three-valued logic and TRUE-only filtering. Other widths,
column comparisons, null literals and fallible/lazy expressions still decline.

Ordinary simple filters already push into the admitted Vortex scan provider.
The candidate activates above a computed projection or for projected Boolean
expressions; it does not replace that provider. Debug-only observations show
one numeric input preparation and a 9-byte mask for 65 rows, with no generic
Boolean selection pass. Six streamed consumer controls also prove activation.
This removed work did not produce a lower complete-cohort observation.

## Correctness and preserved failures

The candidate passes strict default and full-native workspace lint, formatting,
6,581 checked native tests with 35 existing manual ignores, and 3,290 default
tests. Public acceptance covers 1,305 complete comparisons and 779,070 rows over
three sizes, three formats and five interfaces. The isolated installed package
passes 284 tests without skips, followed by uninstall verification.

All failed attempts are retained:

- `m002`: two incorrect activation/producer-bound fixture assertions. Corrected
  controls preserve scan pushdown and directly exercise the U16/usize boundary.
- `m018`: 36 existing SQL-parser refusals of literal-left comparison syntax;
  1,269 complete comparisons remain evidence. The correction uses the exact
  equivalent column-left form on four SQL-backed interfaces, preserving the
  typed literal-left case, independent references and byte-identical runtimes.
- `m024`: an omitted retained-path alias in the evidence collector, corrected
  using bytes already preserved. No query rerun was needed.
- `m026`: preflight found the new callback module absent from the original
  harness. It stopped before creating the cohort or running queries. The
  correction binds the exact accepted Python-source differences while retaining
  every original measured harness byte and unchanged observation/supervision.

The public control named `mask_decline_i32` uses the recorded I16 source schema;
private native tests separately cover I32 refusal. No SQL syntax or type domain
was added by the fixture correction.

## Observation and source disposition

The comparison keeps all 347 cases in original order, five original Parquet
inputs, independent references, the original native SQL harness, a 2-GiB budget
and parallelism two. Full hashes run before/after execution. The standard guard,
deadlines, active-output limits, complete-result checks and cleanup remain in
force. No other task work runs during queries; no cache manipulation, pacing or
repeat trials are added.

The callback unit is already merged in #1628. Its Python additions are not
exercised by this native SQL corpus. Both source versions are explicitly bound
in the protocol; the current native runtime includes that accepted composition.
The earlier reporting-prefix correction also leaves the executed harness intact.

Material per-case regressions are `projection_limit_temporal_31`,
`join_population_inner_count` and
`aggregate_metric_category_mixed_arithmetic_all`; none crosses the frozen
material-family threshold. These diagnostics neither change the binary rule
nor establish the cause of the total difference.

The six modified native source/test files are restored to the accepted source.
The two newly added candidate test files are removed. The packet retains all
eight exact candidate files, original accepted source bytes and the complete
rejected patch. Application callbacks and all previously accepted optimizations
remain intact. Grouped-cost attribution is the next investigation inside item 20.

## Portable verification

The [packet](evidence/native-single-predicate-masks-2026-10-30/manifest.json)
retains both complete public attempts, the full candidate and baseline query
outputs, references, source/runtime identities, failed and passing process
receipts, fixture correction, hosted application prerequisite and source
retirement proof. Large source/runtime payloads retain identities rather than
copies. Content hashes establish internal consistency, not signed provenance.

The verifier reads complete saved values without executing archived code or an
engine. Negative controls reject altered values, source identities, coverage,
operand equivalence, ownership, harness continuity and disposition. Replay with:

```sh
python3 -B scripts/verify_single_predicate_mask_packet.py docs/benchmarks/evidence/native-single-predicate-masks-2026-10-30
python3 -B -m unittest discover -s scripts -p test_single_predicate_mask_packet.py -v
```

The [acceptance record](evidence/native-single-predicate-masks-2026-10-30/acceptance.json)
contains the recomputed complete disposition. Replay executes no new query.

The [final verification record](evidence/native-single-predicate-masks-verification-2026-10-30/verification.json)
retains all 11 passing corruption controls, formatting, CI wiring and public
status checks. A source-only copy reproduces the complete acceptance and all
controls while forbidding original-data access, engine imports, subprocesses
and network access. The original mutation-test failure is also preserved: its
selected aggregate count row had no `row_id`; the corrected test mutates that
row's existing integer value. No verifier, engine, reference or observation
changes for this correction.
