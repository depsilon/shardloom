# Current native workload comparisons — October 22, 2026

The published 0.7.0 runtime passes all **704 complete result comparisons** against
an independently executed pandas 3.0.3 reference for the 22 declared traditional
analytics tasks. This closes the current-source evidence gap left by the public
format-strategy correction. It accepts a finite correctness matrix; it does not
complete PERF-12, the fifteen protected public contracts, or CG-5/CG-6.

The machine-readable
[workload gap matrix](evidence/perf12-comparisons-2026-10-22/perf-12-acceptance.json)
records each task's SQL, reference implementation, versions, semantics, resources,
32 accepted comparisons and remaining guarantee reviews. The
[phase plan](../architecture/phased-execution-plan.md) remains the only active
queue. No optional operator, type, platform or destination is promoted here.

## What ran

| Dimension | Accepted scope |
| --- | --- |
| Native source | `7ae9f0e44ddf3c8f09520fe318852cd873db3817`; published 0.7.0 executable, SHA-256 `eb1af7ec23426f58c3cb0f748adfa379e14ae8d788492a790811de8aca12275d` |
| Harness source | `f1c445eb8314b361ae9d920fce47f8d63c9615eb`; frozen before execution |
| Independent reference | pandas 3.0.3 on Python 3.12.14; full dependency versions and install receipt retained |
| Host | macOS 27.0, arm64, ten logical CPUs, 16 GiB RAM |
| Data | 1,000 fact rows, twenty dimension rows, eight fact parts, the declared `tiny_smoke` fixtures |
| Input to collect | CSV, JSON, JSONL, Vortex, Parquet, Arrow IPC, Avro and ORC; raw and prepared |
| Input to writers | CSV to the same eight explicit destinations; raw and prepared |
| Native resource request | 1 GiB and two lanes; existing deadlines, storage guards, sampled process guard and cleanup retained |
| Coverage | Eighteen accepted cohorts, 32 cells per task, 704 native and 704 reference records; one execution per accepted cell |

This is a deliberately bounded matrix, not the complete input/output Cartesian
product. Pandas reads the original logical CSV fixture for a Vortex comparison;
it does not read candidate output or execute ShardLoom work. Native Vortex remains
the persistence target; compatibility files remain explicit translation outputs.
Every accepted native request carries `fallback_attempted=false` and
`external_engine_invoked=false` through the existing public workflow contract.

The reference process completes before its candidate workload. No query-answer
cache is admitted. The operating-system page cache is uncontrolled, and fixture
preparation and hashing can warm it. Native credits do not imply a whole-process
RSS ceiling. These observations do not establish comparative engine performance.

The original environment preflight records nine installed distributions. The
frozen protocol also enumerates ShardLoom 0.7.0 metadata after adding the source
tree to the import path. The original post-run verifier checks exact equality
with all ten declared distributions, but its receipt stores only that outcome.
A separately dated
[metadata-only observation](evidence/perf12-comparisons-2026-10-22/baseline-environment-postflight.json)
now retains the complete mapping
and distinguishes the nine installed packages from source-tree metadata. Replay
requires equality for both complete mappings and rejects a missing distribution.
This supplemental observation executes neither engine and is explicitly separate
from the original run; the original packet remains unchanged.

## Results and semantic limits

All 22 declared tasks pass their 32 complete normalized result comparisons:

| Declared task | Native collected statements with checked logical schema | Explicit terminal order checked |
| --- | ---: | ---: |
| csv/file ingest | 16 | 0 |
| selective filter | 16 | 0 |
| group by aggregation | 16 | 0 |
| sort and top-k | 16 | 16 |
| hash join | 16 | 0 |
| wide projection | 16 | 0 |
| distinct count | 16 | 0 |
| filter + projection + limit | 16 | 0 |
| multi-key group by | 16 | 0 |
| join + aggregate | 16 | 0 |
| row number window | 16 | 0 |
| partition pruning | 16 | 0 |
| many-small-files scan | 16 | 0 |
| null-heavy aggregate | 16 | 0 |
| high-cardinality string group/distinct | 32 | 16 |
| top-N per group | 16 | 0 |
| clean/cast/filter/write | 16 | 0 |
| malformed timestamp / dirty CSV | 16 | 0 |
| small change over large base | 16 | 0 |
| nested JSON field scan | 16 | 0 |
| scale stress skewed join aggregation | 16 | 0 |
| scale stress multi-stage etl | 16 | 16 |

Task names retain the existing declarations. In particular, “scale stress” names
do not turn the 1,000-row fixture into a large-scale acceptance, and “csv/file
ingest” counts and sums a supplied file rather than measuring durable ingestion.

The original comparator rounds relevant floating values to four decimal places,
maps absent/NaN aggregate sums to zero, and canonicalizes selected grouped,
Top-K and rank results. Complete normalized equality therefore cannot establish
raw precision, NULL fidelity or original SQL ordering by itself.

A separate replay of the **original native envelopes** checks all 368 collected
statements in the two collect cohorts: exact column names and logical numeric or
string types, value domains, well-formed nullability flags and value compatibility,
complete-payload flags and agreement between duplicated envelope fields. It also
checks all 48 statements
whose declarations request terminal order. Window ordering does not imply an
outer result order, so no such claim is made for the rank tasks.

SQL-derived nullability inference is not independently certified by that check.
No NULL value occurs in these collected outputs, including the null-heavy
aggregate's final result. Empty/all-null behavior, exact floating evaluation,
writer schema/order fidelity and pressure/failure guarantees therefore retain
their separate regression obligations. Pandas records do not retain a logical
schema, so this packet does not claim equal logical schemas across engines.
Writer results retain their original complete-value readback checks.

## Interruption and retained evidence

The first seventeen cohorts completed. The original eighteenth cohort stopped
when retained logs exceeded the unchanged 192-MiB log budget. Its failed report
and partial records remain in the packet and receive **no acceptance credit**.
Completed logs were compacted and verified; then the entire eighteenth cohort
ran once with the same executable, baseline, workload and limits. The accepted
matrix consists of the original complete cohorts 1–17 and that complete
replacement. This was not an uninterrupted run or a selectively repaired case.

The [portable packet](evidence/perf12-comparisons-2026-10-22/perf12-packet-929.json.xz)
contains 11,811 original evidence members and 1,335 omitted physical-payload
identities. It retains protocols, sources, environment/build bindings, complete
results, original per-call envelopes, preparation/readback receipts, failures
and continuation records. Replay verifies stored bytes and identities without
executing either engine or reopening omitted physical payloads. Its original
absolute paths are lookup keys within the packet, not local dependencies.

The prior complete comparison remains historical pandas **2.2.3** evidence in
the [cyclic-existence packet](evidence/cyclic-existence-2026-10-21/acceptance.json).
It is not relabelled as this current 3.0.3 run. The original
[Full43 acceptance](../architecture/native-format-strategy-parity-2026-10-21.md)
also remains unchanged: **48.754238077 engine seconds**, 43 queries, one execution
each, 381 result rows. It uses retained native regression values; an independent
full-size oracle and paired performance comparison remain unproven.

## Verification and next decision

The subsequent [selected guarantee review](native-workload-guarantees-2026-10-22.md)
completes the four named follow-up reviews with source-bound existing regressions
and an explicit missing-independent-Full43 disposition. The matrix observations
and limits below remain unchanged. This finite review advances the shared
prerequisite stage without closing all required owner contracts or CG-5/CG-6.

The complete portable replay passes. Forty corruption cases reject altered
identities, incomplete coverage, changed baseline/resources/SQL, fallback,
missing writer readbacks, an erased interruption, incomplete dependency mappings
and unsupported claims. Five
additional original-envelope cases reject reversed ordered rows, a changed
logical type, a missing row field, an incomplete flag and duplicate fields.
Five acceptance-summary checks also reject missing comparisons, optional-scope
promotion and false schema, independent-Full43 or historical-version claims.

From the repository root, set `evidence` to
`docs/benchmarks/evidence/perf12-comparisons-2026-10-22` and run the following with
Python 3.10 or later. No engine or baseline dependency is needed for replay.
Each output path must be new:

```sh
evidence=docs/benchmarks/evidence/perf12-comparisons-2026-10-22
python3 "$evidence/verify_packet_929.py" --repository . --manifest "$evidence/perf12-packet-929.json.xz.manifest.json" --output /tmp/perf12-replay.json
python3 "$evidence/check_packet_corruptions_930.py" --repository . --manifest "$evidence/perf12-packet-929.json.xz.manifest.json" --output /tmp/perf12-corruptions.json
python3 "$evidence/verify_collected_payloads.py" --repository . --manifest "$evidence/perf12-packet-929.json.xz.manifest.json" --output /tmp/perf12-payloads.json
python3 "$evidence/verify_acceptance.py" --repository . --acceptance "$evidence/perf-12-acceptance.json" --output /tmp/perf12-acceptance-check.json
```

The original next decision was a source-bound review of existing semantic,
resource, lifecycle and installed-support guarantees. The subsequent
[guarantee acceptance](native-workload-guarantees-2026-10-22.md) completes those
four reviews. The acceptance JSON preserves their original owner, evidence and
admission contracts under `reviewed_action_contracts` and links the disposition.
The independent Full43 reference gap remains explicit.

The fifteen required contracts, five scheduled rows and 38 optional rows keep
their scope. The next shared prerequisite stage covers region mechanisms,
exact local reduction, preparation costs and instruction attribution. Dependent
investigations retain their admission gates. No release, performance superiority,
production certification or new execution provider is admitted by this report.
