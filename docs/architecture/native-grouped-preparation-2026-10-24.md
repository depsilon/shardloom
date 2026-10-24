<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native grouped preparation campaign

Status: G1 retained; G2 finite candidate contract, without performance acceptance.

The October 24 maintainer feedback selects compound grouping first, text
preparation and grouped measures second, and exact URL grouping third. This is
a finite PERF-01/03/04/05/06 investigation. The broader queue remains paused;
CG-1 through CG-23 remain independent gates. H1 and H2 retain their original
rejections, including H2's failed CPU gate. Do not rerun either unchanged.

PR #1594's reviewed head is `e1af0d685e3d5d6c2dbb1e816b89f4320db2bfd9`.
All 44 hosted checks pass. The requested isolated Full43 records
**48.693636581 engine seconds**, one execution of each query, with all 381
independent reference rows matching. It uses the same executable as the earlier
51.333297036-second observation; their difference is not a code improvement.
The seven selected queries contribute 19.280 engine seconds. Comparisons with
the earlier 52.823-second campaign baseline are arithmetic, not attribution.

## Source findings and order

- Q17 already prepares its numeric/text inputs and reduces complete keys in
  `compound_count_workers.rs` jobs. Q19's `triple_count_workers.rs` prepares
  accessors, binds chunk dictionary values to the producer's string domain,
  and routes every complete key before submitting only partition updates.
  Both use the existing bounded `AggregateChunkJobs` and admitted CPU grant.
- Q19's current diagnostic counters attribute about 1.988 seconds to caller
  accessors, 0.949 to caller dictionary binding and 0.889 to caller routing and
  submission. These are diagnostic stage observations, not independent CPU
  measurements or predicted savings. Q19's distribution differs from Q17;
  their engine-time difference is not the cost of minute extraction alone.
- Q23's predicate/projection decoding opportunity remains credible after H2's
  rejection. A later candidate must reduce additional work or establish a
  different reuse mechanism while preserving selection and lazy failures.
- Q28 and Q29 enter through the relational route after the explicit
  `OCTET_LENGTH` benchmark translation. Their recorded physical policy is
  `transformed_dictionary_aggregate`; provider lowering invokes the shared
  prepared aggregate. The facade's aggregate-present flag is not a physical
  strategy. The relational wrapper currently omits the detailed accessor and
  group-update counters, so those costs remain unknown. Byte-length semantics
  and the unchanged independent reference remain authoritative.
- Q34/Q35 remain exact string-count and ranked-selection controls. No defect
  or optimization is inferred from their engine times alone.

## G1: route bound complete keys in existing count jobs

Provider decision: reuse the admitted Vortex `PrimitiveArray` owners and
native/chunk dictionary identities, then move the existing exact typed
histogram/scatter into the same ShardLoom count job that updates partitions.
The local Vortex provider inventory and `native_numeric_owner.rs` already
provide original-width integer slices and lifetime ownership. This is a
ShardLoom complete-key ownership/scheduling change, not a replacement scan,
encoding, decoder, provider, or query-engine integration.

Keep accessor construction and ordered global string binding on the producer.
Transfer the two numeric owners, dictionary row codes and already bound IDs to
the existing job without copying numeric payloads or retaining chunk text
values after binding. Preserve one routed complete-key buffer, exact byte
equality, signedness, raw/prepared minute semantics, cross-chunk dictionary
identity, tie order and complete final selection. Non-typed inputs retain the
existing admitted general path. No new workers, source pass, dictionary lock,
query cache, spill, fallback or public syntax is introduced. Q17 is the
companion control for this existing worker-preparation pattern.

Reserve transferred adapter capacities and native numeric `nbytes` estimates
alongside the routed-key capacity before submission. Keep that ownership until
the job finishes; window permits cover queued, active and completed jobs.
Distinguish key-buffer bytes from retained input estimates and provider-owned
allocations. These reservations do not bound RSS or every upstream allocation.
Cancellation, invalid codes/minutes and capacity denial must cancel/drain and
fail without installing partial results or replaying committed input. Worker
routing diagnostics must be separate from caller routing/submission and must
not be added to engine elapsed as disjoint time.

The expert comparator is a native columnar-engine maintainer reviewing exact
key identity, asynchronous input lifetime, bounded queue ownership, error
propagation and complete-query resource-adjusted benefit. Before measurement,
prove the existing raw/prepared, integer-width, dictionary-domain, Unicode,
tie/offset, empty, malformed-input, cancellation and pressure cases. Add a
deterministic delayed-worker test that drops source owners before routing and
checks full results and final lease release. Preserve all general-path tests.

### Frozen measurement and decision

Use the accepted PR #1594 executable, its recorded source/build receipt, the
unchanged 99,997,497-row Vortex input, canonical query file and independent
reference. Freeze both executable hashes and complete Rust/Cargo identities
before comparison. Use complete public CLI operations with 24 GiB admission.
Run five alternating baseline/candidate pairs of Q19 at parallelism 12, then
three pairs each of Q19 at parallelism 1 and Q17/Q23/Q28/Q29/Q34/Q35 at
parallelism 12. This is a separately declared 52-call targeted cohort, not a
routine Full43 pass. Preserve every observation and failure.

Retain only if complete values/schemas and no-fallback evidence agree, target
engine median and median paired saving improve, and at least four of five
target pairs improve. Each control engine median may rise by at most the
greater of 5% or 5 ms. Target median CPU may rise by at most 5%; each case's
median peak RSS may rise by at most the greater of 25% or 256 MiB. Require
positive worker-routing activation on every Q19 candidate call. A failed gate
drops G1 without changing thresholds or excluding an inconvenient observation.

Use standard storage, concurrency, deadline, result-validation and cleanup
guards. Hash inputs and inventory source/build/evidence before or after query
execution. Keep other task work, agents, builds, packaging and profiling idle
during measurement; do not manipulate caches or add pacing. Run focused tests
before comparison, required workspace and native checks for retained code, and
one isolated standard Full43 only after retention. Record CPU/resource details
in evidence; routine timing communication uses engine time only.

Later text-preparation candidates require their own finite source-backed
contracts. This candidate cannot establish a Q17 speedup, general SQL parity,
external-engine superiority, a CG gate, hosted integration or package release.

### G1 decision and complete acceptance

The frozen 52-call cohort passes every gate. Q19/12 improves in all five
pairs, with median engine execution changing from **4.370869207** to
**3.580011250 seconds**, an **18.094%** reduction. Median paired saving is
0.774900125 seconds. Worker routing activates on every candidate Q19 call.
The single-lane and six companion controls pass; their numerical changes are
not attributed to this implementation.

| Case | Baseline median engine seconds | G1 median engine seconds |
| --- | ---: | ---: |
| Q19, 12 lanes | 4.370869207 | 3.580011250 |
| Q19, 1 lane | 6.846754124 | 6.878928416 |
| Q17, 12 lanes | 2.482753667 | 2.450697417 |
| Q23, 12 lanes | 3.047823917 | 3.063272834 |
| Q28, 12 lanes | 2.629902374 | 2.346813249 |
| Q29, 12 lanes | 2.991511582 | 2.965427999 |
| Q34, 12 lanes | 1.969679501 | 1.967714126 |
| Q35, 12 lanes | 1.982756126 | 1.981574167 |

Formatting, default and release-native Clippy pass. Default workspace tests
pass 3,242 cases; release-native workspace tests pass 6,041, with 35 existing
ignored cases. The 20 focused triple-key cases include delayed-owner lifetime,
cancellation, pressure and asynchronous invalid-minute propagation.

One isolated standard Full43 at runtime commit `9ca3cbda` passes all 43 queries,
one execution each, and all 381 independent rows and accepted schemas. Engine
time is **48.718933086 seconds**. The prior 48.693636581-second single
observation is effectively level; this is not evidence of an overall suite
speedup. Q19 is 3.720030542 seconds in this pass; the paired cohort above is
the attribution evidence. The setup invocation through system Python 3.9
failed during imports before any query; the unchanged helper then ran under
its declared Python 3.13 interpreter. Both outcomes remain recorded.

The [portable packet](../benchmarks/evidence/native-grouped-preparation-2026-10-24/manifest.json)
retains complete raw results, source/build/input identities, every comparison,
resource gates, validation receipts and the per-query timing CSV. Its
[verifier](../benchmarks/evidence/native-grouped-preparation-2026-10-24/verify_packet.py)
replays 52 comparisons and Full43 without a binary or dataset. It does not
execute archived helper code. Historical gates and failed observations remain
unchanged.

The subsequent adversarial review finds one ownership edge outside Q19's
distinct-column shape: the integer grouping key and minute expression may
reference the same source column. Preserve a single transferred owner and let
both roles borrow it, charging its native estimate once. A regression test
covers narrow/signed-extreme/unsigned-extreme inputs at one and three lanes,
exact results after dropping the source and complete lease release. The
following G2 control cohort and final broad validation cover the corrected
retained runtime; the original G1 observations remain immutable.

## G2: one average with repeated count measures

Source finding: `sql_syntax.rs::rewrite_having_aggregate_predicate` appends a
separate COUNT for `HAVING COUNT(*)`. The shared provider preserves the declared
measures. Q28 therefore carries AVG, its output COUNT and its HAVING COUNT.
`compact_numeric_block.rs` admits exactly two measures and declines this shape,
even after existing embedded byte-length lowering provides a native numeric
owner. This is a source-confirmed dispatch opportunity, not a measured cause
of Q28's complete engine time. Do not infer decoding from the policy label.

Provider decision: extend the existing native-owner block recipe to one AVG
and one or more COUNT(*) measures in any declared order. Reuse Vortex
`PrimitiveArray` owners, validity and original-width numeric dispatch; keep
the existing two-measure loop intact. The wider recipe increments each count
state in order, evaluates AVG at its original position and updates the remaining
counts afterward. Preserve group lookup/admission, selected row order, NULLs,
signedness, integer accumulation, nonfinite failures and partial state when a
count overflows. No count deduplication, SQL normalization, new state family,
worker, allocation, source read, decode, materialization or external engine is
introduced. Multiple averages, COUNT(column), transformed measures and other
unsupported shapes remain on the existing native consumer.

This extends the shared numerical consumer used by primitive and composed
providers. The Vortex-first review finds no different scan or kernel provider
needed: the remaining operation is ShardLoom's exact ordered aggregate-state
update. PulseWeave/capillary scheduling and admission are unchanged. Metadata
avoidance and original-width execution already apply. Existing block-activation
counters and complete result checks supply evidence; no new public field or
per-row logging is needed. The expert comparator checks semantic identity and
complete-operation benefit, not a synthetic loop speedup.

Before measurement, compare the wider block with the existing row updater over
all numeric widths, nullable/empty/sliced input, nullable integer keys, every
AVG position among repeated counts, reordered/duplicated selected rows,
cross-chunk width changes, source-order limits, HAVING and ordered output. Seed
each count/AVG count at overflow and use NaN/infinity/nonfinite sums to compare
the exact error and partial state. Reject unsupported recipes before updates.

Freeze the G1 executable and its successful build/source receipt as baseline.
Use the unchanged input, reference and public runner at 24 GiB. Run five
alternating Q28/12 pairs, then three Q28/1 pairs and three pairs each of
Q17/Q19/Q23/Q29/Q34/Q35 at 12 lanes: **52 calls**. Retain only when complete
values/schemas/no-fallback agree, the target median and paired median saving
improve and at least four of five target pairs improve. Every control permits
at most max(5%, 5 ms) engine regression; target CPU permits at most 5%; each
case's median RSS permits max(25%, 256 MiB). Use focused direct-consumer tests
for positive wider-block activation because the current relational result
wrapper does not retain that counter. Preserve all observations and drop on
any failed gate, without an unchanged rerun or revised threshold.

The existing G1 cohort machinery, storage/deadline/cleanup guards and isolated
measurement rules apply. Required broad validation and one isolated Full43
follow only if retained. This candidate does not change provider selection,
frontends or timing boundaries. The later user-surface parity pass retains
those separate requirements.
