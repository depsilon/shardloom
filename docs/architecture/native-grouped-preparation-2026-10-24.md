<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native grouped preparation campaign

Status: G1 finite candidate contract; no performance acceptance.

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
- Q28 and Q29 now follow the composed relational route after the explicit
  `OCTET_LENGTH` benchmark translation. Inspect those consumers before assuming
  primitive-aggregate preparation explains their cost. Byte-length semantics
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
