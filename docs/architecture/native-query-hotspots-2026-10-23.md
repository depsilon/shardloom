<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native query hotspot candidates

Status: H1 dropped; H2 finite candidate contract; no performance acceptance.
The maintainer's October 23 instruction prioritizes measured slow queries over
the earlier general structural candidates. Those candidates remain preserved on
their branch. This work is under PERF-01/03/04/05/06; it does not reopen rejected
region, sort, dictionary-capacity or full-view reconstruction mechanisms, or
close any CG gate.

The merged `06653efb` baseline completes one isolated Full43 pass with all 43
complete results validated and 52.823293216 seconds of engine execution.
Q19 contributes 4.715848959 seconds; Q23 contributes 3.283155918 seconds.
Diagnostic samples of the frozen merged executable validate both results but
receive no timing credit. Q19 has substantial serial preparation while its
existing triple-key workers wait. Q23 spends 2.700821135 seconds constructing
accessors in the unprofiled pass. Its sampled caller decodes compressed text
again after native predicate evaluation.

## H1: reuse one projected text conjunct

Provider decision: wrap the existing Vortex-native provider. Vortex 0.85's
`vendor/vortex-layout/src/scan/tasks.rs` owns pruning, conjunction ordering,
selectivity feedback, sparse masks and early empty-selection return. The
existing `local_primitives/text_scan_reuse.rs` owns a weak registry whose entries
are held by projection futures. It shares one decoded Zstd text comparison with
projection but admits no conjunction or contains predicate. Extend that owner;
do not replace Vortex filter execution, decode another column early, add a cache
across queries, or change codec/provider scheduling.

Choose the first projected UTF-8 comparison or contains leaf in declaration
order, descending through AND only. Reuse only when Vortex reaches that exact
bound leaf with an all-true input mask. Other predicates, sparse input masks,
pruning and the provider's dynamic ordering keep their current behavior. Retain
the original prepared column until projection; apply the final mask after all
conjuncts. Validate that it is a subset of the mask produced by the reused leaf.
An encoded dictionary remains encoded. Metadata and buffers keep existing query
leases and active-range lifetime. Cancellation/error/empty selections must
release every owner. No new unsafe code, dependencies or public syntax.

The expert comparator is a native columnar-engine maintainer checking exact
predicate/null semantics, lazy failure order, row-mask alignment, encoded paths,
retained buffer ownership and complete-operation benefit.

Before measurement, prove decoded-reference equivalence for conjunctions with
additional numeric and text restrictions, reversed declaration order, nested
AND, nullable/Unicode text, positive/negative contains and repeated predicates.
Test late empty selections, sparse input bypass, allocation refusal, source
errors, cancellation and release. A real provider-allocation regression must
show that the reused column is decoded once for filter plus projection.

Freeze both executable hashes, source manifests, build receipts, full query and
reference hashes and the existing input generation. The baseline is the frozen
merged executable used above. Use complete public CLI queries with 24 GiB and
the same input. Run five alternating baseline/candidate pairs of Q23 at
parallelism 12, then three pairs each of Q23 at parallelism 1 and Q22/Q19/Q29 at
parallelism 12. These are targeted comparisons authorized by the optimization
request, separate from routine Full43. Preserve every observation and failure.

Retain only if complete results/schemas and no-fallback evidence agree, the
target engine median and median paired saving improve, and at least four of
five target pairs improve. Each control engine median may rise by at most the
greater of 5% or 5 ms. Target median CPU may rise by at most 5%; each case's median
peak RSS may rise by at most the greater of 25% or 256 MiB. Failed gates drop the
candidate. Do not change thresholds after seeing results. No cache manipulation,
artificial pacing, concurrent builds, profiling or other task work during
measurement. Preserve storage/deadline/process cleanup guards; full input
hashing and recursive inventories stay outside active query execution.

Run focused tests before comparison, required workspace/native checks for
retained code, and one isolated standard Full43 after each retained optimization.
Later candidates require their own finite contract and evidence. The earlier
merged Full43 observation remains the baseline, not a candidate result.

### H1 decision

All 34 complete comparisons pass value/schema and resource checks. The target
engine median rises from 3.424686625 to 3.570875665 seconds, with only one of five
positive paired savings. H1 fails its frozen engine gate and is removed. Its
source remains in `5bc89827`; all raw comparisons and the decision remain in
the local `hotspots-1011/conjunct-comparison-1012` packet. The public Q23 path
restores provider drivers after declining aggregate workers; it does not supply
the worker-memory argument required by text reuse admission. No performance
claim or Full43 acceptance follows from H1's passing correctness tests.

## H2: prepare selected text on native scan tasks

Provider decision: use the existing Vortex `ScanBuilder::map` surface
(`vendor/vortex-layout/src/scan/scan_builder.rs`). Its mapper runs inside the
already spawned split task, after the complete native filter and projection
(`vendor/vortex-layout/src/scan/tasks.rs`). This moves the existing canonical
UTF-8 conversion from the serial accessor constructor into those tasks. It
does not create threads, change the CPU grant, change scan concurrency, reorder
chunks, decode before the final filter, change filters, or cache query results.

Admit only filtered grouped aggregate scans without residual predicates,
winner refinement, source-order group admission, or dedicated aggregate workers,
with more than one admitted CPU lane. Require a nonnullable struct projection
containing UTF-8. Leave native Dict and already canonical text fields unchanged;
preserve other fields, field order, dtype, validity and row count. Use the scan's
same admitted session, native allocator and provider registry. Aggregate state,
dictionary construction and updates remain on the existing caller. Record
relocated provider work separately from the existing caller-only timing fields.

The existing bounded scan task window and native buffer leases govern retained
data. Charge replacement struct metadata before allocating it; an unavailable
metadata lease declines preparation before decoding. Do not claim an RSS bound
or full provider allocator coverage. Propagate decode/source errors unchanged;
check cancellation around preparation and between fields; release all prepared
owners on completion, cancellation and errors. No new provider retry or external
execution is allowed. This is materialization after selection, not zero-decode.

Before measurement, test exact text values and nulls, dictionary and numeric
preservation, column order, final selection, multiple ordered chunks, empty
selections, source errors, allocation refusal, cancellation and lease release.
Verify through the real aggregate consumer and expose positive activation
evidence through public CLI summary fields. The comparator is a native engine
maintainer reviewing selection/order, provider context, ownership, failure and
complete-query benefit.

Use the same frozen merged binary/input/reference and complete-query protocol
as H1: five alternating Q23/parallelism-12 pairs and three pairs each of
Q23/parallelism-1, Q22/12, Q19/12 and Q29/12. H2 independently uses the same
predeclared engine, paired-win, CPU and RSS gates. Require positive preparation
activation on every target candidate call; a gate failure drops H2 without
threshold changes. Record immutable identities, every observation and failures;
keep other task work idle and hashing outside execution. Required workspace and
native checks plus one isolated standard Full43 follow only if H2 is retained.

The maintainer subsequently requested a fresh Full43 observation before work on
the attached shared-operator feedback. H2 therefore receives one isolated
Full43 after its focused correctness and native lint checks, before the paired
benefit decision. That observation does not replace or relax H2's acceptance
gates. The feedback's composed-operator changes require their own composed
workloads; primitive Full43 remains regression evidence for those changes.

### H2 decision

The requested isolated Full43 at `a676e093` validates all 43 queries and all 381
result rows, with 51.372338870 seconds of engine execution. Q23 reports positive
scan preparation. Full input and source identities remain unchanged and the
process group is drained. The first preflight attempt executed no queries: its
own Python imports created bytecode in the frozen harness, which the inventory
correctly rejected. That failure and the exact task-owned bytecode cleanup are
preserved before the successful `-B` invocation.

The subsequent frozen 34-call comparison passes every complete value/schema
check. Q23 at parallelism 12 improves in all five pairs; median engine execution
falls from 3.011960000 to 2.154102708 seconds. All engine controls and peak-memory
gates pass. The target CPU gate fails: median CPU use increases from 12.408428 to
14.076489 seconds, exceeding the frozen 5% allowance. H2 is therefore dropped;
its source remains in `a676e093`. The Full43 observation remains evidence of that
candidate, not a retained-runtime result. Do not relax the threshold or discard
the failed resource decision because elapsed engine execution improved.

Complete observations, identities, activation evidence and decisions remain in
`native-engine-optimization-20261023/hotspots-1011/scan-text-comparison-1013` in
the local evidence root. The Full43 receipt is
`native-engine-optimization-20261023/full43-scan-text-run-1014.json`. The next
work follows the maintainer's attached shared-operator feedback: reconcile the
preserved value-only preparation, measure reuse, sort-location and predicate
candidates before extending finite-demand sorting, expression preparation or
reusable aggregate consumption. These composed paths need composed-workload
acceptance, not a benefit claim inferred from primitive Full43.
