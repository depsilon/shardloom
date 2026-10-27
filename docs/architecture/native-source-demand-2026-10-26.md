<!-- SPDX-License-Identifier: Apache-2.0 -->

# Universal source demand: Parquet first

**Status:** implemented and locally accepted; not yet merged or released.
The [acceptance report](../benchmarks/native-source-demand-2026-10-26.md)
records the complete comparisons, final runtime, failures and bounded claims.

## Scope and prerequisite

This is the source-demand work unit under item 20 and the
[broad query program](native-broad-query-program-2026-10-26.md). The maintainer
requested one shared demand contract, with Parquet as the first physical
adapter. The foundation and its three review corrections are merged in
[PR #1611](https://github.com/depsilon/shardloom/pull/1611) as `4d25a06e` after
all 57 checks pass at `a9cbcb67`. The merge tree is identical to that head;
[integration evidence](../benchmarks/evidence/native-broad-query-review-2026-10-26/pr1611-integration.json)
records the completed workflows. Earlier benchmark observations keep their
original runtime attribution. No benchmark was rerun for the merge.

The outcome to prove is an ordinary eligible local Parquet query that binds its
schema before reading data, requests only needed columns and eligible row
groups, and feeds owned Vortex arrays to existing native operators without
creating a complete prepared input artifact. SQL, embedded workflow, Python
SQL, DataFrame and typed relations must use the same implementation.

This does not accept the unfinished item-18 branch, add an external execution
engine, publish a package, or close item 20 or CG-1 through CG-23. Incremental
column/region persistence, background preparation, object-store and connector
expansion remain outside this first work unit.

## Vortex-first provider decision and reuse map

Decision: **use_vortex_native_provider** for arrays and existing execution.
Pinned Vortex `0.90.0` supplies DType, owned arrays, Arrow interoperability,
expressions, scan projection/filter/order/selection concepts and native output.
Its scan `DataSource` is a provider concept, not permission to replace
ShardLoom's admitted source, replay, spill, cancellation and certificate owners.
Parquet `59.3.1` supplies metadata and column/row-group decoding only. It never
evaluates a relational query or supplies external residual execution.

| Responsibility | Existing owner and extension |
| --- | --- |
| Source identity | Retain `SourceIdentity` from metadata admission through execution; compare held descriptor and path generations before reads and after the final consumer. |
| Planning demand | Extend the common bound native scan path, reusing `ProjectionRequest`, `PredicateExpr`, shared backward column pruning and the existing typed scan-predicate converter. |
| Format declaration | Add an owned local-format descriptor to existing batch-source declarations; it owns metadata and an execution reader, not a second logical plan. |
| Physical read policy | Parquet projection masks and selected row groups operate on positional file reads. Do not load the complete encoded file and then project it. |
| Pruning proof | Translate only admitted exact statistics into existing `SegmentStats`/`prove_predicate_from_stats`; unknown or incompatible facts retain the region. |
| I/O resources | Reuse the resident operation's request/byte admission, cancellation token, memory grants, reader ownership and final I/O drain. |
| Intake and operators | Reuse `ResidentMemorySource` Arrow intake, existing native batch ownership, replay and relational operators, including their current spill policies. |
| Public syntax | Converge ordinary SQL and typed declarations on the existing metadata-first native declaration preparation. Preserve explicit primitive options and explicit durable preparation. |
| Evidence | Extend completed batch-input evidence and existing Native I/O capability/pushdown reports; distinguish requested demand, applied physical work and unavailable measurements. |

The Arrow compatibility decode is an explicit source boundary. Existing external
buffer immutability/allocation copies remain in force. Native Vortex input,
encoded execution and Vortex output keep their existing paths. There is no
decode-to-Arrow query evaluator and no fallback engine.

## Required semantics and lifetime

- Derive demand after binding and shared pruning. Retain hidden predicate,
  grouping, join, sort, window and subquery columns and row-bearing count input.
- Combine all references to a source before narrowing its schema. Read the union
  of required columns and the union of regions that any reference may need.
  An unrestricted reference prevents row-group exclusion. Repeated consumers
  use the existing explicitly credited native replay owner.
- Preserve complete type/nullability/field order. Rebind native projections to
  the admitted narrowed source schema; never substitute fabricated null columns.
  Dynamic schemas conservatively retain full declared source demand until their
  later requirements are established. Do not weaken exact batch-schema checks.
- Reuse the current safe filter/projection normalization. Computed expressions
  that can fail, LIMIT/OFFSET, outer joins and residual predicates remain semantic
  barriers. Row-group exclusion cannot replace native row predicate evaluation.
- Preserve source order. Do not turn a global limit into an independent limit
  per row group, file or repeated scan. This first reader does not claim limit
  pushdown or early completion of declared query input.
- Query access validates metadata, demanded data and the held source generation.
  It does not certify unread columns or excluded regions. Explicit full ingest
  continues to validate and prepare its complete requested source. These scopes
  must be visible in reports and corruption tests.
- Decode one demanded batch at a time and release the original batch before
  the next demand. Reader state and encoded ranges retain their credits; final
  output certification occurs after EOF, reader release, I/O drain and the
  final source-generation check. No background work survives the result.
- Native retention and read buffers use the existing shared memory pool.
  Predecode working grants and input admission do not establish an allocator
  limit for all upstream decoder allocations or process RSS. Report that scope
  honestly and test deterministic pressure refusal and cleanup.
- A read, decode, identity, budget or cancellation failure terminates that
  execution. It must not trigger a retry through full preparation or another
  query engine. Explicit prepared artifacts remain available for repeated use.

## Prospective acceptance

Before runtime edits, freeze the current runtime and a focused Parquet baseline
using the existing independent 1M-row, 38-column fact family. Persist source
batches into bounded row groups, verify complete typed readback, and retain
original source and build identities. A fresh source means no prepared artifact;
it does not mean a cold filesystem cache.

Declare a finite common-source comparison: current full preparation plus query,
demanded native batches, already-prepared Vortex, and one equivalent Polars
observation per selected logical case. Reuse typed workload declarations and
strict complete-result comparators. Keep engine clocks separate from complete
workflow, first-output and sequence clocks. Do not rerun the initial 347-case
baseline. The selected baseline/noise observations determine a written relative
and absolute retain/drop threshold before any candidate execution.

The selected cases must include narrow projection, hidden selective predicates,
a statistics-prunable predicate and an unprunable control, broad consumption,
grouping, sorting, a repeated-source composition, shared/new-column sequences
and an already-prepared native control. Fresh seeds or predicate/projection
combinations are held out from the initial performance decisions. Inspect
actual selected native operators: eliminating preparation is insufficient if
the resulting complete query regresses materially.

Correctness and lifecycle acceptance also cover all five real interfaces,
typed empty input/results, null/unsigned/decimal/temporal/nested data, missing or
ambiguous statistics, changed source generations before/during/after delivery,
invalid demanded and undemanded data, cancellation, low memory, slow consumers,
large output, spill where supported, and mixed native/Parquet/caller batches.
Physical projection and region pruning require observed read evidence.
Counters that cannot be measured remain unavailable, not zero.

Use focused Rust and Python checks while implementing. At the cohesive boundary,
run required workspace formatting, Clippy and tests, native feature checks,
installed-package and public-surface acceptance. Justify any ClickBench run
against the changed shared behavior and use its isolated single-pass procedure.
Retain unfavorable observations and original failures. Keep the implementation
only when the frozen correctness, resource and benefit/control gates pass.

## Frozen first-use comparison

Before runtime edits, all 39 observations pass complete independent comparison:
two fresh observations for each of nine cases, nine already-prepared Vortex
controls, nine same-Parquet Polars controls and a three-query shared-source
sequence. The independently generated 1M-row fact source was converted to
Parquet with 123 row groups, all 38 columns and complete typed readback. Unique
copies isolate automatic prepared-artifact reuse; OS caches are uncontrolled.
The exact cases and immutable jobs, receipts and source/build identities are in
the local `source-demand-2026-10-26` evidence root. The first fixture-freeze
attempt failed on an incorrect evidence pathname before writing input data;
its failure is retained and the corrected freeze passed.

`baseline-decision.json` freezes the following acceptance before runtime edits:
at least six of nine cases, including the statistics-prunable single-row case,
must improve complete workflow cost by both 25% and 0.250 seconds against the
better pre-change observation. Every case must remain within the larger of
10%, 0.100 seconds or its observed baseline spread. The three-query sequence
must improve cumulative complete workflow cost by at least 10%. Prepared
Vortex engine controls must remain within the larger of 10% or 0.020 seconds.
These are finite retain/drop thresholds, not statistical confidence intervals.
Polars remains comparison context, not a superiority gate; its unavailable
engine clock is not substituted.

## First candidate and broad checkpoint

The first candidate passes all 21 focused observations and six observations on
the separately frozen held-out seed. All nine fresh cases meet the prospective
benefit threshold, and the prepared controls and three-query sequence pass.
One isolated Full43 execution passes all complete values and schemas with
46.194256330 seconds of engine time. These observations remain attributed to
the first candidate runtime, whose source snapshots, hashes and build receipt
are retained before the follow-up correction.

The maintainer subsequently requested a current broad checkpoint. All 347
existing executable cases pass on the same logical 1M-row dataset, now with
Parquet sources, using Python SQL and one execution per case. Native engine
time is 296.288798449 seconds. Every query reports the demanded-source route
and zero prepared-input bytes, but only 34 queries reduce physical column
demand and 21 exclude row groups; 49 distinct queries do either. This is not a
regression comparison with the earlier 86.828000503-second prepared-Vortex
cohort: source format, preparation treatment and runtime differ. The earlier
cohort recorded 2.041 seconds of native preparation and 0.056 seconds of
prepared-state publication from Arrow IPC, paid once for its five tables.

The checkpoint exposes a shared binding gap. Backward demand sees expanded
identity projections before native binding removes them. Ordinary aggregates
and computed projections can consequently retain every source column even
though the fully bound operator needs only a few. The correction narrows the
already-bound scan from all expression dependencies, grouping and measure
columns, and join keys/output/condition columns. It does not delete expressions,
move computations across filters or ranges, infer source row counts, or skip
ordinary name/type validation. This additional narrowing is admitted only for
owned sources with the demanded-read validation contract. Prepared Vortex and
caller batches retain their existing validation scope: a regression test caught
an unused nonfinite Vortex payload being excluded before join validation, and
the correction preserves that existing failure. Ordinary encoded column-view
projection remains unchanged. All first-candidate observations are retained,
and the revised runtime is separately validated below.

The planned broad Polars pass was canceled by the maintainer before execution.
Existing competitor observations remain fixed; no new Polars query ran for
this checkpoint. Reuse requires honest attribution of the original input and
clock boundaries, including the original broad Polars Arrow IPC starting point.

## Final local acceptance

The corrected runtime passes all frozen first-use, prepared-control and sequence
gates, all six cases on the originally held-out seed, and the complete broad
cohort. The latter now takes 50.877676708 seconds of engine time; 344/347 cases
physically narrow columns, with 21 also excluding row groups. All 347 use source
demand with zero prepared-input writes and preserve all 12,565 complete output
rows. The original candidate's 296.288798449-second checkpoint remains intact.
The reused seed is regression coverage, not a new unseen holdout.

Required workspace checks, native feature checks, 568 relational tests, 151 public
workflow tests and 184 installed-package integration tests pass. Four independent
cost screens are intentionally ignored. One isolated final Full43 passes all
43 complete values/schemas at 46.783400285 seconds of engine time. The portable
packet replays 873 distinct complete observations without executing queries.
The [acceptance report](../benchmarks/native-source-demand-2026-10-26.md) records
measurement limits, the prepared-source validation regression and the scope
amendment canceling a new broad competitor pass.

This accepts the first Parquet physical adapter and common native demand
propagation. It does not close item 20, accept item 18, change CG-1 through CG-23,
or authorize incremental persistence, background preparation or other adapters.
