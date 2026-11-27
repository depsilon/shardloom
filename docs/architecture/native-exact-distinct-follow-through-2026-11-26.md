<!-- SPDX-License-Identifier: Apache-2.0 -->

# Exact DISTINCT and aggregate follow-through (2026-11-26)

## Status and grounding

Prospective design and validation contract; implementation, correctness acceptance,
and performance acceptance are separate outcomes. No speedup is claimed here.
Base: `9074900d6cff8fd97be43336fbf151f5cfffc25e`, after [PR #1723](https://github.com/depsilon/shardloom/pull/1723).
This extends item 20's [source-aware foundation](native-source-aware-engine-2026-11-26.md)
through existing native aggregate, key, ownership, and range-worker components.
It creates no new phase or competitive gate and preserves CG-1 through CG-23.
CG-5 correctness and CG-6 benchmark evidence remain prerequisites for claims;
`claim_gate_status = not_claim_grade`.

## Feedback and evidence boundary

The maintainer supplied these warm medians for four 5M-row queries, with 192
matching results and Arrow output included. They are external feedback, not
measurements reproduced by this change or verified native engine-clock records.

| Query | #1722 (ms) | #1723 (ms) | Change (ms) |
| --- | ---: | ---: | ---: |
| Inner join | 226 | 127 | -99 |
| 50K-group aggregation | 884 | 1,088 | +204 |
| Filtered aggregate | 233 | 301 | +68 |
| String DISTINCT | 393 | 524 | +131 |
| Sum of the four medians | 1,736 | 2,040 | +304 |

The supplied approximately 4% slowdown agrees with the geometric mean of the
four latency ratios: `1.044733` (+4.47%). The sum increases 17.51%. These are
different aggregation rules. Grouping contributes the largest absolute increase;
string DISTINCT has the largest reported relative competitor gap.
The reported 2.36x comparison with published 1.3.0 remains supplied feedback.

Exact SQL, data/schema identities, reference results, engine versions, worker
settings, session/cache protocol, and the Arrow-inclusive timing boundary are
not available in the checked-in feedback. The prior [24-query record](../benchmarks/native-source-aware-engine-2026-11-26.md)
uses 1M-row fixtures and cannot substitute for this four-query cohort.
Its referenced `run_source_aware_cohort.py` is not tracked at the base revision.
No reproduction, causal attribution, or repair of the 5M regression is asserted.

## Decision and scope

1. Admit exact `COUNT(DISTINCT)` range-local state merging only after native
   set union is implemented. The base explicitly rejects distinct measures in
   `Spec::admits_parallel_range_merge`; summing partial counts would be incorrect.
2. Reuse `RowSet`'s original-width numeric visitor for dictionary codes so type
   and validity dispatch occur once per batch. Preserve epoch admission,
   dictionary snapshots, bounds checks, null policy, and first-occurrence output.
3. For scalar `COUNT(column)`, count the already-executed validity mask in the
   measure's selected/FILTER domain. Preserve `COUNT(*)`, root validity, and
   ordinary expression/source validation; do not infer a count from schema alone.

These changes share native key and aggregate owners. They do not add query-name
dispatch, result caching, approximate distinct counts, a second scheduler, or
external execution. Separate direct-primitive DISTINCT paths require route
activation evidence before claiming that relational `RowSet` changes affect them.

## Exact range-state merge

Each existing contiguous range retains its own memory-reserved `Aggregate` and
exact distinct sets across decoder batches. Reuse the shared session
`ComputePool`, bounded in-flight range controller, and ordered range completion.

Preserve the existing native Vortex provider selection and CPU ownership rules.
Its scalar-partition and general-group kernels already admit specialized parallel
execution. A supplied session whose provider drivers own the CPU grant can
legitimately keep reduction serial; redirecting it to another compute pool would
risk overlapping CPU owners. A proposed routing override was rejected during
review before publication or performance measurement. Shared range-merge tests
use caller-driven provider progress when the compute pool owns the CPU grant.
Native Vortex scans and encoded values stay inside the existing provider boundary.
Worker ceilings alone do not prove which reduction owner ran or how many workers
overlapped.

- Scalar distinct sets union values into the destination set; increment the
  destination count only for newly inserted values. Never add partial counts.
- Grouped merging first interns source group keys into destination groups in
  source-range and local first-seen order. Keep an explicitly reserved mapping
  from each source group ordinal to its destination ordinal.
- Rewrite each partial distinct entry's `(group, value)` using that mapping,
  then union it into the destination measure's native exact set. Increment only
  the mapped group's newly inserted pairs. Different local ordinals or dictionary
  domains must not change logical equality.
- Preserve existing canonical measure sharing: merge each leader once and let
  followers observe that state. Separate measure filters retain separate domains.
- Preserve NULL exclusion for distinct values and NULL equality for group keys,
  existing numeric/byte equality, overflow errors, empty-input semantics, and
  first-seen group representatives including signed-zero representation.
- Admit compatible multi-measure plans only when every measure can merge exactly.
  Float `SUM`/`AVG` remain on ordered source delivery with one operator worker;
  distinct union does not authorize reassociation of floating-point accumulation.
- Preserve deterministic admission declines for other unsupported aggregate
  classes. New `SUM(DISTINCT)`, `AVG(DISTINCT)`, quantile, selector, collection,
  or generalized nested distinct merging is outside this bounded admission.

The mapping, destination membership, worker states, and merge scratch use
`LiveMemoryPool` reservations. Partial state duplication across workers can raise
peak memory even when it reduces serial work. Existing memory limits, cancellation,
error drain, source-release checks, and final lease release remain mandatory;
no new spill capability or unreserved global set is introduced.

The first declared cohort rejected this runtime candidate on complete engine
time. Its serial DISTINCT control also exposed a changed compiler specialization:
using `DistinctMeasureRows` in both ingestion and merge moved insertion work out
of the existing hot helper. A second candidate wraps only the merge adapter in
the private transparent `MergeDistinctRows` type. All row operations still
delegate to the original borrowed adapter, with no new allocation or set logic.
This separates Rust generic instantiations; release disassembly and the same
complete cohort must establish whether it restores specialization and performance.
The first failed gate and unresolved Vortex-provider regressions remain recorded
in the [cohort evidence](../benchmarks/native-exact-distinct-cohort-2026-11-26.md).

## Vortex-first provider check

- Subject: exact flat scalar/grouped distinct membership, dictionary codes, and
  selected-domain validity counts.
- Upstream concepts checked: Vortex `Dict` values/codes, validity/mask metadata,
  native array/scan providers, and grouped aggregate kernels documented in the
  [current API inventory](vortex-public-api-inventory.md).
- Decision: `implement_shardloom_kernel` for exact group remapping/set union,
  reusing existing Vortex-native representations and admitted provider boundaries.
- Rationale: Vortex grouped sum/count kernels consume already-grouped arrays;
  they do not supply this flat SQL hash-group/set-union lifecycle. Constructing
  grouped lists would duplicate work without established benefit.
- Provider surface: existing feature-gated `shardloom-vortex` integration with
  Vortex 0.93.0. The vendored `aggregate_fn/accumulator_grouped.rs` still accepts
  list-view/fixed-size-list groups, confirming the inventory's provider boundary
  against the current source. No dependency update or query-engine integration.
- Residual handling: existing ShardLoom-native execution or explicit decline.
- Decode/materialization boundary: preserve dictionary/code and validity owners;
  retain only owned first-occurrence state, never input aliases across consumption.
- Evidence: native semantics/resource tests and actual route/worker reports;
  native Vortex input/output and explicit Arrow compatibility output remain intact.
- Incumbent parallel providers: `scalar_distinct_workers.rs` and
  `general_group_workers.rs` already implement exact partition/group work and
  ordered merges. Preserve their schema-selected worker admission; the new
  shared merge fills the separate relational and resident-session gaps.
- `fallback_attempted = false`; `external_engine_invoked = false`.

## ShardLoom technique review and incumbent effect

Reuse capillary range units, dynamic resource admission, the shared compute pool,
and metadata-first validity counting. Existing PulseWeave limits still govern
admitted workers; no new finite PulseWeave feedback is applied. Dictionary-domain
reuse and exact native keys avoid repeated type dispatch and unnecessary value
work. Keep source workers distinct from operator workers, and engine timing
distinct from Arrow delivery. Evidence-tier controls prevent work reduction from
being reported as a measured speedup.

The intended incumbent effect is more CPU concurrency for exact distinct
membership and less repeated work in grouping/filter consumers. Actual exposure,
memory cost, merge cost, and complete-cohort benefit require measurement.
Orders 6 (operator-guided layout/encoding) and 7 (adaptive skew/selective pipeline
scheduling) remain queued. Retired campaigns, Chrysalis, closed Resonance,
deferred finite feedback, and external Foundry/serverless dispositions are unchanged.

## Frozen semantic acceptance and prospective performance gate

Before measurement, cover these affected-owner cases and nearby controls:

| Area | Required cases |
| --- | --- |
| Exact union | Scalar/grouped distinct; repeated values within and across ranges; overlapping/disjoint domains; empty and all-null inputs. |
| Group mapping | Different per-range first-seen orders, nullable keys, newly discovered groups, repeated logical values under changed dictionaries, signed-zero representatives. |
| Measures | Independent FILTER domains, mixed exact measures, canonical duplicate measures, `COUNT(column)` nullable/root-null inputs, float `SUM`/`AVG` staying serial. |
| Dictionary visitor | Original-width signed/unsigned codes, null codes/values, equal/changed epochs, late invalid codes, bounds and error parity. |
| Ownership | Bounded reservations, low-budget failure, cancellation during consume/merge, worker error drain, consumed-input release, final reservation return. |
| Source/control parity | Owned arrays, direct Parquet and native Vortex; one versus multiple workers; selective and dense cases; unchanged inner-join control. |

Require complete typed values and ordering where promised, plus meaningful
activation assertions. Run required formatting, clippy, workspace tests, affected
native tests with release-user-surfaces, and applicable Python/public-surface checks.
Record failures and unexecuted checks without borrowing prior PR validation.

Freeze exact requests, source/runtime/reference identities and resource settings
before timed work. Reuse `engine_execution_seconds` with missing clocks unavailable;
record Arrow-inclusive delivery separately. Apply the [benchmarking policy](../skills/benchmarking.md):
lower complete declared-cohort engine total admits the performance candidate after
correctness/resource checks; retain every regression and the secondary geometric
mean. No broad Full43/full10k run or competitor rerun follows from this note.
The exact supplied four-query comparison remains pending its missing identities.
