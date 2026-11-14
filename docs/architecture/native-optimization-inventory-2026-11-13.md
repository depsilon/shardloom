<!-- SPDX-License-Identifier: Apache-2.0 -->

# Native optimization inventory and coverage evidence

Status: ongoing audit and implementation under the
[optimization coverage contract](native-optimization-coverage-2026-11-13.md).
All 10,000 original declaration identities have been joined to their saved
results and operator paths. The source inventory records existing owners,
callers, admission rules, tests and missing evidence. It does not establish
complete optimization coverage or assign a coverage percentage.

The original 10,000 observations belong to `9f4580c723f876f087d3e75fb56153b4b099ef20`.
The corrected owner inventory describes `cac86e8b0a5fa5fa8bf4c900dbef7a703746bfa8`.
The subsequent cardinality/unary connection is measured separately at
`c79f27a8222e7a1fe5009b07a2a19c1c9dd387bb`. Neither later cohort is a new
complete 10,000-query pass.

## Existing mechanisms and outstanding coverage

Each optimization needs an applicable operator/source/sink opportunity. A
positive query counter does not prove that every eligible node used it. An
admission restriction can identify unfinished integration; it does not close
the opportunity. Selection among competing legal algorithms also needs proof.

| Mechanism | Saved evidence and remaining work |
| --- | --- |
| Exact metadata COUNT | One exact metadata-only count; sixteen value-evaluation declines. Expand source, interface and metadata variants. |
| Statistics and region pruning | Region exclusion in 1,601 queries across all five interfaces. Whole-file pruning and individual predicate opportunities have separate owners. |
| Source-column demand | All 6,122 Parquet/IPC declarations classified; 194 excessive-column observations are corrected through shared owners below. Vortex evidence remains incomplete. |
| Safe source-prefix demand | No positive observation in the original corpus. Existing completion, range and prefix tests cover a separate axis; most corpus limits follow sorting. |
| Predicate and derived-field pushdown | Shared owners and negative controls exist. Reports do not identify every eligible and declined predicate boundary. |
| Ranked reads and selected payload | Twenty-four original admissions. Of nineteen prior admissions in the Sort cohort, eleven remain and eight decline under the existing compressed-byte cost rule. |
| Native Vortex file kernels | 2,974 file-kernel invocations in 2,379 queries, within 66,280 native operator invocations. These counts do not measure all optimizations. |
| Encoded, dictionary, constant and run-end reductions | Root reports expose some accessor/reduction work. Embedded provider work lacks an equivalent complete opportunity inventory. |
| Private-reader buffer transfer | Executed in 4,238 Parquet queries across all five interfaces. Required copies can coexist with transfer; transfer does not mean zero decode. |
| Typed keys, shared indexes and exact lookup caches | Join, set and aggregate owners have source/test evidence. Detailed cache and directory activation is largely test-only. |
| Equality joins and semi/anti membership | 3,193 ordinary-join declarations; 2,096 queries report detached streaming build rows. This does not classify every physical strategy. |
| As-of lookup | 392 declarations exercise as-of joins. Resident search-strategy decisions lack comprehensive reporting. |
| Cyclic existence semi-join | The file-backed corpus does not exercise the current memory-only admission. Native and installed tests establish that route; broader source integration remains open. |
| Compact typed aggregate state | 7,828 aggregate declarations. Root-specialization reports provide partial evidence; the direct-source COUNT bridge remains queued. |
| Aggregate preparation and measure reuse | Compatible repeats have accepted owners and tests. The corpus does not expose a per-measure opportunity/activation denominator. |
| Exact distinct and bounded aggregate workers | Distinct measures occur in the corpus. A relational Aggregate node does not itself prove primitive worker admission. |
| Existing-order reuse | Executed in 4,333 queries; 9,342 queries record consideration. Multiple operators can contribute decisions within one query. |
| Top-K and late payload gathering | 9,313 declarations contain a limit and 9,508 contain a sort; 8,457 queries report streaming prefix validation. Full-sort behavior is a separate axis. |
| Analytic windows and shared state | 776 window declarations, each with one window function. Multi-function state sharing and additional frame families require separate compositions. |
| Subquery indexes and equality decorrelation | All 392 membership declarations are uncorrelated IN/NOT IN. Correlated-membership decision fields exist, but this corpus does not exercise them. |
| Set membership and streaming UNION ALL | 789 set declarations. Complete per-algorithm opportunity evidence remains open. |
| Action-local materialization and recursion | 196 materialized-reuse and 196 recursive declarations; native replay executes in 895 queries. This is distinct from cross-query caching. |
| Expression preparation, reuse and typed bulk kernels | 1,465 declarations contain scalar calls. Recipe, bulk-kernel and reuse decisions are not comprehensively recorded per expression. |
| Vector, tensor, Variant and rich typed providers | No vector/tensor/Variant expression calls in the corpus. Carrying rich source fields alone does not exercise these providers; dedicated installed tests remain separate evidence. |
| Explode, melt and sparse pivot | 196 list-expansion declarations. The 82 excessive Parquet/IPC expansion projections are corrected below. Melt and pivot need additional corpus declarations. |
| Native rolling state | Analytic rolling SUM does not prove use of the separate unary rolling owner. |
| Prepared weighted sampling | No sampling or repeated prepared-sampling declaration in the corpus. Existing tests exercise immutable-memory calibration reuse and population rebuilding. |
| Adaptive bounded work | Scan/decode/replay/sort-output windows execute in 8,632/4,238/853/8,649 queries. None records pressure-driven shrink or regrowth. |
| Memory, native spill and cleanup | No original-corpus spill execution. Current-source tests separately force spill, pressure, denial, cancellation, late failure and cleanup. |
| Native output and bounded delivery | The corpus completes row delivery. Installed tests separately exercise native output, producers, formats and credit-window boundaries. |
| Bounded workers and queues | Operator-specific worker evidence exists. Selected lane counts and Capillary labels do not prove general parallel scheduling; applied PulseWeave ingest policy is separate. |

The planned general runtime-filter, skew-handling and cross-query reuse
capabilities remain unimplemented or conditional under their existing contracts.
Planning records do not establish execution. Rejected experiments remain
rejected, and the direct COUNT bridge's invalid baseline remains preserved.

## Query-by-query source-column audit

Dependency reconstruction retains hidden sort/join/filter keys, all computed
expressions and measures, complete distinct records, every expanded list,
window dependencies and retained/recursive inputs. Required columns are unioned
per declared source, including repeated scans. Every frozen declaration identity
was reconstructed; no query was executed for this static audit.

Originally, 5,927 Parquet/IPC observations match their logical required-field
count, one uses metadata COUNT, and 194 read more fields. There are no
below-bound cases or unclassified declaration operators in this screen.

- The 98 Sort cases now match required columns in the separately frozen
  606-query cohort. All 606 complete results pass.
- Fourteen filtered COUNT(*) cases no longer read a second carrier column when
  a predicate field already supplies the real row population.
- Eighty-two expansion/reduction cases no longer carry an unnecessary scalar
  source field. All expanded lists remain for cardinality and shape validation.

The latter 96 cases match required columns in a separately frozen 213-query
cohort, with 117 controls. Its 135 comparable Parquet/IPC cases all match the
dependency count; 78 Vortex cases remain correctness/performance controls for
this metric. Both candidate cohorts cover all five public interfaces and have
no overlapping case IDs. The [observation report](../benchmarks/native-optimization-coverage-2026-11-13.md#shared-source-demand-connections)
records their separate engine times, resources and retained regressions.

This closes the 194 identified excessive-column observations. It does not prove
byte-optimal reads, unchanged behavior for every unmeasured future declaration,
or a current full-corpus engine total.

## Native Vortex and validation obligations

All 3,878 Vortex declaration identities are independently reconstructed. Of
these, 172 saved rewritten projection/predicate scan contracts exactly match
the static dependencies; 3,706 have no comparable saved contract. Logical scan
contracts do not measure physical per-column reads or decodes.

The generic `native_source_demand_projected_columns` metric belongs to completed
owned columnar batch reads. Direct prepared Vortex scans and embedded file
kernels have other report scopes. A missing generic counter is neither proof
of absent projection nor evidence that an opportunity is covered.

Prepared Vortex joins validate declared payload values at selected output
positions, even when a parent masks those payload columns. Unmatched build
rows become observable for Right/Full output. A mixed execution with an attached
streamed provider detaches and validates the full delivered build record before
matching. Null parent masks, unreferenced dictionary values, field visitation
order, nonfinite values and decimal precision remain observable at their
existing boundaries. Twelve exact current-source tests establish these cases
and the existing ordinary Vortex column-view projection route.

The next Vortex connection must preserve those checks while identifying legal
work removal. Simply broadening owned-source admission would suppress existing
errors. The existing validated join output mask is one example of reducing
materialization while retaining selected-value validation; it does not establish
source-read avoidance for every operator.

The subsequent bounded-sort audit reproduces a separate equivalence error:
the file shortcut can omit invalid hidden or non-winning payload values that
the original Sort validates. The
[admission correction](native-optimization-coverage-2026-11-13.md#preserve-the-sort-payload-contract-during-file-kernel-admission)
retains the shared Sort whenever that extra validation is required, while
preserving eligible integer/text-key and prior-projection kernels. This closes
the observed correctness discrepancy; it does not count as an optimized
Vortex demand connection or a performance improvement.

The follow-up [388-case correction cohort](../benchmarks/native-optimization-coverage-2026-11-13.md#sort-payload-validation-correction)
removes all 21 previously reported file-Sort shortcuts and retains all 36
other file-kernel cases. Its 337.804658081-second candidate total is a
correctness result, not a speedup claim; its four historical groups preserve
their separate baseline source identities. The ten owned-source controls retain
their column counts. The source-bound packet verifies the native pipeline,
absence of catch-all/fallback execution, denied reservations and spill, and
preserves validation evidence for 3,325 default tests, 7,145 native tests (39
ignored), and 442 installed tests with no skips. All 5,691 source identities
match the measured fingerprint. This does not make a missing per-opportunity
or Vortex source-work counter complete.

## Breadth and evidence limits

The corrected historical source inventory verifies 255 unique source/design
hashes. Its 136 literal operator-test references resolve to 338 unique tests:
337 passed and one manual attribution test is ignored. Repeated references and
default/native log occurrences do not count as additional tests.

At the later cardinality/unary source, formatting, strict default/native
workspace Clippy, 3,325 default tests, 7,139 native workspace tests and all 442
installed-package tests pass. The full native library contributes 4,166 passes
and 36 intentional ignores; the workspace has 39 intentional ignores. Every
check, build and installed proof binds the same 5,689 source-file identities.

Existing public tests separately cover nine input formats and eight outputs
through five interfaces under two grants: 180 collections and 80 writes with
complete readback. Additional tests cover producers, memory sources, pressure,
spill, prepared reuse, cancellation and output delivery. This is broader than
the 10,000 corpus, but it is not the full source/operator/type/output/interface
cross-product or universal optimization activation proof.

The [inventory packet](../benchmarks/evidence/native-optimization-inventory-2026-11-13/README.md)
preserves corrected and historical inventories, exact source snapshots, test
receipts and complete declaration/observation matrices. Its portable verifier
checks artifact and source identities; it does not independently repeat semantic
inference, tests or query execution. The separate
[connection packet](../benchmarks/evidence/native-optimization-connections-2026-11-13/README.md)
replays identities, engine clocks, dependency counts, certificate work and
source-bound validation for both measured cohorts. Complete answer comparison
was performed locally against the original independent reference ledgers.

Per-opportunity decisions, uncovered current compositions and planned mechanisms
remain active work. Item 19, item 20, RFC 0044 and CG-1 through CG-23 remain open.
