<!-- SPDX-License-Identifier: Apache-2.0 -->

# Broad-query foundation and initial observation — October 26, 2026

All **347 executable cases pass at 1,000,000 fact rows**, with one native
execution and one Polars execution per case. Native engine time sums to
**86.828000503 seconds**. The three sequential cohorts retain 79, 30 and 238
distinct cases; none is repeated or discarded. This establishes the initial
measurement foundation for [item 20](../architecture/native-broad-query-program-2026-10-26.md),
not completion of that item's analytical capabilities or competitive gates.

The [corpus and reproduction instructions](../../benchmarks/traditional_analytics/query_surface/README.md)
describe the shared typed declarations, independent expectations, five real
public interfaces and guarded runner. The [portable packet](evidence/native-broad-query-2026-10-26/manifest.json)
retains complete requests, values, schemas, native reports, timing observations,
frozen source snapshots, validation logs and earlier failures. Offline replay
recompares values without executing a query.

## What was observed

The frozen native runtime is the accepted connected-programming build at merged
source `c287ba45`. The CLI SHA-256 is
`b309284cfa8c9ba746ab64782e5d2ab9ae5485145b636439a4ba7482cc1e1027`;
the native library is
`10e8792d3139456bbe48323fc3100996ba5b877394077bc57f2b8d3816dcb1f2`.
Subsequent correctness corrections do not rewrite these baseline observations.

The independently seeded 38-column fact source has 1,000,000 rows, with 100,000
entities, 10,000 products, 200,000 relationships and 731 calendar rows. Native
preparation and full logical-schema/value readback pass before query execution.
Dataset manifests record observed distributions, identities, file sizes and
physical batch boundaries. This cohort uses the sorted layout, balanced profile
and seed `20261026`. The other four supported layouts have small-fixture identity
and readback tests; no 1M performance evidence is claimed for them.

| Native cohort | Distinct cases | Sum of engine seconds |
| --- | ---: | ---: |
| `million_baseline_r1` | 79 | 30.577111706 |
| `million_composition_r1` | 30 | 25.217447082 |
| `full238_million_r1` | 238 | 31.033441715 |
| Complete initial selection | 347 | 86.828000503 |

The full catalog contains 366 cases across 79 archetypes and 21 families:
347 executable cases across 60 archetypes and 19 pending requests. Its aggregate
space contains 240 feasible factor combinations. A 21-case pairwise selection is
available for smaller developer runs; the final broad selection includes all
240 combinations. Pairwise or full coverage of these declared factors is not
coverage of arbitrary SQL, types or operator compositions. All previously held-out
case IDs were released for discovery by the maintainer's 347-case request;
future independent acceptance must use fresh seeds/layouts/combinations.

Every result has complete names/order/logical types and values checked against
independent standard-library expectations. Exact integers/decimals, NULL and
duplicate multiplicity are preserved; floating policies are field-specific and
declared before execution. Candidate engine results never supply their own
expectation. Native input uses Vortex and all executed candidates retain explicit
no-fallback evidence.

## Initial Polars comparison

The separately authorized comparison uses Polars 2.1.0 and PyArrow 25.0.1, once
per native case. Each pair has the same typed declaration and independent
expectation. Both complete results pass. The baseline is external to ShardLoom;
it is not a runtime dependency or residual evaluator.

The ratios below compare **declaration through complete Python-row delivery**
for both APIs, excluding explicit pre-clock imports and source preparation. They do not compare
ShardLoom engine time with a Polars API clock. ShardLoom reads prepared Vortex;
Polars reads the original Arrow IPC containing the same verified logical records.
Both use two threads and the external RSS guard, while only ShardLoom declares
an internal 2-GiB grant. Polars does not expose nonnullable-field metadata; this
metadata alone is excluded from parity, with values and observable logical types
still checked strictly.

Each case uses a fresh worker. Initialization triggered inside a timed API is
included, including the Polars Arrow-delivery boundary; only imports performed
before the clock are excluded. These costs are not warmed steady-state API
latencies. Earlier raw records use the shorter phrase "imports excluded";
this clarification preserves their measured boundaries and original values.

Native and Polars ran in separate sequential cohorts. Preparation/prehash cache
effects and host drift are uncontrolled. These single observations prioritize
investigation; they provide no noise estimate, balanced-order comparison,
complete first-use comparison, retained optimization result or superiority claim.

| Family | Correct pairs | Median ShardLoom API / Polars API |
| --- | ---: | ---: |
| Projection | 26 | 41.07 |
| Predicates | 16 | 0.35 |
| Nullable expressions | 8 | 0.30 |
| Numeric expressions | 4 | 43.54 |
| Strings | 5 | 46.80 |
| Temporal expressions | 6 | 43.71 |
| Global aggregation | 67 | 1.08 |
| Grouped aggregation | 180 | 1.33 |
| Distinct | 4 | 1.98 |
| Joins | 14 | 2.60 |
| Multiway joins | 2 | 13.12 |
| Ordering | 10 | 0.33 |
| Sets | 4 | 0.64 |
| Composite analytics | 1 | 7.80 |

No pooled ratio is used: 247 aggregate variants would otherwise dominate much
smaller families. Windows, subqueries, CTE composition, nested reshaping,
input/output workflows, persistent lifecycle and resource scenarios have no
executed rows in this first catalog selection. Their pending declarations remain
visible, along with richer statistics, distinct aggregates, timezone/as-of and
quoted-identifier targets. A missing corpus adapter is not proof of an engine
limitation; existing accepted capabilities keep their earlier evidence.

## Demonstrated investigation targets

**Computed projection followed by ordering and a small result.** The controlled
neighbors all return 31 rows with the same ordering and output width:

| Projection | Native engine seconds | Explicit `row_id < 31` neighbor, engine seconds |
| --- | ---: | ---: |
| Direct value | 0.010095416 | 0.017546834 |
| Arithmetic | 1.703128959 | 0.027853250 |
| COALESCE | 1.718112292 | 0.028431000 |
| Lowercase string | 1.852001916 | 0.029696083 |
| Extracted year | 1.714788916 | 0.028180750 |

The exact unrounded clocks remain authoritative in the packet. LIMIT 11 and 100
neighbors retain the same broad difference. Reports select the equivalent native
file provider for the direct case and shared relational execution for the
computed cases. The explicit prefiltered neighbors produce the same values on
this fixture, demonstrating sensitivity to how much input reaches computation.
They do not authorize moving a filter or projection across LIMIT: such a rewrite
could hide invalid values or change error, null, ordering or cancellation behavior.

Source inspection identifies full relational sort/payload validation as a
candidate investigation. Streaming input may already certify payload validation;
file/resident inputs do not acquire that proof merely because a small LIMIT is
present. The baseline does not isolate sort CPU from expression evaluation or
prove that a proposed rewrite is safe. No sort-algorithm optimization is retained
in this foundation PR.

**Full-population joins and composition.** Ten new neighbors exercise complete
fact-table inner/left/semi/anti joins, duplicate fanout, two star-join orders and
a filtered join/HAVING report. Their API ratios range from about 3.8 to 13.4.
The older basic-join cases select only 101 fact rows and cannot establish large
join efficiency. Multiple/nonfile sources reach the shared relational owner;
further attribution must measure build/probe, filtering, aggregate reuse and
materialization before selecting a mechanism.

**Aggregate expression composition.** Direct, arithmetic and COALESCE arguments
cross five value domains, four key choices, SUM/mixed measures and full/one-percent
input. Large family medians conceal individual slow computed/mixed variants.
Keep argument preparation, null handling, group lookup and update sharing in the
same shared owner. A single API ratio is not evidence for a new kernel or worker
pool; actual strategy and work differences must justify the candidate.

## Empty-result correction and preserved failures

The original 238-case tiny parity cohort completes 1,190 calls: 1,189 pass and
one CLI SQL call fails. `predicate_eq` finds no matching rows in the 127-row
fixture. Other interfaces return the complete typed empty result; the CLI
reported `SL_INVALID_INPUT` because its sort result could not be certified.
This original failure remains unchanged in the packet. The 1M case is nonempty,
so its successful baseline result does not close the small-fixture defect.

Root cause: a pushed-down Vortex predicate can finish successfully after reading
data while returning zero arrays. Sort reporting equated zero delivered arrays
with no source work. Count and aggregate reporting already use the shared
`empty_provider_filter_work` contract. The correction applies that existing
contract to sort, conservatively declaring possible read/decode/materialization
work without inventing bytes or claiming row delivery. Native schema, source
generation, memory ownership and certificate admission remain required.

Vortex-first decision: **wrap_vortex_concept**. The pinned Vortex 0.90.0 filtered
scan and its returned arrays are unchanged. ShardLoom extends its existing report
and certificate around that native provider; no reader, scheduler, fallback,
Arrow execution path or per-query special case is introduced. The fixture covers
an unprunable absent value, metadata-pruned absence, a nonempty value, hidden
ordering input, typed delivery and result lifetime. Negative certificate mutations
still reject missing scan/materialization evidence and unsafe effects.

Earlier harness failures also remain: the first supervised attempt failed during
native readback before a query summary existed; the first 79-case tiny cohort
had two adapter errors. The DataFrame `%` constructor now declares the already
implemented shared native remainder, and the typed semi-join adapter uses its
actual public name. Those original failures are not relabelled as expected errors.

## Acceptance boundary and next work

Local acceptance retains exact commands and outcomes in the packet:

- Formatting and default/native workspace Clippy pass. Default workspace tests
  pass 3,261 cases; the feature-enabled native sort suite passes 80 cases,
  including both new regressions. The empty-sort regression fails before the
  correction and passes after it.
- The unchanged original reproducer and three controls pass all twenty calls
  across five public interfaces. The installed wheel passes 183 integration
  tests with no skips, including 210 complete calls across the five interfaces,
  and passes isolated load and uninstall checks.
- The full native Python run executes 1,047 tests with nine optional/environment
  skips. Its only two failures are stale generated API references after the `%`
  addition. Regeneration corrects the recorded source identities; the subsequent
  83 focused reference/builder/package/replay checks and source-only core run
  pass. The latter executes 681 tests with 28 optional/environment skips from
  891 discovered cases. Four separate Polars adapter tests pass.
- Offline replay checks 2,331 complete results and 2,334 original receipts, with
  all three failed query observations and the earlier pre-summary failure still
  visible. It derives the same 347-case selection and engine total without
  executing a query. Archive and semantic corruption controls exercise the
  verifier separately; eleven mutations of the real packet are rejected. A
  relocated source/evidence copy reproduces the replay while denying original
  workspace/data access, native/Polars/PyArrow imports and process/network effects.

The failed first installed attempt is a test setup error: the fixture assumed
`ShardLoomClient` had a context manager. It now uses the supported explicit close
method; the succeeding installed run above is retained alongside that failure.
Legacy receipts without original job hashes and two early cohorts without a
Python snapshot inventory remain explicit proof gaps. Hashes establish packet
consistency, not signed execution provenance. Large data and runtime payloads
are excluded, with identities and regeneration sources retained.
Completed package environments and duplicate package payloads are retired after
verified installation, closed-handle checks and identity inventory. The current
native runtime, tested wheel, original baseline runtime, inputs and all compact
results remain available; unfinished item 18 is untouched.

Hosted acceptance completes in [PR #1611](https://github.com/depsilon/shardloom/pull/1611)
as `4d25a06e`, with all 57 checks passing at `a9cbcb67` and an identical merge
tree. The [integration receipt](evidence/native-broad-query-review-2026-10-26/pr1611-integration.json)
records the completed checks. No Full43 rerun is needed for this
report-only sort correction; existing ClickBench observations are retained, and
no performance improvement is claimed for the corrected runtime.

Item 20 remains open: the six real analytical workflows, richer statistics and
temporal/numeric/recursive/batch-function requirements, broader layouts/resource
coverage and evidence-backed performance corrections are not completed by this
finite baseline. CG-5 and CG-6 are not closed.

The PR's [compression and replay review correction](../architecture/compression-dependency-intake-2026-10-26.md)
has separate source, test, audit and package evidence. It removes the affected
compression dependency versions, returns ORC codec errors without panicking,
and preserves supervisor failures before parsing incomplete worker output.
The original packet, measured runtime attribution and observations above remain
unchanged; the correction's replay executes no queries.

The maintainer's later universal source-demand work unit, starting with Parquet,
is [locally accepted](native-source-demand-2026-10-26.md), with shared demand
binding, bounded native batches and distinct query/full-ingestion validation.
Its corrected Parquet checkpoint passes all 347 cases at 50.877676708 seconds
of engine time; 344 cases narrow physical column reads. These new observations
retain their Parquet input/runtime attribution and do not replace the original
prepared-Vortex/Arrow IPC results above. Existing competitor results are reused.
Incremental persistence and background preparation are later stages.
Existing input/refresh ownership remains with item 18; its unfinished branch is
preserved. See the [sequenced contract](../architecture/native-broad-query-program-2026-10-26.md#active-work-unit-universal-source-demand-starting-with-parquet).
