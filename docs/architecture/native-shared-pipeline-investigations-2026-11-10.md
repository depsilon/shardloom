<!-- SPDX-License-Identifier: Apache-2.0 -->

# Shared execution across native pipelines

Status: maintainer-requested investigation program, November 10, 2026.
`v1_candidate_pending_feasibility`; `claim_gate_status=not_claim_grade`.
This extends item 20 and RFC 0044. It creates no new phase or competitive gate.
The [phased plan](phased-execution-plan.md#planned) alone owns execution order.

## Decision and starting evidence

Preserve native structure, prepared computation and validated knowledge across
compatible consumers, and expose useful choices to PulseWeave and Capillary.
The later operator-state intake places recommendations 8 and 9 first, with
independent decisions, followed by recommendation 10's resource objective. The
initial A/B sequence then remains: fact-position-preserving joins into ranking,
followed by one bounded direct-source bridge into an existing optimized
operator. The dictionary repair's hosted integration and retirement now pass
alongside Variant and UUID. Recommendation 8's
[fixed shared-index candidate is locally retained](native-shared-index-metadata-2026-11-10.md)
and awaits hosted integration; recommendation 9 is next. Carry later mechanisms
only where those paths or separate attribution demonstrate a consumer benefit.
The separate required spatial and resource-admitted deep-traversal obligations
under item 19 remain open.

The maintainer's supplied research inspected `0cae77b` and considered UUID PR
#1675. It is an investigation input, not proof that a candidate is already
implemented, safe or faster. Reconcile each hypothesis against the accepted
source at experiment start, including the current Variant expression changes.
Check pinned Vortex array, selection, source and scan capabilities before
extending an owner. No external query engine executes ShardLoom work.

The [accepted saved 10,000-case results](../benchmarks/evidence/native-expanded-query-2026-11-03/shared-mechanisms/results.json)
at `ecef3a10a18e144c86eacb55ee680b3f89251362` total
1,513.095098453 engine seconds. Ordinary joins account for 427.421712469,
ordering for 171.173029974, windows for 162.317283748 and multiway joins for
143.973395709 engine seconds. These totals prioritize attribution; they do not
estimate recoverable time. They are historical source-bound observations, not
new measurements of the current rich-data runtime. The
[interpretation and request-shape evidence](../benchmarks/native-expanded-shared-mechanisms-2026-11-04.md)
remain unchanged.

The subsequently requested current-runtime full10k observation completed all
attempts but contained 96 failures. Its total engine time is unavailable. The
[dictionary repair](native-dictionary-execution-repair-2026-11-10.md) passes a
separate predefined cohort of all 96 failures and 421 passing controls. Preserve
both observations. Later experiments use relevant selections frozen before the
candidate, exact saved-case reuse from the declared baseline runtime, and a
predefined baseline measurement for any missing cases. The maintainer did not
request routine full10k repeats. New multi-output windows or pressured spill
compositions retain their own versioned baseline/candidate protocols.

## Intake dispositions and shared owners

All seven proposals are accepted for investigation and merged into existing
runtime obligations. None is accepted as a performance implementation yet.

| Order | Proposed work | Existing owners and concrete decision artifact |
|---|---|---|
| A | Preserve join mappings through ranking | Extend existing join index/table/selection, batch and ranked-output owners. Produce a uniqueness/admission contract and complete star-join → top-k comparison. |
| B | Admit optimized operators by proved source capabilities | Extend the existing aggregate scan/lowering/update/finalization family with a bounded source contract. Produce a capability matrix and one direct grouped/count comparison. |
| C | Carry prepared keys, ordering and structural validity | Extend existing immutable native owners with only facts used by a named consumer. Produce owner/domain/property attribution and eliminate one demonstrated reconstruction. |
| D | Feed completed join constraints into source demand | Extend bound-tree demand, native pruning and Capillary admission. Produce generation-bound inner/semi-join constraints and evidence of work never scheduled/read/decoded. |
| E | Share typed preparation across consumers and exact ranking | Extend current expression preparation/reuse and vector span owners. Produce source/domain/policy identity, last-consumer lifetime and constant-query-norm protocols. |
| F | Add measured pipeline feedback to PulseWeave | Extend current work sizing and finite concurrency decisions after useful execution choices exist. Produce one starvation/blocking attribution and an overhead-bounded feedback comparison. |
| G | Amortize managed-object metadata growth | Inspect the actual capture/publication and arena insertion call sites separately. Produce a credited growth/index strategy and distinct-object scaling/lifecycle comparison. Generalize only for a second matching consumer. |

The current [prepared relational dispatcher](../../shardloom-vortex/src/local_primitive_prepared_relational.rs)
declines the specialized route for multiple or nonfile sources. The existing
[aggregate source abstraction](../../shardloom-vortex/src/local_primitive_aggregate_scan_source.rs)
already distinguishes `File` and `Owned` while sharing aggregate work. The
[batch owner](../../shardloom-vortex/src/local_primitives/native_relational_batch.rs)
has both ordinary key preparation and a prepared-key constructor. These are
concrete extension seams; their existence does not prove that arbitrary streams,
joins or selected domains satisfy a specialized operator's contract.

## A: join mappings into real ranked output

Start with two inner/left equijoins whose dimension uniqueness is proved by
completed native build state. Retain the fact-row domain, nullable dimension
coordinates, actual native payload owners and per-consumer column demand.
Compose the next join's mappings and consume ranking keys without gathering
every wide field. Gather each payload column when its admitted consumer needs
it; narrow frequently used columns may still be cheaper to materialize early.

Freeze real output-producing star-join → top-k cases, including wide text and
rich payload. Include full-output and narrow controls, skew, missing matches,
NULL keys/parents, stable ties, reordered selections and spill/cancellation.
Charge mappings, indirection, retained backing allocations, uniqueness proof,
key preparation and final gathers. A small selection pinning a large allocation
must remain visible. General duplicate-preserving match groups require later
evidence and must not be silently treated as unique.

The experiment changes the representation crossing compatible operators. It
must identify the boundary removed relative to earlier probe/coordinate caches,
not merely count fewer helper calls inside the same expanded-output design.

## B: bounded direct-source operator admission

Choose one important direct grouped/count workload from saved evidence. State
the existing kernel's requirements before connecting it: logical types, null
and floating policy, encoding/dictionary identity, source generation and row
identity, ownership/grants, restart/random-access capability, statistics and
ordering. A source proves each required property or the candidate declines
explicitly to an already admitted ShardLoom-native route.

Preserve bounded streaming. Do not route a stream through a fully retained
`OwnedArraySource` merely to pass a dispatch check. Footer completion, persisted
helpers, layout information and file-specific reuse remain file capabilities;
equivalent values alone do not establish them. Include direct Parquet, prepared
Vortex and native intermediate controls, plus multi-file cases only where their
row/generation contract is proved. SQL, DataFrame, typed plan, Python and CLI
front doors must converge on the same native operator family.

## Later experiments and semantic boundaries

Reusable facts are optional additions to existing owners, scoped by source
generation, representation, row domain, expression/comparator identity and
semantic policy. A certified take/slice may derive a fact for covered rows.
Changed bytes, import, representation conversion and uncovered rows require
fresh checks. Structural validity does not waive numeric errors. An unverified
persisted trusted flag supplies no cross-process proof. Ordering facts include
direction, NULL placement and stable tie identity; equality consumers retain
their distinct null rules. Prefer recomputation when retention costs more.

Dynamic join constraints must be conservative, have no false negatives and be
bound to completed build state and source generation. Start with selective
inner/semi joins. Outer/anti semantics, null matching and observable expression
errors require separate admission. Include full-population negative controls.

Expression bundles must preserve selected-domain and lazy-error behavior and
release intermediates after their last consumer. Constant-vector norm reuse
begins only at a legal observation point: empty inputs, dimensions, nulls and
nonfinite components retain existing semantics. Exact scoring/ranking and later
conservative L2 pruning get their own vector workload cases; they cannot explain
historical scalar-corpus costs. Skipping a tail must not hide an invalid value.

PulseWeave feedback should sample completed service time, rows, credited bytes,
queue/downstream acceptance, state growth and spill pressure at legal completion
boundaries. Use a finite set of supported work sizes/concurrency levels, cheap
sampling and hard grants. Preserve small-query controls. Scheduling cannot undo
an unnecessary intermediate already constructed.

Managed-object growth must retain deduplication, deterministic publication,
failure-safe admission, exact owned cleanup and shared payload lifetimes.
Count entries moved per distinct object, separately from payload bytes copied.

## Evidence and acceptance

Keep the original 10,000-case corpus, input generations, interfaces and source
routes stable. Group saved cases additionally by repeated physical work: join
payload reconstruction, key preparation, structural validation, premature
materialization, ineffective pruning, recomputed invariants, pipeline blocking
and metadata movement. Add only counters that decide a named experiment.

Version new composed-workflow cases separately. Vary direct/prepared/intermediate
sources; uniqueness/duplicates/skew/unmatched rows; selectivity; plain/dictionary/
constant/nested/rich representations; full/narrow/top-k/group/window/write/reopen
consumers; comfortable/tight grants, real spill and cancellation; nulls, strict/
extended floats, ties, lazy branches and selected malformed values.

Freeze each candidate's source identities, required cases, independent expected
results, cost counters, engine clock, isolation and retain/drop rule before
execution. Reuse must pay within the scored request. Separately label warm
cross-request preparation; do not insert it into cold or independent-process
comparisons. Keep Full43's isolated single-pass protocol when applicable.

Correctness, resource ownership, no-fallback, all public routes and complete
outputs precede performance acceptance. Lower complete-cohort engine time ships;
higher or unchanged drops, with no minimum percentage. Counter reductions and
small target wins cannot substitute for the complete gate. Preserve every
failure, regression and changed disposition. Additional instrumentation needs
its own measured overhead control. Recommendation 8's complete frozen cohort
passes with engine sum 320.508980218 → 319.977114074 seconds across 2,452 cases.
The baseline combines 433 exact saved repair observations and 2,019 new missing
cases; the candidate runs each case once. This is not a new valid full10k result.
Debug-only attribution is separate from the ordinary release runtime. The small
net gain, every family regression and extra credited metadata/container bytes
remain in its decision record. Window sharing and rolling DISTINCT are unscored
and retain their own acceptance contracts.

The [October 25 workflow rejection](../benchmarks/native-workflow-drop-2026-10-25.md),
October 28 join-probe reuse outcome and
[#1657 selection-retention rejection](../benchmarks/native-selection-retention-2026-11-04.md)
remain dropped. A new mechanism needs a new protocol; do not rehabilitate an old
candidate by changing its denominator. Verify the supplied external research
against primary sources before adopting a design, and never transfer another
system's measured benefit to ShardLoom. Item 19, item 20, RFC 0044 and CG-1 through
CG-23 remain independently open.
