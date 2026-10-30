# Shared analytical performance continuation — October 29, 2026

Status: prospective implementation/acceptance contract for the seven candidates
already recorded in [item 20](native-analytical-completion-2026-10-27.md#performance-candidates-after-composition-acceptance).
The recursive/materialized unit is accepted and merged in PR #1625 at
`a1ff5bb1d8698eb261bc1c0e3e9987b7da1439b5`, after all 54 hosted checks passed
and review was clear. The prerequisite for a fresh baseline and candidate
measurement is satisfied. Batch-function and connector/refresh work remain later.
Both item-20 tracks and CG-1 through CG-23 remain open.

All candidates use the same native engine across formats. Reuse source-demand,
array, reservation, execution, spill and result owners; keep decoder and file
metadata work at the corresponding adapter boundary. Vortex remains native
input/output. No new external executor, dependency or fallback is admitted.

## Candidate 1: projection-transparent bounded ordering

Disposition: **dropped** after its single complete comparison. The accepted
composition baseline records **15.305938168 engine seconds**; the candidate
records **15.383331123**, an increase of 0.077392955 seconds. Both pass all 347
queries and 12,565 complete result-row comparisons. The binary total rule fails,
so the production change and its experimental tests are restored to the accepted
source. Exact candidate source versions, local acceptance and all observations
remain retained. The admission contract below records the attempted mechanism.

Fresh targeted tests prove prefix activation, but the original 347 cases show
no changes in their existing ordering counters. Projection and ordering family
totals also increase in this observation. This does not support a performance
benefit or justify another timed trial. The accepted baseline stays at the
composition revision for candidate 2.

Source review finds a concrete blocked path in
`local_primitive_relational_transform_bind.rs`: `limit_input` communicates its
required prefix only to an immediate bound Sort. A direct-column Project or a
flat pipeline of such projections between Sort and LIMIT loses that hint, even
though it preserves cardinality and row order and performs no value computation.
The sort therefore retains or emits more rows than its consumer requires.

Admit prefix propagation through only bound direct-column projections, including
renaming, reordering, repeated references and hiding the original ordering key.
The bound Sort keeps its original fields, keys, NULL placement and stable tie
rules. Preserve each source's admitted validation scope and completion obligations,
including complete streamed input traversal for zero-count consumers. Do not push
demand into the source, change columns demanded for
validation, commute operators, or rebind ordering keys against projected names.
The prefix remains the checked sum of outer offset and count, with zero count
using a zero prefix. Preserve the existing LIMIT node and result slicing.

Use an iterative traversal through ordinary Project nodes and flat pipelines
whose stages are all direct-column Project stages. Every filter, computed
expression, observation, intervening range, retained relation, join, aggregate,
window and other operator is a barrier. Binding must already have validated each
expression and schema; literals/casts/functions receive no inferred exemption.
Existing zero-output completion, cancellation and resource-failure contracts stay
in force. No hint crosses the action-owned materialization boundary.

Vortex-first decision: reuse the existing admitted native `Sort`,
`StreamingPrefix`, Vortex arrays and field/gather/slice providers. This is a
ShardLoom relational admission change, not a new ordering algorithm or array
abstraction. Vortex's array and scan APIs do not own this bound relational
LIMIT/Project/Sort contract. The pinned 0.90 provider and no-fallback boundaries
remain unchanged. Existing native ordering counters prove actual activation,
complete rows validated and retained/copied work; no new public diagnostic is
needed solely to label this rule.

## Correctness and mechanism acceptance

Before measurement, freeze fresh fixture seeds and independent complete expected
values covering hidden and renamed keys, repeated/reordered columns, nullable
keys and payloads, stable ties, mixed directions, offsets, empty inputs, zero
LIMIT and prefixes beyond the input. Include flat pipelines longer than the
ordinary nesting guard. Verify exact rich/nested/decimal/temporal/floating
payload carriage where the existing ordering owner admits it.

Tests must show both application and refusal. Computed projections with a late
arithmetic error, filters that change cardinality, observations and retained
relations remain barriers. Complete-source validation must still reject late
producer/schema/payload errors, including when count is zero or a winning prefix
was found in the first batch. Preserve each source owner's release before the
next demand, retained output ownership, memory denial, cancellation, consumer
failure and successful fresh retries. Exercise ordinary resident, streamed and
already-admitted spill paths without changing their admission limits.

Use complete independent comparisons through the five existing public interfaces
and Vortex, Arrow IPC and Parquet controls. Runtime counters must show reduced
retention/copying for the intended shapes and absence of prefix admission across
the declared barriers. Existing native payload validation remains authoritative;
no schema/counter-only correctness result is sufficient.

## Cohort and disposition

The historical 347-query observation remains 15.304317834 engine seconds for its
recorded source/runtime. Capability additions since that snapshot require one
fresh complete baseline on the accepted composition revision before attributing
any change to a new candidate. Use the same five existing Parquet inputs,
347 queries, independent complete references, Python SQL surface, two native
lanes and 2 GiB memory policy. The exact frozen runner, protocol, source/product
identities and resource settings belong in the pre-execution receipt.

Each candidate then executes the unchanged cohort exactly once against its
immediate accepted baseline. Validate all 12,565 result rows and schemas, retain
per-query/family observations and all failures, and use total engine time for the
binary decision: lower retains; unchanged or higher drops after correctness,
resource, ownership and no-fallback gates pass. Historical observations are not
rewritten and unrelated-family movement is not attributed to the candidate.

Keep other task-owned work, agents, builds, packaging and profiling idle during
query execution. Retain the existing lightweight concurrency guard, storage
limits, timeouts and process cleanup. Perform full hashes and recursive evidence
inventories before or after queries. No cache manipulation, artificial pacing,
competitor repeat or automatic diagnostic rerun is admitted. Full43 belongs at a
meaningful integration boundary, not each candidate. No measurement or benefit
claim follows from this prospective contract.

The remaining six candidates keep their original order and obligations in the
parent plan. Freeze each mechanism's exact admission and correctness evidence
before implementation/measurement; extend this record without inventing new
phase IDs or splitting unrelated public interfaces into separate engines.

## Candidate 2: parent demand through native joins

Disposition: **retained locally**, pending the combined integration and hosted
review gates. Its one complete 347-query pass records **15.070406716 engine
seconds**, against **15.305938168** for the accepted composition baseline, a
reduction of 0.235531452 seconds. All 12,565 independent result-row comparisons
pass. This becomes the next candidate's accepted comparison baseline.

The two full-population count joins each reduce demanded columns from six to
three, source reads from 518 to 259, and compressed data read from 12,407,471 to
7,436,162 bytes. Their engine observations fall from 0.228796916 to 0.125751916
seconds for inner join and from 0.236391916 to 0.124164500 for left join. The join
family total falls by 0.254824667 seconds; multiway and composed-report families
also decrease. These are single observations, not statistical confidence or a
competitor comparison.

One filtered grouped-aggregate case increases by 0.010049959 engine seconds and
crosses the diagnostic case threshold. Its relation contains no join and its
retained physical counters are unchanged, so this join rule does not activate
there. Preserve that observation and the increases in other families without
attributing an unproven cause or rerunning the cohort. No family regression flag
fires; the complete total passes the maintainer's binary rule.

Acceptance includes 41 source-demand tests, 6,503 native workspace tests (35
existing opt-in cases ignored), 3,289 default workspace tests, strict lint in
both configurations, formatting, 165 public-interface comparisons over 1,695
rows, and 260 isolated installed-package tests with no failures or skips. Earlier
fixture failures, the demonstrated count-projection regression and its before,
baseline and corrected controls remain retained with exact source versions.
The corrected public test resolves the installed bundled CLI through the public
client. Full43 remains due at the combined performance integration boundary.

Extend `narrow_source_demand` at the existing bound Join node. Parent projections
and aggregates currently stop there, so the join gathers declared output columns
that its consumer never uses. Retain requested output fields in their original
order, then reuse `narrow_join_inputs` to preserve both key sets and all ON
dependencies. Apply this recursively to admitted multiway joins. Keep every join
kind's multiplicity, ordering, outer NULL extension and ASOF dependencies.

Admission requires both child trees to satisfy the existing private owned-source
demand contract. It depends on source ownership and validation scope, not a
format-name switch. Prepared native Vortex and public Arrow sources remain
controls under their current complete-payload validation obligations; this
candidate does not remove their gathers. Every computed projection keeps its
evaluation and first-error order. Sort, observation, retained relations and other
existing demand barriers remain barriers. Do not add predicate pushdown or alter
join selection, completion, spill or output ownership.

For count-only or constant consumers, retain the first declared join output as
a row-bearing field, matching existing scan narrowing. Preserve the declared
public join schema checks before optimization; introduce no zero-field payload
or synthetic row-count representation. When a join needs only one input's row
population, add that input's first already-bound field to the join's dependencies.
Ordinary projection, filter and window demand then keeps its actual source
dependencies. This requirement belongs at the join boundary: propagating a
generic keep-first rule into unrelated projections changes which source pages
an ordinary count observes. Computed projections retain their own input demand.
Charge temporary demand metadata through the existing binder owner. Rebind
narrowed source projections with the existing source-demand pass.

Vortex-first decision: use the existing Vortex DTypes, field projections, native
join gather providers and source demand. Upstream array/scan APIs do not own
ShardLoom's relational parent-output dependency contract. No new array, join,
reader, memory owner, dependency or external executor is needed. Pinned Vortex
0.90 and native Vortex input/output contracts stay unchanged.

Freeze fresh fixtures with keys containing duplicates, misses, NULLs and empty
inputs. Cover projected/renamed outputs, count-only and mixed aggregates,
multiway joins, ON-only dependencies, all admitted join kinds, ASOF, rich payloads
and existing spill. Complete independent results must agree across all five
public interfaces and the three existing format controls. Corrupt excluded
Parquet pages may be omitted under its existing demanded-read scope; demanded
pages, computed errors and prepared/external complete-payload errors must still
fail. Check cancellation, memory denial, source completion/generation, consumer
failure, cleanup and fresh retry.

Debug-only native observations count constructed join rows and payload cells;
existing source-demand reports count requested versus available columns, bytes
and completed inputs. These establish removed work independently of timing.
Freeze the complete 347-case comparison against candidate 1's accepted
disposition (or its predecessor if dropped). Declare join, multiway, aggregate
and report families for diagnostic attribution and apply the same binary total
engine-time decision. No gain follows from this contract alone.

## Candidate 3: controlled reader buffer transfer at shared intake

The demanded local reader currently hands an owned Arrow batch to
`ResidentMemorySource::from_arrow_in_context`, which validates it and copies every
referenced buffer through the shared credited allocator. The existing ingest
owner in `ingest_arrow_ownership.rs` already admits exact per-buffer transfer
from a controlled Parquet reader. Reuse that owner and its ownership policy at
the common native intake boundary. Keep reader provenance inside the private
`ColumnarReader`; a format label, visible buffer length or caller assertion never
grants transfer. Public Arrow/custom producers retain validated-copy intake.

Preserve the existing schema, complete Arrow-array, nested-value, floating-policy,
cancellation and memory checks before publishing a native source. A transfer
attempt requires the pinned Arrow provider's unique standard allocation proof,
zero byte/array/validity offsets, required alignment and complete allocation
capacity no larger than the admitted copy charge. Reserve that complete capacity
before adoption. Shared, sliced, opaque, oversized, empty and otherwise unsuitable
regions use the existing credited copy. A refused reservation remains a failure;
it does not authorize an unaccounted adoption or a second execution path.

Use Vortex's existing Arrow import and `HostAllocator::reserve_external` provider
surfaces with the current credited buffer owner. Structural metadata credits must
survive through transferred buffers, child arrays and slices just as they survive
through copied buffers. The reader's declared decoder-workspace reservation and
generation checks remain separate and unchanged. Reader/decoder internal
allocations are outside the exact buffer-transfer claim; this is neither a
whole-query zero-copy claim nor an RSS bound. Native Vortex input does not pass
through Arrow to use this shared engine.

Record actual copied buffer bytes, transferred visible bytes, transferred regions
and the full retained allocation charge at intake. Aggregate those observations
through the existing native batch-input report and certificates. Preserve existing
copied-byte meanings and distinguish these Arrow-boundary observations from the
separate encoded-exchange transfer report. Ordinary external intake reports zero
transferred bytes. Instrumentation must measure the ownership decision itself,
not infer transfer from reduced memory, elapsed time or source format.

Fresh real-reader fixtures must include primitive widths, NULL/Boolean bitmaps,
strings and binary, decimal, temporal, list and struct payloads, dictionary and
plain encodings, tiny batches, multiple row groups and demanded projections.
Pair enabling private-reader cases with public Arrow and prepared Vortex controls.
Retain the existing ownership owner's sliced/shared/opaque/oversized/empty-buffer
negative controls and prove that a mislabeled custom producer gains no admission.
Check complete values, metadata and floating bits; source closure and surviving
output/child lifetime; full-capacity credit release; memory denial, cancellation,
consumer failure, changed generation and fresh retry. Exercise real public query
surfaces across the three format controls rather than using historical ingest
observations as query evidence.

Vortex-first decision: use the pinned native Arrow-import and host-allocation
providers, wrapped by existing ShardLoom source admission, memory and certificate
owners. No new array owner, source abstraction, per-format operator, dependency or
external executor is introduced. Another decoder needs its own provenance and
allocation proof before receiving a transfer policy.

After correctness and actual transfer activation pass, freeze one unchanged
347-query comparison against the then-current accepted baseline. Apply the same
complete-result and binary total engine-time rule. Preserve all observations,
including low transfer eligibility or a dropped result; neither the existing
ingest result nor this prospective contract establishes a query speedup.
