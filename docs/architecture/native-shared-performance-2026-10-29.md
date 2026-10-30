# Shared analytical performance continuation — October 29, 2026

Status: prospective implementation/acceptance contract for the seven candidates
already recorded in [item 20](native-analytical-completion-2026-10-27.md#performance-candidates-after-composition-acceptance).
The recursive/materialized unit is accepted and merged in PR #1625 at
`a1ff5bb1d8698eb261bc1c0e3e9987b7da1439b5`, after all 54 hosted checks passed
and review was clear. The prerequisite for a fresh baseline and candidate
measurement is satisfied. Batch-function and connector/refresh work remain later.
Both item-20 tracks and CG-1 through CG-23 remain open.

**October 30 scheduling decision:** hold all further Full43 execution until
item 18's universal I/O work and format/interface acceptance are complete.
This supersedes the prospective Full43 checkpoints in the candidate contracts
below. The completed October 30 observation, original protocols and all candidate
dispositions remain unchanged. Continue the declared 347-case candidate checks
and required correctness, resource, public and installed-package verification;
saved-evidence replay does not execute a new query.

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

Disposition: **retained locally**, pending the combined integration and hosted
review gates. Its single complete pass records **14.862644046 engine seconds**,
against the accepted candidate-2 baseline of **15.070406716**, a reduction of
0.207762670 seconds. All 347 queries and 12,565 independent result-row comparisons
pass. This becomes the next candidate's accepted comparison baseline.

Actual transfers activate in 344 cases. Across those repeated query observations,
intake copying falls from 7,460,151,868 to 252,350,010 visible buffer bytes;
7,207,801,858 bytes transfer in 131,728 regions with 7,207,825,036 bytes of complete
allocation capacity credited. Every case conserves baseline copied bytes as
candidate copied plus transferred bytes. Source columns, row groups, decoded
rows, compressed bytes and read requests remain unchanged. These are summed
intake observations, not unique source bytes or whole-query allocation savings.

The grouped case `aggregate_optional_value_group_key_category_sum_coalesce_all`
increases from 0.041607375 to 0.065333083 engine seconds, a 0.023725708-second
diagnostic regression. Its transfer activates and its source reads are unchanged;
the observation does not establish a cause. Preserve it and all family/case
movement without a repeat. No family regression flag fires, and the complete
total passes the maintainer's binary rule. Single observations establish neither
statistical confidence nor a controlled competitor ranking.

Final acceptance includes 6,511 native workspace tests (35 existing
opt-in cases ignored), 3,289 default workspace tests, strict lint in both
configurations, formatting, 180 public-interface comparisons over 7,950 complete
rows, and 261 isolated installed-package tests with no failures or skips and a
verified uninstall. Every Parquet public interface records actual transfer;
Arrow IPC and native Vortex controls retain their existing paths.

Retain all earlier attempts: fixture assumptions about tiny-buffer eligibility,
an observer-default regression corrected before final acceptance, lint failures,
the insufficient low-memory test budget, and one incorrectly filtered invocation
that executed zero tests and receives no verification credit. The final budget
test proves public validated-copy admission is unchanged while controlled
transfers reserve their additional structural metadata before cloning handles.
The performance disposition above comes from the separate complete-cohort pass.

Before this candidate, the demanded local reader handed an owned Arrow batch to
`ResidentMemorySource::from_arrow_in_context`, which validated it and copied every
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

The query entry into the shared admission owner uses fixed counters without the
ingest profiler's repeated-region map. It therefore needs no address index or
retained original-buffer clones for observation. Existing ingest profiling keeps
its repeat semantics. For controlled transfers, before cloning Arrow array
metadata, account for the actual number of backing-buffer handles in variadic
string/binary views, including nested views; a schema-node allowance alone cannot
bound those vectors. Share that structural credit with copied and transferred
buffers for their full lifetime. Public validated-copy intake retains its
existing schema-metadata admission. Tiny batches can legitimately remain copies
under the same capacity and uniqueness rules. Mechanism tests compare copied plus transferred bytes to
the complete referenced regions, and require activation on admitted fixtures.

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

## Candidate 4: source ranking before deferred payload reads

Disposition: **retained locally**, pending the combined integration and hosted
checks. The unchanged 347-query cohort records **13.477651246 engine seconds**,
against the accepted candidate-3 baseline of **14.862644046**, a reduction of
**1.384992800 seconds (9.32%)**. All 347 complete comparisons and 12,565 result
rows pass. This becomes the next candidate's accepted comparison baseline.
No case or family crosses the preserved diagnostic regression thresholds; every
individual observation, including smaller increases, remains retained.

Five cases activate ranked reads. Each ranks all 123 admitted source groups and
fetches payload from one winning group. The wide projection reads 3,513,287
compressed bytes instead of 107,525,511, and the rich projection reads 2,796,536
instead of 19,914,372. Rank-plus-payload byte counts equal the reported actual
source reads in every active case, while distinct group counts retain their
original meaning. The projection family records **1.498156292 engine seconds**
against **2.819951626**. These are single-pass observations, without a statistical
confidence claim or an attribution of unrelated case changes.

The retained packet is `broad-ranked-demand-r1`, built from
`ranked-demand-release-r2`; its exact 3,124 source-file identities, both runtime
products and all 694 baseline/candidate output hashes were independently checked
before accepting the result. `ranked-demand-analysis.json` retains every case,
family, physical counter and input identity. Full43 remains due at the combined
performance integration boundary. No competitor was rerun.

The final source passes 58 focused source-demand tests, including 11 ranking
tests, and 6,522 native workspace tests with 35 existing opt-in cases ignored.
All 3,289 default workspace tests pass without skips. Required strict lint and
formatting pass. The reduced-feature CI build passes; seven warnings remain in
six files identical to the accepted source. An additional, stricter lint attempt
for that reduced configuration remains recorded as failed, including an unchanged
`Source::external` warning. New ranking code is excluded when no ranked source
adapter is compiled, and its metadata lease remains retained in every build.

Preserve the initial test-formatting and lint failures, the feature-boundary
correction, and the incorrectly filtered invocation that ran zero tests and
receives no verification credit. Full native/default tests and required lint
passed again after the final feature correction. No performance conclusion
follows from these source checks.

Public acceptance passes 180 complete comparisons over 12,465 rows across all
five interfaces and Parquet, Arrow IPC and Vortex. Seven enabling Parquet cases
activate on every interface; narrow/all-key projections, large prefixes,
computed/range barriers and other formats retain their expected routes. In the
fresh shuffled wide fixture, ranking reads 95 groups and payload retrieval reads
seven groups (43 decoded payload rows for 31 selected rows). Predicate exclusions
preserve absolute positions, and zero demand reads ranking dependencies but no
payload groups. All 262 isolated installed-package tests pass without failures
or skips, with exact binary integrity and verified uninstall.

The October 30 PR review identified a correctness issue in optional ranked
admission under a tight memory grant. Structural eligibility now precedes the
temporary binding lease: single-group sources, source prefixes and projections
containing only ranking/predicate columns decline without allocating ranked
state. Refusal of the optional binding or adapter metadata lease also declines
before the once-only source starts, leaving ordinary native streaming available.
Source validation, cancellation, and failures after source consumption still
propagate. The cost calculation and native key/selection semantics are unchanged.
This is a review correction, not another performance candidate; final integration
must validate the corrected source and retain the original candidate observation.

Vortex-first check for this correction: retain the existing Vortex arrays,
`StreamingPrefix`, source-demand lifecycle and shared memory pool. The change is
ShardLoom admission policy around those existing providers, with no new array,
decoder, external executor or materialization boundary. Focused checks cover
ineligible admission without reservations, optional budget refusal before reads,
ordinary consumption of the same producer, complete result retry, cancellation,
invalid demand, and released credits.

The first public attempt passed 177 executions and failed three DataFrame-adapter
intakes before execution because one fixture mixed per-key direction/null
placement. Correct only that fresh fixture to the adapter's admitted common
ordering; native tests retain mixed-key coverage. The second frozen runtime has
identical binaries and exactly one changed source file, that Python test. Preserve
both attempts and identities. No original cohort case or engine behavior changes
for this correction.

Extend the common execution-scoped source input contract with an optional ranked
demand operation. The existing source owner retains once-only consumption,
original-batch exclusion, memory accounting, generation validation and final
completion. The relational consumer supplies ranking columns, a bounded output
prefix and native ranking/selection callbacks. An adapter supplies exact source
positions and selected-region reads; it does not compare keys or execute SQL.
Do not open an unreported reader from the sort operator or branch on a format
name there. Public producers and prepared Vortex/IPC sources keep their existing
native route and complete-payload validation unless their own source capability
is separately admitted. No Arrow conversion is introduced for native Vortex.

Admit only an immediate bounded Sort with its existing prefix limit of 16,384,
whose input resolves through direct-column projections/renames to one owned
source scan. Repeated columns are allowed; computed projections, intervening
ranges, observations, joins, aggregates, windows, retained/dynamic/correlated
relations and spill are barriers. The source must have one statically declared
use, with no replay or nested-consumption obligation. Candidate 1's dropped
projection-over-sort rule stays dropped. Exact already-admitted scan predicates
may participate when all their dependencies are ranking columns and the existing
native predicate provider preserves source positions. Unadmitted residuals remain
barriers, including filters whose fallible evaluation domain would move.

For the first adapter, retain the original sorted file/group order and record
each row group's absolute source-row base before metadata exclusions. Each decoded
batch carries its checked contiguous source-position range. The ranking payload
contains native ordering columns and one credited UInt64 source position; attach
positions before residual row filtering. Reuse the existing stable
`StreamingPrefix`, key/NULL semantics, native field projection and filter provider.
Retain only the bounded selected positions, sorted in source order for retrieval.
No population-sized row-location array is permitted. Source positions are private
to this held generation and never become durable/public row identifiers.

Bind both phases to the same held source descriptors and directory membership.
The first adapter reads/decode-completes whole selected column chunks in each
winning row group, then gathers the chosen rows through existing native providers.
It does not claim page-level or selected-row decoding. Read groups in source
order, independently own only selected payload before releasing a producer batch,
and use the existing Sort on the bounded result to restore ordering and stable
ties. Preserve the outer LIMIT and offset. Validate selected positions as strictly
increasing, unique, within the originally admitted groups and at most the prefix.
Account for every selected row exactly once before final result delivery.

Admission requires at least one genuinely deferred physical column and a strict
metadata byte bound: all ranking-column compressed bytes plus the largest
`min(prefix, admitted_groups)` full-payload group byte totals must be less than
the original full-demand compressed total. At most one distinct winning group
can be required per selected row. This conservative bound includes repeated key
reads during payload retrieval. A single group, all-key projection, large prefix,
missing/invalid metadata or no provable read saving keeps the existing native
route. It is an I/O bound, not a runtime-speed prediction. Metadata vectors,
ranking ordinals, schemas and retained values consume existing memory credits;
denied reservations fail without a hidden second execution or relaxed grant.

The private demanded-read validation scope deliberately becomes two-phase.
First, complete all ranking-dependency intake, filtering and ordering validation
over every metadata-admitted group in source order. Only after that succeeds,
validate all decoded demanded payload in every winning group, including its
unselected rows. Payload in groups with no winner remains unread and unvalidated.
Thus a ranking failure takes precedence over a deferred-payload failure, even
when that payload is in an earlier group. This is an explicit source-demand
contract, not an assertion that the previous interleaved error order is unchanged.
A zero prefix still completes ranking validation and source-generation checks,
then requests no payload groups. A complete-query success requires both phases
and the ordinary post-consumer generation checks. Cancellation, consumer failure
or changed generation releases every cursor/credit and produces no success
certificate; a fresh execution retains the normal retry rules.

Record distinct source and metadata facts once, retaining the existing distinct
`row_groups_read` meaning. Execution read bytes, requests and decoded rows include
both phases. Explicit rank/payload group counts are visits whose sum may exceed
the distinct group total. Add phase counts for rank/payload rows, groups and bytes, selected
rows and actual ranked sources, together with the precise validation/read
granularity and admission limitations. Preserve the existing actual intake
copy/transfer observations. Counters must demonstrate skipped compressed payload
reads rather than infer them from a small output or elapsed time.

Vortex-first decision: `implement_shardloom_kernel` for relational demand
orchestration, reusing admitted native providers rather than creating an ordering
or array implementation. Pinned Vortex 0.90 already supplies field/filter/take,
credited native buffers and strict sorted source indices; the existing prepared
Vortex selected-sort provider uses `StrictSortedBuffer` and `with_row_indices`.
Those APIs do not own ShardLoom's once-only compatibility-source lifecycle or
Parquet compressed-chunk selection. Preserve that existing Vortex provider and
reuse its source-order ordinal concept at the common demand boundary. Existing
native Sort/StreamingPrefix, source identities, reservations and certificates
remain authoritative. No dependency or external executor is added;
`fallback_attempted=false` throughout.

Freeze fresh shuffled multi-file/multi-group fixtures before measurement. Cover
mixed directions, nullable keys, stable ties across groups/files, hidden/renamed/
repeated columns, offsets, empty and zero results, rich nested/decimal/temporal
payloads and eligible predicates with metadata exclusions. Pair activation with
all-key/single-group/large-prefix/computed/residual/replayed/spill refusals and
prepared Vortex/public Arrow controls. Corrupt ranking dependencies, selected
payload groups and unselected payload groups independently; verify the declared
first-error order. Check generation changes between phases and after final
consumption, source closure/output lifetime, memory denial, cancellation,
consumer refusal, complete release and fresh retry. Require complete independent
values across all five public interfaces and the three existing formats, actual
read-byte reductions, workspace/package checks, then one unchanged 347-case pass
under the existing binary engine-total rule. Full43 remains a combined gate.

## Candidate 5: inline first rows in the shared membership index

Disposition: **retained locally**, pending combined integration and hosted
checks. The unchanged 347-query cohort records **13.433815339 engine seconds**
against candidate 4's **13.477651246**, a reduction of **0.043835907 seconds
(0.325%)**. All 347 complete comparisons and 12,565 result rows pass. This is
the next candidate's accepted baseline under the binary complete-cohort rule,
not a statistical or isolated causal claim.

The join family records 1.131766710 seconds against 1.213341917; multiway joins
record 0.895376375 against 0.934683084. Grouped aggregation records 5.487348256
against 5.438681039. One case, `aggregate_signed_value_group_key_mixed_direct_all`,
crosses the diagnostic regression threshold (0.026795917 to 0.040427334), with
unchanged reported source counters. No family crosses its diagnostic threshold.
Preserve all increases and the flagged case; no repeat or causal attribution is
introduced to explain them.

Exact memory controls fail against the previous representation and pass after
the change: eight unique entries fit the 320-byte entry/bucket grant without any
duplicate storage, and first-duplicate/growth denial preserves existing rows and
permits retry. All seven focused index tests, 6,524 native workspace tests
(35 existing opt-in cases ignored), 3,289 default tests without skips, strict
native/default lint and formatting pass. Fresh public fixtures pass 225 complete
comparisons over 44,295 rows across five interfaces and three formats. All 263
installed-package tests pass without failures or skips, with verified uninstall.

The retained packet is `broad-singleton-index-r1`, built from
`singleton-index-release-r1`. All 3,125 frozen source identities, both runtime
products and 694 baseline/candidate stdout hashes were independently verified
before acceptance. `singleton-index-analysis.json` preserves complete case and
family movements; `singleton-index-accepted-source.json` links the proof. Full43
and portable/hosted acceptance remain combined gates. No competitor was rerun.

The existing `native_relational_index::RowIndex` allocates a separate two-word
link even for a key retained only once. Store that first row directly in its
existing three-word entry. Allocate a link only for each additional duplicate.
Keep the entry size unchanged by using its remaining word as the duplicate tail;
a circular duplicate chain supplies its head through that tail. Append remains
constant work. Iteration emits the inline first row followed by each duplicate
exactly once, in insertion order. Hash collisions still require the same native
equality callback against the first row. No key-domain, uniqueness or density
inference is needed, and arbitrary row ordinals must not become iterator sentinels.

Use the current credited `ReservedVec` owners for entries, links and buckets.
Resolve every fallible reservation and cancellation check before publishing an
entry or duplicate, including the singleton-to-duplicate transition and link
capacity growth. Failed insertions retain the previously visible membership and
duplicate order; all credits remain owned through failure and drop. Existing
unique insertion keeps first-occurrence entry ordinals and allocates no duplicate
state. Rehash preserves entry and chain identities. Do not reopen the dropped
probe-cache/gather-coordinate experiment or combine this with a dense directory.

This is a representation change in the existing shared native owner, used by
joins and condition-join candidate iteration, RowSet membership and grouping,
correlated subqueries, and DISTINCT window state. Keep each consumer's current
NULL, equality, order, multiplicity and source-completion contracts. Pinned Vortex
key/array providers remain authoritative for values; no new array abstraction,
format dispatch, external executor, dependency or unsafe code is required.

Prove the structural change with exact credited-memory and retained-link tests,
including all-unique keys, long duplicate chains, interleaved collisions, missing
keys, zero/one rows and arbitrary ordinals. Deny the first duplicate allocation
and later growth independently; check cancellation and equality errors before
publication, successful retry, and zero remaining credits. Run the full shared
consumer suite plus fresh independent public comparisons across five interfaces
and Parquet, Arrow IPC and Vortex, then installed-package and workspace checks.
The existing public join/source counters must keep their meanings; exact private
container tests establish link elimination without inventing a public metric.

After acceptance, freeze one unchanged complete-cohort pass. Declare all affected
families (joins, multiway joins, composite, distinct, sets, global and grouped aggregation)
and retain every observation. Lower complete engine time retains this candidate;
unchanged or higher drops it. No speed or whole-query memory saving is inferred
from the smaller private representation alone.

## Candidate 6: prepared strings, requested temporal fields and scalar operands

Disposition: **retained locally**, pending combined integration and hosted
checks. One unchanged 347-query pass records **13.274446547 engine seconds**
against candidate 5's **13.433815339**, a reduction of **0.159368792 seconds
(1.19%)**. All 347 complete comparisons and 12,565 result rows pass. This is the
next candidate's accepted comparison baseline; no competitor was rerun.

The strings family records 0.646765917 against 0.752734915 engine seconds,
temporal 0.551815499 against 0.576947250, and numeric 0.245724125 against
0.243036749. Grouped aggregation records 5.505450043 against 5.487348256.
No family crosses the preserved diagnostic regression threshold. Two individual
cases do: `aggregate_optional_value_category_mixed_arithmetic_all` increases
from 0.046734917 to 0.059402541 engine seconds, and
`aggregate_metric_category_sum_arithmetic_all` from 0.033480750 to 0.049414167.
Their complete values, SQL declarations and source observations are unchanged;
the only differing public report field is `native_query_execution_nanos`.
Retain both regressions and every other observation. This single pass cannot
isolate causes or establish a statistical confidence claim; the maintainer's
complete-cohort binary gain rule governs retention.

The retained packet is `broad-prepared-kernels-r1` with runtime
`prepared-kernels-release-r1`. The acceptance verifier independently checks all
3,131 frozen source files, both runtime products and 694 baseline/candidate
output hashes. Every complete result is exactly equal, and all source physical
observations are unchanged. `prepared-kernels-analysis.json` and
`prepared-kernels-accepted-source.json` preserve this verification and both
diagnostic case increases. Full43 remains due at the combined integration gate.

Validation passes 45 focused expression tests, 6,537 native workspace tests
with 35 existing opt-in cases ignored, all 3,289 default workspace tests without
skips, strict native/default lint and formatting. Fresh acceptance covers 360
complete executions and 28,530 rows across five interfaces and Parquet, Arrow
IPC and Vortex. All 264 isolated installed-package tests pass without failures
or skips, with binary integrity and uninstall verified. Preserve the earlier
test-helper lifetime assertion failure, two lint findings and their corrections;
the full checks pass on the final measured source. No runtime source changed
after the accepted build.

Remove repeated scalar dispatch and temporary ownership inside existing shared
expression owners. After the current left-to-right argument evaluation and
complete native-domain preparation, bind unary UTF-8 LOWER, UPPER, TRIM, LENGTH,
OCTET_LENGTH and already-compiled literal REGEX once per batch. Read the prepared
native variable-width values through Vortex's checked borrowed UTF-8 accessor,
retaining existing dictionary-code and domain-validity handling. Do not cache
dictionary results or skip validation of unused dictionary entries. LOWER/UPPER
keep their existing Unicode operations and admitted scratch bounds. TRIM borrows
only until the current expression output builder copies the substring into
independent credited native storage. Binary byte length and other unadmitted
functions retain their current implementation. No source borrow may survive in
the result, and each output value is evaluated once under the existing builder.
Value-only UTF-8 preparation also uses the existing checked borrowed-domain walk
already used by demand hashing, removing an unused byte-owner clone while
validating the same complete domain. Regex preparation retains both evaluated
arguments; the compiled pattern does not remove NULL propagation from its literal
argument. This change does not introduce regex compilation inside a batch or row.

Before expanding binary-expression operands, admit a finite, nonnull primitive
literal on either side when the other input and result have supported native
integer types or exact Float64 types. Keep literal value, type and side scalar;
borrow the other operand's original-width typed slice and validity. Use the
existing i128 integer arithmetic, bound result conversion and floating-policy
helpers without reassociation. Reject optimization admission before evaluating
either child when type, operation, scalar or completed-output reuse conditions
do not match. The admitted nonliteral child retains its current preparation and
completed-output reuse scope. Null/nonfinite literals, temporal/decimal domains
and unsupported numeric combinations retain the existing path. Validate every
present floating operand before arithmetic; preserve overflow, signed zero,
canonical result bits, row order, lazy branch domains and first-error behavior.

In the existing schema-bound temporal Kernel, compute only the requested naive
component after the unchanged source and optional second-operand validation.
Subday fields use exact Euclidean tick arithmetic; weekday uses the absolute
day; calendar fields reuse the current full-range Gregorian helpers. Date32,
Date64 and Time retain restricted-value checks. Any timezone-bearing source
keeps the bound timezone provider and civil conversion, including its range,
DST and error behavior. Do not introduce another calendar implementation or
extend admitted temporal domains as a side effect.

Vortex-first decision: reuse pinned Vortex 0.90 `VarBinViewArrayExt::utf8_slice_at`,
native dictionary/value owners, primitive slices, validity and host allocator;
implement the missing ShardLoom expression dispatch in its current kernel owner.
The vendored accessor checks UTF-8 without cloning a byte-buffer owner. Existing
providers do not supply ShardLoom's exact strict/extended numeric diagnostics or
full-domain temporal semantics, so they do not replace those helpers. No new
dependency, external executor, per-format operator, unsafe code or public API is
introduced. The common native output builder remains the ownership boundary;
`fallback_attempted=false` throughout.

Compare prepared strings with the ordinary scalar implementation over Unicode
expansion/context, combining marks, embedded NUL, long/inline/empty/null values,
actual dictionaries, nullable domains/codes, slices and constants. Preserve
malformed unreferenced-domain and invalid-code rejection. Prove cancellation,
allocation refusal, output lifetime and complete credit release. Compare scalar
numeric operands on both sides and every admitted operation/width with the old
array/scalar helpers, including exact payload bits, null storage, endpoint and
error-order controls, dictionary/constant inputs and lazy branches. Compare every
temporal part/unit with original civil extraction over negative fractions,
leap dates, endpoints, invalid restricted values and timezone controls. Use the
existing debug observation owner to prove route activation, without a new public
counter surface. Add fresh independent cases across five interfaces and three
formats, then required workspace/lint/installed-package checks. Declare all
expression-consuming cohort families, retain every observation and apply the
same complete-cohort binary gain/loss rule. Full43 remains a combined gate.

## Combined review checkpoint and Full43 coverage audit — October 30

The combined C2–C6 runtime, including the optional ranked-admission correction,
passes the unchanged 347-query checkpoint: **13.368176384 engine seconds** against
the fresh composition baseline's **15.305938168**, with all 12,565 result rows
equal and no case/family diagnostic flags. This is a combined integration
observation; it does not replace any original candidate observation or disposition.
The measured runtime source is commit `7d111f51c9c3a362ed51bed13746a77b8f0a81e8`.
Final local checks pass: 6,539 native tests with 35 existing opt-in tests ignored,
3,289 default tests, both strict lint configurations, formatting, 180 public
executions comparing 12,465 rows, and 264 installed-package tests without skips.

One isolated direct-Parquet Full43 pass then completes in **624.450913663 engine
seconds**, with 43 unique queries, one execution each and all 381 result rows
independently checked. The prior same-boundary observation was 651.558161209
engine seconds. Preserve both single observations without a statistical claim.
The source is the original 99,997,497-row Parquet file; no ingest/preparation
command or prepared-input artifact is used. Native/default/package gates,
complete 347 comparisons and full source hashing precede query execution.
The standard 24 GiB / 12-lane / 120-second-per-query settings, lightweight guard,
storage limits and cleanup remain unchanged; all other task work stays idle.

Inspection of the complete saved query reports establishes the following scope:

| Mechanism | Observed Full43 coverage |
| --- | --- |
| Demanded columns | All 43 use direct demand-aware reads; 42 request fewer than the 105 available columns. |
| Statistics pruning | Q2, Q8 and Q20 read 212, 212 and 202 of 226 groups; Q37–Q43 each read three. |
| Rank before wide payload | Q24 ranks two dependency columns across 226 groups, then reads payload from seven winning groups for ten selected rows. The decoded-row counter includes both phases; it is not a unique-row count. |
| Bounded ordering retention | Positive `native_input_ordering_prefix_batches` in 29 queries. This is separate from a source prefix. |
| Source prefix | No query reports a positive requested source prefix; this pass does not prove that mechanism's activation. |
| Ownership and execution | Every query reports source completion, detached output ownership, zero prepared-input writes and no external execution. |

The runner's original summary filter retained `native_source_demand_*` but omitted
`native_source_ranked_*` and `native_input_ordering_*`. Those fields are present
in the preserved raw outputs, including Q24's positive ranked activation. The
filter now retains both families, with single and paired runner tests for exact
values, explicit zero/unavailable values and absent fields. This reporting change
does not change the completed observation, its frozen harness, SQL, values or
clocks, and does not justify another query pass.

Full43 remains a regression workload, not the format-completion gate. Its runner
admits Vortex and Parquet; this observation selects Parquet only. In the current
public source binder, fresh ordinary Parquet queries can admit
`LocalColumnarSource` directly. Arrow IPC/Feather, ORC, Avro, CSV and JSON/JSONL
instead use compatibility preparation to Vortex; their readability does not prove
equivalent direct source demand. Vortex has its own native source boundary.
Full43 also does not cover mixed-format joins, native/compatibility output pairs,
large writes, reopen/reuse or the complete resource/failure matrix. The existing
[item 18 coverage contract](native-universal-io-performance-coverage-2026-10-24.md#cross-surface-and-format-acceptance-gate)
remains open for those obligations. Missing formats, unmeasured combinations and
unsupported combinations must be recorded distinctly. Item 20 and C7 remain open.

The [portable acceptance](../benchmarks/evidence/native-shared-performance-2026-10-30/acceptance.json)
replays all eight complete 347-query observations, the original incomplete
baseline attempt and the final Full43 values. It preserves the dropped C1
decision, five retained candidates, all 134 original process/admission receipts,
their 29 failures/refusals, complete public comparisons and package evidence.
Ten build manifests bind exact source versions and product identities. Four
content-addressed archive parts retain the existing per-part limits; large
records use bounded chunks and no archived code is executed. Large input,
runtime and wheel payloads remain external with frozen identities.

One historical source limitation is explicit: three early nonruntime document
drafts retain their receipt lengths/digests but lack their original bytes. All
pre-execution benchmark contracts and runtime/test source snapshots are retained;
those drafts receive no substitute content or execution-proof credit.

Replay with:

```sh
python -B scripts/verify_shared_performance_packet.py docs/benchmarks/evidence/native-shared-performance-2026-10-30
python -B -m unittest discover -s scripts -p test_shared_performance_packet.py -v
```

The 36 corruption/semantic tests cover unsafe or missing archive entries,
duplicate and reordered chunks, bounded reconstruction/cache behavior, complete
public values and five-interface/three-format grids, actual process/test counts,
and the precisely recorded post-lint formatting edit. Full43 controls repair
transport checksums before attempting changed results, clocks, queries, input
format or hidden ranked activation; semantic replay rejects them. CI runs the
controls and the complete saved-result verifier. Hosted acceptance remains a
separate merge gate. No new query is executed during packaging or replay.
The [follow-up verification](../benchmarks/evidence/native-shared-performance-2026-10-30-followup.json)
retains the packaging failures/corrections and passing control, documentation,
CI-matrix and source-only replay receipts. The separate source-only copy has
identical verifier files and derives the same acceptance without local data or
runtime payloads.
