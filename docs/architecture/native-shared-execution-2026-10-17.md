<!-- SPDX-License-Identifier: Apache-2.0 -->

# Shared execution optimization after native text reconstruction

Status: retained implementation passes local acceptance under PERF-01/04/05/10;
final-head hosted review, wheels and deployment remain merge gates.
The maintainer requests another material whole-workload improvement, with generic
runtime admission and complete validation through merge. The accepted starting
tree is PR #1567, merged at `5bcee25a`; its matched 43-query sum of per-query
two-sample means is 58.1389694790 engine seconds. A new comparison must measure
that accepted runtime again, rather than compare an unrelated later observation
with the historical number. All whole PERF owners and CG-1 through CG-23 remain
open, and immutable 0.6.0 packages are unchanged.

The maintainer subsequently requested a PR before shifting to ingestion size
reduction, then explicitly accepted improvements greater than 3%. The retained
618 runtime measures a 4.206790% whole-workload engine gain. This new acceptance
decision is recorded separately from the original failed 5% gate; neither the
frozen protocol nor any measurement is rewritten. The latest 621 experiment
still fails its resource gate and is removed. Every selected source file is
restored to the measured 618 identity before broad PR validation.

Workspace/native checks, all 100 direct binding methods, 4,827 public cases and
the 18-cohort format/oracle matrix pass. The
[benchmark report and portable packet](../benchmarks/native-shared-execution-2026-10-17.md)
retain every experiment and independently reopen all 1,242 complete comparison
calls. A subsequent two-line comment correction describes the admitted worker
count accurately; its exact byte bridge preserves all runtime tokens and source
line numbers. No measured executable or historical decision is replaced.

The expert comparator is an encoded-columnar maintainer checking physical work,
complete key equality, first-seen identities, native owner lifetimes, allocation
admission and measured complete-query cost. The ordinary SQL, Python, DataFrame
and CLI routes continue to share the same native execution families.

## Attribution and dropped directory screen

Nine serial diagnostic samples execute the accepted native binary for Q19,
Q23, Q29, Q34, Q35, Q10, Q16, Q28 and Q33. All complete results pass, all 2,411
selected source files match the accepted build, and the existing supervisor
reaps its children. Sampling is attribution only. In the Q29 sample,
`Utf8ChunkDictionary::intern_source` has 428 top-of-stack samples, alongside
native decompression, validation and domain parsing. Q19 also exercises that
shared dictionary and persistent partition-table growth. Counts across threads
or nested stacks do not establish exclusive stage timings or a speedup.

The first candidate replaced the shared chunk dictionary's scalar probe with
the already pinned `hashbrown` 0.17.1 `HashTable`. It retained cached full hashes,
exact byte equality, stable first-seen IDs, miss-only ownership and bounded
fallible allocation. Seven focused tests and all 48 complete public calls passed.
The frozen two-target sum of five-sample engine means increased from
10.3804950924 to 10.4340839922 seconds (0.5162% slower). Q19 improved in only
three of five pairs and Q29 in one of five; the 2% combined gain and four-positive
pairs per target requirements failed. The directory replacement is dropped.
Every source patch, build, sample, control, schema and failed gate is retained.
Single-worker and control observations do not override the failed target gate.

## Bind native access at the loop boundary

The same profiles show repeated typed access in text consumers and numeric
grouping. Q10/Q16 exercise `aggregate_direct_integer_key_part`, integer-width
getters and `NativeNumericOwner::integer_key`; Q34 also exposes erased-array
`as_any` calls. These are attribution signals, not accepted speedup measurements.

Vortex-first decision: reuse pinned Vortex 0.85 `Array::as_view`, `ArrayView`,
`VarBinViewData` and `VarBinViewArrayExt`. The owned array's data access performs
an erased-array downcast; its borrowed typed view already stores the checked
data reference. Bind that view once around dictionary preparation, plain text
counting, dictionary hashing, exact merge visits and DISTINCT union. Bind the
partial's native data once for partition reconciliation. No copied buffer table,
new array owner, alternate encoding or unchecked UTF-8 construction is needed.
Checked text access still uses the original provider receipt or validates a
generic builder's selected bytes. Native value retention and independent
promotion of escaping keys remain unchanged.

For single-integer grouped counting, reuse
`aggregate_direct_integer_key_slice` and its original-width typed iterator.
Move width selection outside the row loop, including explicit selections, and
reuse the same shared iterator for existing numeric/text consumers. Materialized
keys keep their existing row-checked path; nullable and unsupported admission,
signed/unsigned identity, exact counts, overflow and partial failure order remain.
There is no value-column copy, transformed-key rule or query-specific condition.

The complete 70-call screen passes every value and schema check. Numeric targets
Q10/Q16 reduce their sum of five-sample engine means from 5.1272616510 to
3.7355836432 seconds (27.14%), with all five paired totals improved; this binding
is retained for the batch. The text family increases from 23.2545347217 to
23.4655935560 seconds (0.91% slower) and fails its frozen 2% family gate. Remove
the text binding changes and retain their source, tests, calls and failed gate.
Single-worker observations do not override that decision. The exact mixed
candidate is not accepted, despite its seven-target combined gain of 4.16%.

## Compact complete numeric-pair records

The accepted Q33 profile attributes substantial work to complete-partition pair
sorting. The existing generic reducer retains two u64 value fields plus two
signedness bits in a naturally padded 24-byte key. The earlier
[complete-pair design](q33-exact-partition-reduction-2026-09-19.md) identifies
this storage cost; no prior compact-record comparison is reused as proof.

Test a 17-byte internal record containing the kind byte followed by both value
fields in big-endian order. Bytewise ordering must exactly equal the existing
`(key_kinds, first_bits, second_bits)` tuple order; full-key equality and final
signed result ordering remain unchanged. Store the complete kind byte per row:
the current worker accepts changes in integer width or signedness between
chunks and must not collapse distinct signed and unsigned keys. Reconstruct
the existing key only for routing or complete-group candidate selection.

Vortex-first decision: `implement_shardloom_kernel` within the existing shared
pair reducer. Pinned Vortex 0.85 primitive slices, validity, scan and session
providers stay in use. Vortex's crate-private fixed-width structural take/filter
trait is not an aggregate COUNT-ranked reconciliation provider. This internal
leased record changes no input array, persisted encoding or provider boundary.
Reuse the current 64 partitions, native unstable sort, jobs, sampling, admission,
late-measure pass and comparator; no new sort implementation, dependency,
unsafe access, query condition or alternate execution route is introduced.

Charge staging, persistent capacity and old-plus-new growth overlap using the
actual compact record size. Preserve cancellation, committed-denial failure,
lease lifetime, sample decline and same-source provider restoration. Prove
round-trip and tuple-order equivalence over all signedness combinations,
extremes and deterministic varied bits; compare mixed-width/mixed-sign chunks
with native serial results and verify exact memory charge and denial cleanup.
The focused retain gate requires at least 5% lower Q33 mean engine time and
four of five positive pairs, plus unchanged controls and independent numeric
binding revalidation. The final 5% whole-workload gate below is unchanged.

The complete 46-call screen passes values, schemas and its frozen gates. Q33
improves by 5.21% with five of five positive pairs; the single-worker control
also passes. Q10/Q16 retain 22.39% and 31.60% improvements over the accepted
baseline, with three of three positive pairs each. Retain both mechanisms for
the batch. These targeted observations do not establish a whole-workload gain.

## Bind group identities in the existing typed aggregate block

The accepted Q28 diagnostic attributes repeated work to
`grouped_key_for_direct_row`, `NativeNumericOwner::distinct_value` and group
metadata lookup while the existing AVG/COUNT block already binds measure type
and validity once. Test binding the admitted single integer group owner at that
same block boundary, retaining its original-width slice and validity mask.
Reuse the ordinary single-key identity and the existing shared group-state
insertion/update routine. A borrowed row-value kernel avoids re-reading group
metadata or re-downcasting the native array for each row. It must not cache
cross-block data, copy a value column or construct a parallel aggregate state.

Vortex-first decision: wrap the already admitted Vortex 0.85 primitive-slice
and mask providers inside `NativeNumericOwner`. The existing numeric block's
shape, lengths, measure order, nullable grouping, first-seen limits, HAVING,
failure order and evidence counters remain unchanged. Do not introduce a new
query route, query name, field name, dtype restriction or dependency. A single
small block-local borrowed closure follows the existing bound numeric kernels.

Compare complete calls with the retained compact-pair build as control so this
mechanism has independent attribution. Freeze Q28 at five pairs with an 8%
mean engine reduction and at least four positive pairs, plus two single-worker
pairs and unchanged-path controls. Preserve the existing 10%/0.15-second control,
CPU and RSS checks and every slower observation. At least the current focused
matrix must prove all original group widths, signed/unsigned extremes, native
validity/slices, reordered/duplicate selections, closed first-seen limits,
cross-block type changes and identical partial state on errors.

The complete 50-call screen passes every value and schema check. Q28 improves
by 29.84% with five positive pairs, and its single-worker control improves by
29.74%. However, unchanged-path Q10 increases from 2.0945095835 to
2.3091059380 mean engine seconds, exceeding the frozen 10%/0.15-second control
limit; both paired observations are slower. The complete candidate is dropped,
including its group binding code and tests; their frozen patch and evidence remain. No explanation
for that control difference is established, and the successful target does not
override the failed acceptance gate. All 2,411 selected source identities are
restored to the previously retained numeric-binding/compact-pair build.

## Store occupied string counts densely

The accepted Q34/Q35 profiles place substantial samples in canonical string
partial counting, zero initialization and the final occupied-entry compaction.
Test a dense first-seen vector of the existing `CountSlot<W>` records, with a
temporary `hashbrown::HashTable<W>` directory containing only vector ordinals.
Retain the existing native values owner, complete cached hashes and exact byte
equality; validate new UTF-8 keys before publication. Do not copy string payloads
or construct per-row dictionary IDs. Native Dict and Constant kernels stay intact.

Vortex-first decision: `implement_shardloom_kernel` inside the existing shared
string-count partial. Vortex 0.85 VarBinView, validity, execution-session and
encoded Dict providers remain the input boundaries; none provides this admitted
exact aggregate partial and its coordinator contract. Reuse the already pinned
hashbrown 0.17.1 public fallible reserve/find/insert APIs, with no dependency
change or copied implementation. The existing partition reducer, complete-key
merge, pressure handling and late output remain shared.

Keep the original `partial_bytes` admission and held lease. Before allocation,
bound dense records plus directory within that reservation. The directory bound
uses at least 16 buckets and the existing twice-row power-of-two bound, one
control byte per bucket, and 32 bytes for alignment/control suffix. This safely
exceeds the pinned provider's maximum 16-byte control groups and 7/8 load policy
for u32/u64 ordinals. Check actual reserved capacity/allocation size afterward.
If the combined bound does not fit for a small chunk, use the existing sparse
kernel before doing any count work. Both allocations are fallible and fully
pre-reserved; insertion cannot grow. Release the directory before handing off
the partial, but keep the original conservative capacity charge through merge.

Prove compact/wide equivalence, forced complete-hash collisions, Unicode/NUL and
empty keys, first-seen order after partitioning, capacity-bound edges, cancellation,
allocation denial and checked work-counter failure. Compare complete operations
against the retained numeric-binding/compact-pair build. Freeze five pairs each
for Q34/Q35: require at least 5% lower combined target engine mean, at least four
positive pairs for each target, and no target mean regression. Add two pairs each
for Q34/Q35 at one worker and unchanged-path controls Q2/Q10/Q16/Q19/Q23/Q28/Q29/Q33.
The existing 10%/0.15-engine-second control limit applies to all controls;
target CPU and every case's RSS retain their existing limits. Preserve every
failed/slower observation; the final whole-workload 5% gate remains unchanged.

The complete 60-call screen passes all results and schemas but fails retention.
The combined Q34/Q35 mean improves by 4.21%, below 5%; Q35 improves in only three
of five pairs. Q10's mean increases by 17.27%, exceeding its unchanged-path
control limit. All CPU/RSS checks pass. The candidate code and tests are removed;
its frozen source patch, design, binary and every measurement remain retained.
The repeated Q10 control difference requires investigation before another
candidate; no causal explanation or whole-workload improvement is established.

The follow-up inspection finds no differences in the recorded non-clock Q10
runtime fields, build command, package features, compiler profiles or dependency
artifacts. Three inspected numeric access functions have identical normalized
instruction sequences; the fourth differs in two constant-address offsets. This
limited inspection does not prove equivalent generated programs or identify a
timing cause. Both failed candidates remain dropped; no measurement is discarded
or reclassified as environmental noise.

## Keep integer group identity outside count-map entries

The next coherent numeric-group candidate changes the shared count table itself.
The current `(u64, bool)` key occupies 16 bytes; together with its u64 count,
each occupied entry requires 24 bytes before the existing hash-table overhead.
Separate signed and unsigned maps retain the same exact identity with a u64 key
and u64 count per entry. Bind the map once for an original-width native slice;
mixed materialized keys select their map explicitly. Preserve complete signed
and unsigned domains, checked counts, fallible growth, generic demotion, exact
winner selection and the existing admission boundaries. Per-key storage estimates
must describe the new representation without claiming allocator or RSS coverage.

This changed count-table mechanism is measured together with typed group binding
in the existing AVG/COUNT block. The prior group-binding candidate remains a
failed standalone observation; it is not reused as acceptance evidence. There
are no column names, values or query numbers in runtime admission. Row/measure
order, nullable semantics, source-order limits and partial failure state remain
unchanged. Native slices remain borrowed from their original Vortex owners.

Vortex-first decision: `implement_shardloom_kernel` in the existing native
aggregate state and numeric block. Pinned Vortex 0.85 primitive arrays, validity,
scan and execution-session providers remain the input boundary. Upstream array
providers do not supply ShardLoom's persistent count-ranked group state, mixed
signed-domain identity or late-measure winner contract. Reuse the existing
`FxHashMap`, fallible reservation helper, count workers and candidate comparator;
add no dependency, unsafe code, array copy or external executor.

Before execution, freeze five counterbalanced pairs each for Q10/Q16/Q28 at
twelve workers. Require at least 8% lower sum of their mean engine times, no
individual target mean regression and at least four positive pairs per target.
Q10 therefore receives five pairs and a stricter nonregression gate in this new
mechanism. Run two pairs per target at one worker and two pairs for unchanged
Q2/Q9/Q19/Q23/Q29/Q33/Q34/Q35 at twelve workers. All controls retain the existing
10%/0.15-engine-second limit; target CPU and all RSS limits are unchanged. Every
value, schema, native-provider and no-fallback check must pass. The final
whole-workload 5% rule below remains unchanged.

The complete 74-call screen passes every value and schema check but fails
retention. Combined target engine time improves by 12.44%; Q16 and Q28 improve
by 5.22% and 27.82%, respectively, in all five pairs. Q10 is nearly unchanged
(0.08% lower mean) and improves in only three pairs. Q34 and Q35 controls regress
by 17.22% and 11.94%, with both pairs slower. All CPU/RSS gates pass. Drop this
complete candidate, preserving its source, design, binary and all measurements.
The prepared Full43 comparison is not run. No whole-workload improvement is
established. Further work must change the mechanism; the existing aggregate
state representation is restored before another candidate is designed.

## Bind group preparation without changing aggregate-state layout

The next candidate preserves the existing single numeric count map, complete
`(bits, signed)` key, and aggregate-state field layout. Its internal hash folds
signedness into one u64 before invoking the pinned `rustc-hash` 2.1.3 hasher.
Unsigned bits pass unchanged; signed bits are XORed with one fixed domain tag.
Within each domain this is bijective. Cross-domain hash collisions remain
possible and must use complete key equality; this hash is neither persisted
nor an identity. Result ranking continues to use the explicit full-key
comparator. Test deliberate equal-hash pairs through counting, worker merge,
overflow, generic demotion and bounded ordered results.

Bind the integer group owner in the admitted AVG/COUNT block, but replace the
previous optional bound-key parameter with a generic key-preparation closure.
The ordinary path supplies its existing key/value preparation; the numeric
block supplies its borrowed typed key getter. Both instantiate the same group
admission and state-update body, with no runtime optional-key dispatch or
aggregate-state field added. Preserve nullable grouping, row/measure order,
closed first-seen limits, error ordering, chunk ownership and work counters.
The earlier failed group-binding and two-map candidates remain rejected.

Vortex-first decision: wrap the existing Vortex 0.85 primitive-slice and mask
providers for block-local access. ShardLoom still owns exact aggregate hashing,
group admission and result ranking. No upstream query-engine integration,
dependency, new input representation, column-sized copy or unsafe code is used.
The pinned hasher source confirms one arithmetic update per u64 write on the
local 64-bit target; its existing provider handles 32-bit targets normally.
No timing cause is inferred from the earlier control failures.

Freeze five counterbalanced pairs for Q16/Q28 at twelve workers, requiring an
8% reduction in their combined mean engine time, no individual mean regression
and at least four positive pairs per target. Their single-worker controls use
two pairs. Q10 uses the separate mixed-DISTINCT state, not the changed count-map
key; it is an unchanged-path control in this candidate, as are Q34/Q35. Give
each of these three controls five pairs, retaining the existing 10%/0.15-engine-
second limit. Q2/Q9/Q19/Q23/Q29/Q33 receive two control pairs. All 82 complete
calls must pass values, schemas, native-provider and no-fallback checks; target
CPU and all RSS gates remain. The independent 5% Full43 acceptance gate below
does not change. Preserve every sample, including failed/slower observations.

The focused suite passes 108 tests, including deliberately colliding signed and
unsigned keys, worker-weight failure order, generic demotion, ordered offsets,
all original integer widths, nullable and sliced groups, selected row order and
cross-block signed identity. Strict library Clippy and formatting checks pass.
The complete 82-call screen passes values and schemas but fails retention.
Q16/Q28 improve by 1.15% and 26.34%, respectively, with five positive pairs each
and a 15.57% combined mean improvement. Q10 and Q35 pass their controls, but Q34
regresses by 11.95%, with four of five pairs slower. All CPU/RSS gates pass.
The entire candidate is dropped; its source, design, binary and all observations
remain. The prepared Full43 comparison is not run. Preserving the state layout
did not eliminate control regressions, and no causal explanation is established.

## Count canonical strings in occupied table entries

The next candidate changes only the shared canonical string partial. Store each
existing `CountSlot<W>` directly in a pre-reserved `hashbrown::HashTable`, then
consume only occupied entries into the existing partial vector. This avoids
zero-initializing empty count slots and scanning them for compaction. An entry
lookup shares probing between update and insertion. The native values owner,
cached full hash, full byte equality, compact/wide words, partitioning and
explicit first-key/result ordering remain. Dict and constant counting remain
on their encoded paths. No aggregate-state representation changes are included.

The pinned hashbrown 0.17.1 public fallible reservation and entry APIs suffice;
there is no dependency update or unsafe code. Its flat table is an internal
ShardLoom count directory, not an execution provider. Existing Vortex 0.85
VarBinView/Dict/constant and execution-context providers remain the data boundary;
upstream grouped kernels still require already-grouped arrays.

Before queueing, reserve a conservative table bound plus a worst-case occupied
vector. For these 16/24-byte records, the pinned table requires no more than the
next power of two above twice the row count, with at least 16 buckets, one
control byte per bucket and 32 bytes for alignment/control padding. Empty input
needs no allocation. Verify actual table allocation and capacity after fallible
reservation; the row bound prevents growth during entry insertion. Keep both
allocations charged during extraction, then release temporary/excess credits
and retain only the occupied vector's exact capacity lease. All errors release
private state. Test one-byte-short admission, allocator bounds, cancellation,
complete-hash collisions, Unicode/NUL/empty keys, compact/wide equivalence,
partition/order behavior, projected dictionaries and final lease cleanup.

Freeze five counterbalanced pairs for Q34/Q35 at twelve workers, requiring a
5% reduction in their combined mean engine time, no target mean regression and
at least four positive pairs per target. Use two single-worker pairs per target;
Q10 receives five control pairs, and Q2/Q16/Q19/Q23/Q28/Q29/Q33 two each. All 66
complete calls must pass values, schemas and native/no-fallback checks. The
existing 10%/0.15-second control, 10% target CPU and 25%/256-MiB RSS gates remain.
The independent 5% Full43 gate remains unchanged. Preserve all observations and
drop the entire candidate on a failed gate; no additional repetitions or timing
cause is inferred from earlier candidates.

The focused correctness gate passes 80 tests, including all four new table
allocation/collision/admission/failure tests and existing worker-pressure,
partition, ownership, constant/dictionary and ordering coverage. One existing
file-supplied diagnostic remains explicitly ignored. Strict library Clippy,
formatting and diff checks pass. All 66 complete-operation results and schemas
match, and every control, CPU and RSS gate passes. However, the target combined
mean engine improvement is 2.86%, below 5%; Q34 improves 4.81% in four pairs and
Q35 improves 0.83% in only three. Drop this complete candidate, preserving its
source, design, binary and all observations. Its contingent Full43 plan is not
run. The retained numeric binding and compact complete pair records now enter
their full-workload acceptance comparison, with all runtime files verified
against the retained candidate and only the CI validator marker differing.

The complete 172-call comparison passes all values, schemas, native evidence
and resource-policy equality checks. Its sum of two-sample per-query engine
means falls from 63.3521125675 to 62.1332405780 seconds (1.923964%); both paired
Full43 totals improve and no query crosses the material regression threshold.
This fails the unchanged 5% material-gain gate and does not accept the batch.
The two focused mechanisms remain a measured base for a changed mechanism,
not a whole-workload success claim. The final scorer required two reporting
repairs: the raw clock is named `native_query_execution_nanos`, and the existing
point-lookup route omits three optional operator-policy fields. The corrected
scorer verifies the maintained clock extractor and identical field presence
and values across both builds and pairs. Original scripts, all 172 calls,
thresholds and measurements remain; no query is repeated for these repairs.

## Compact the shared string lookup directory

The accepted-runtime profiles identify shared chunk string interning and its
directory probes in Q19/Q29. The original scalar directory stores a complete
64-bit hash, a 32-bit value ID and an occupancy flag in a padded 16-byte slot.
The earlier hashbrown replacement remains rejected. Test the existing scalar
probe algorithm with an 8-byte slot: a nonzero 31-bit hash fingerprint and the
complete u32 ID. Keep the full 64-bit hashes in a dense vector indexed by the
first-seen ID for growth. A fingerprint only rejects mismatches; complete byte
equality still decides every match and proves duplicate UTF-8 validity.
The occupied tag must preserve every possible u32 ID, including `u32::MAX`.

Rehash occupied dense hashes on growth, retaining the same bucket function,
load threshold, linear probe order, initial capacity and first-seen IDs. Keep
miss-only source-owner retention, UTF-8 rejection, and exact copy/ownership
counters. Bound and fallibly reserve the hash vector together with the existing
values and slot vectors. The new row bound must include every allocation and
must not exceed the old bound: each bucket saves eight bytes and each possible
occupied value adds eight bytes, while the bucket count is at least the row
count. No temporary value copies, input representation, public type, dependency,
unsafe access or query condition is introduced.

Vortex-first decision: `implement_shardloom_kernel` inside the existing shared
chunk dictionary. The pinned Vortex 0.85 VarBinView/Dict/validity/session providers
continue to supply native data and ownership. They do not supply this admitted
first-seen aggregate dictionary and its worker budget contract. Existing scalar
DISTINCT, dictionary preparation and grouped consumers share the same change.
Native encoded dictionaries keep their existing path.

Freeze five counterbalanced pairs for Q19/Q29 at twelve workers, requiring at
least 5% lower combined mean engine time, no individual target mean regression
and four positive pairs per target. Two single-worker pairs per target and
five pairs each for Q10/Q34/Q35 are controls. Q2/Q5/Q6/Q9/Q16/Q23/Q28/Q33 receive
two control pairs each. All 90 complete calls must pass values, schemas and
native/no-fallback checks. The existing 10%/0.15-second control, 10% target CPU
and 25%/256-MiB RSS gates remain. Compare against the exact measured 611 build;
the whole-workload material acceptance rule below is unchanged. Before timing,
prove full-hash and fingerprint collisions, all ID bits, UTF-8 failure without
publication, first-seen order through growth, no duplicate owner retention,
exact bounded capacities and worker/ownership cleanup.

All 37 focused tests pass, including three new compact-slot/collision/capacity
tests and the existing dictionary preparation, scalar DISTINCT and ownership
tests. Strict library Clippy passes. All 90 source-bound complete comparisons
pass values, schemas and native checks. Combined Q19/Q29 mean engine time falls
from 10.0515262502 to 10.0066475334 seconds (0.45%); each target improves in only
three of five pairs. Both targets fail the frozen improvement/consistency gate.
Controls and resource gates pass. Drop the complete compact-directory change;
retain its original patch, binaries, samples, validation and decision evidence.

## Prepare native text inside the bounded worker grant

The accepted-runtime Q29 profile places 1,086 of 1,820 main-thread stack samples
under `aggregate_canonical_utf8_chunk`, including 639 Zstd decoder samples. These
are overlapping stack samples, not exclusive timings. The source performs native
canonicalization on the caller before submitting dictionary construction to one
background worker, regardless of larger CPU grants. The existing count workers
already canonicalize native text inside bounded jobs with the admitted session
and shared memory pool.

For a host-capped CPU grant of at least four, reserve one caller and one native
provider-progress lane and use the remaining grant for dictionary preparation.
Move native canonicalization into each job before dictionary construction. Bound
outstanding jobs to the preparation worker count plus one. Preserve the existing
caller canonicalization, one preparation worker and two-chunk window at grants
two and three; single-worker execution keeps its current route. Admission uses
the existing operator/type proof, with no query or data-value conditions.

Keep source-ordered consumption, first-seen IDs, aggregate update order, native
Dict bypass after draining, maximum chunk rows and native source ownership.
Reuse `AggregateChunkJobs`, its leases, cancellation, first-failure handling and
teardown. Pre-admit the dictionary bound plus native input-byte estimate. The
same admitted provider allocator charges native buffers. If canonical bytes
exceed that estimate, grow the task lease before allocating dictionary metadata.
A post-admission failure aborts without replay. Initial capacity denial drains
and retires workers before the unchanged serial consumer handles untouched
input. Preserve the operation's native I/O drain and provider-driver lifetime.

Keep the caller provider timer's meaning and add a separate summed worker
provider timer. Report actual worker count, window and CPU ceiling; describe
overlapping elapsed stages accurately without claiming exclusive CPU or an RSS
bound. The window metadata uses the existing bounded-worker reservation pattern.

Vortex-first decision: `use_vortex_native_provider`. Reuse pinned Vortex 0.85
sessions, execution contexts, ArrayRef/VarBinView execution and the registered
native allocator inside the existing worker boundary. No decoder, source, scan,
scheduler, dependency, Arrow execution route or external engine is added.
Metadata/pruning and encoded Dict admission still precede canonicalization.

Prove ordinary, prepared and owned values across small and larger CPU grants,
out-of-order worker completion with ordered results, provider allocation denial
inside an admitted worker, no same-operation replay, fresh-operation recovery
and exact refund. Extend pressure retirement, active cancellation, committed
source errors, native Dict interleaving and zero-spare-credit pruning coverage.

Freeze one 98-call screen against the retained 611 runtime: five Q29 pairs at
twelve workers; two Q29 control pairs at each of one, two, three and four workers;
five control pairs each for Q10/Q19/Q34/Q35 at twelve workers; two control pairs
each for Q2/Q5/Q6/Q9/Q16/Q23/Q28/Q33 at twelve workers. Require 5% lower target
mean engine time and four positive pairs, with the same control, CPU and RSS
bounds as the previous screen. Complete values, schemas, native execution and
source identities must pass. The final material Full43 gate remains unchanged.

All 33 focused dictionary, bounded-job and UTF8 ownership tests pass on the
frozen source, including the two new concurrency/failure fixtures. Ordinary,
prepared, owned, pressure, cancellation and committed-source-error fixtures cover
grants through twelve, capped to the host. Strict library Clippy and formatting
pass after one redundant-method-closure style correction; the tests pass again
on that exact source. All 98 complete comparisons pass values, schemas, native
clocks, worker-role evidence and equal resource policy. Q29 mean engine time
falls from 4.9547498506 to 2.9783612918 seconds (39.89%), improving in all five
pairs. Every engine control and RSS gate passes, but target CPU increases by
26.54%, exceeding the frozen 10% limit. Drop the entire candidate and preserve
its source, binary, design, diagnostics and all measurements. The retained 611
source is restored and verified before designing another mechanism. Do not run
the contingent Full43 plan or infer a cause from overlapping stage timers.

## Reduce dictionary-local numeric measures in preparation jobs

The retained path canonicalizes on the caller, builds a first-seen dictionary
in one worker, then counts dictionary codes and reduces transformed groups on
the caller. Keep native canonicalization and provider progress in those original
locations. For grants of at least four, let at most two preparation workers also
perform the existing dictionary-local numeric reduction. Bound the window to
three chunks, reserving the caller and one provider lane in the same grant.
Smaller grants and UTF8 MIN/MAX measures retain the original preparation route.

The added recipe admits only the existing non-null, ordered URL-domain group
with COUNT and length SUM/AVG/MIN/MAX measures, no spill, no source-order group
admission cap and no competing compact-measure plan. This is an operator/type
proof; query names, aliases and data values do not select the route. Encoded
native Dict chunks still drain earlier jobs and use their existing consumer.
The existing sampled chunk-partial criterion remains. When it declines, return
the original accessor to the original caller consumer. Do not regroup a chunk
whose existing path applies dictionary values directly: that would change the
floating accumulation order.

Build counts in dictionary-ID order and reduce values in their original order,
using the existing weighted numeric state update. Retain each group's source
value index and numeric partial; no string payload is copied or promoted in a
worker. Keep the native dictionary owner through ordered consumption. The caller
validates the original dense-plan admission and merges each partial with the
existing checked merge operation. Preserve per-group floating arithmetic order,
count overflow behavior, HAVING, ordered result ties/offsets and independent
persistent key ownership. Source-order admission remains excluded; ordered
result ties continue to compare complete key values rather than internal IDs.

Use the already pinned hashbrown 0.17.1 fallible map API and Fx hasher for the
temporary directory, with full string equality. Pre-admit conservative row-bound
capacity for counts, sampled-domain scratch, the temporary map and output vector,
in addition to the original dictionary/native-byte reservation. Check actual
map allocation and vector capacities against those bounds. Initial denial drains
and retires the stage before serial consumption; post-admission failure or
cancellation aborts without reopening or replaying source data. Add separate
worker-reduction diagnostics and report the actual pool/window/CPU ceiling.

Vortex-first decision: `implement_shardloom_kernel` within the existing native
dictionary preparation stage. Vortex 0.85 still owns canonicalization, native
buffers, validity, scan and the admitted allocator. Its grouped kernels require
pre-grouped arrays and do not provide this ordered persistent aggregate-state
merge. Reuse the existing native dictionary and aggregate formulas; add no
dependency, decoder, input layout, unsafe code or external executor.

Before timing, prove complete numeric measures against the unchanged consumer,
including precision-sensitive weighted sums, domain collisions, empty and
high-cardinality decline, malformed IDs, capacity denial, cancellation, ordered
multi-chunk/native-Dict interleaving, pressure retirement, native source failure,
ordinary/prepared/owned results and source-credit release. Preserve the original
UTF8-extrema ownership tests and verify that those plans stay on their old path.

Freeze the same 98-call cohort and all thresholds as the rejected preparation
screen, against the retained 611 build: five Q29 pairs at twelve workers; two
Q29 pairs at grants one, two, three and four; five Q10/Q19/Q34/Q35 control pairs;
and two Q2/Q5/Q6/Q9/Q16/Q23/Q28/Q33 control pairs. Require at least 5% lower Q29
mean engine time, four positive pairs, the unchanged 10% target CPU limit,
control/RSS limits and all complete values/schemas/native evidence. A retained
candidate then enters the unchanged independent 5% Full43 gate. Preserve all
results and drop the whole candidate on any failed gate.

The feature gates, execution/Native I/O certificates and materialization
boundaries remain; `fallback_attempted=false` and `external_engine_invoked=false`
are required. PulseWeave, capillary units and dynamic admission keep their
existing grants. Metadata/pruning and encoded providers still run before these
canonical boundaries. Late materialization and evidence-tier controls remain.
The earlier dropped capacity and triple-sort experiments remain dropped.

The numeric-only implementation passes 38 focused tests and strict native
all-target Clippy, but its proposed performance target was incorrect. The
canonical Q29 also requests MIN(Referer), which this recipe deliberately
excludes. Its recorded candidate diagnostics confirm that no worker partial
was used. Stop the screen after discovering the mismatch: 18 calls in three
completed cohorts pass values/schemas; the remaining cohort is interrupted
and the supervisor drains all children and releases the lock. Preserve every
measurement, source and binary, but accept no performance conclusion. The
conditional Full43 comparison is not run.

## Carry dictionary extrema by source index

Extend the same bounded dictionary-local reduction to the existing dense plan's
UTF8 MIN/MAX measures. A temporary indexed borrowed value contains its original
dictionary index and a reference to the checked native value. Its string
comparison uses the existing weighted-state update, with identical null,
count-overflow and floating arithmetic order. The completed record stores only
the winning source indices beside the numeric partial. It retains the unchanged
dictionary accessor through the ordered caller merge. The caller resolves those
indices and uses the existing checked merge, promoting only persistent winners.
No worker copies or promotes a string; no borrowed reference escapes its owner.

The operator/type admission remains the existing non-null ordered domain group,
COUNT, length SUM/AVG/MIN/MAX and source UTF8 MIN/MAX, no spill, no source-order
group admission cap and no competing compact plan. At host-capped grants of at
least four, admit two preparation/reduction workers and one provider lane plus
the caller, with a three-chunk window. Smaller grants retain the original
preparation route. Native Dict chunks and a declined partial-cardinality sample
retain their original consumers. Query IDs, aliases and values never select
the route. Account for the larger indexed borrowed-state table and optional
winner indices in the same conservative pre-admission bound.

Vortex-first decision remains the existing ShardLoom aggregate-state reduction
over Vortex 0.85 native dictionaries and checked string buffers. Reuse the pinned
providers and existing state formulas; add no decoder, dependency, input layout,
unsafe access or external executor. This changes the failed screen's actual
mechanism/admission, rather than reusing it as performance evidence.

Before any measured comparison, run the unchanged canonical target SQL on a
small native input through both frozen public CLIs and assert the candidate's
worker-partial route is exercised. Compare complete values and schemas. Extend
focused proof to source-backed UTF8 minimum/maximum winners, same minimum and
maximum, forced full-hash collisions, independent ownership after source drop,
ordered multi-chunk merge, cancellation and pressure. Keep numeric precision,
ties/offsets, native-Dict interleaving and failure/no-replay coverage.

Freeze a new independent 98-call protocol against build 611, with the same target,
controls, pair counts, 5% target engine/consistency requirement, 10% CPU limit,
RSS limits and 5% independent Full43 gate as the preceding design. No result from
the interrupted numeric-only screen counts toward this candidate's acceptance.

The extrema implementation passes 39 focused tests, strict native Clippy and
the unchanged canonical target on a small native fixture. All 98 screen calls
pass complete values/schemas and the resource gates. The target mean improves
from 5.196611 to 4.552338 engine seconds (12.40%), with all five pairs improved.
Retain this component. The subsequent independent 172-call Full43 comparison
passes correctness and regression checks, with both paired totals improved,
but its mean total falls from 57.310192 to 54.899272 engine seconds (4.21%).
It fails the original 5% material gate. At that point the batch is not accepted.
The later explicit maintainer decision above admits this measured improvement
under a greater-than-3% bar, subject to the remaining validation and PR checks.

## Keep native preparation and reduction in the same bounded jobs

The retained two-worker reduction still leaves native canonicalization on the
caller. Move that existing provider operation into the same two jobs when the
dense partial recipe is admitted. Preserve the fixed two-worker, one-provider,
one-caller CPU partition and three-chunk window at grants of at least four.
This differs from the rejected preparation candidate's larger worker pool and
caller reduction: native preparation, dictionary building and local reduction
now consume the buffers in one job. Smaller grants keep caller canonicalization.

Pre-admit the encoded input, existing dictionary bound, dense partial bound and
completed record. Canonicalize with the original Vortex session and allocator,
check rows and dtype, then grow the task reservation for a larger canonical
buffer before allocating dictionary metadata. Keep the original native owner
only through canonicalization. A post-admission provider or reservation failure
aborts the operation and drains its jobs; never replay that operation. Initial
admission denial drains and retires the pool before the untouched source chunk
uses its original serial consumer. Native Dict chunks keep their original route.
Completed dictionary owners and source indices remain charged until ordered
merging finishes. Report caller and worker provider times separately as
overlapping stage observations, not exclusive CPU or complete engine time.

Reuse pinned Vortex 0.85 canonicalization and the admitted session/provider
boundary. Add no source format, decoder, external executor, or query-specific
condition. Existing PulseWeave/capillary admission, metadata pruning, encoded
providers, materialization and no-fallback certificates stay authoritative.

Retain the focused semantics and ownership checks; add real deferred native
provider execution, post-admission allocation denial, no replay, credit release
and fresh independent retry. Verify ordinary/prepared/owned execution at all
grants and out-of-order jobs merged in source order. Before timing, run the
unchanged canonical SQL and require both worker provider and partial evidence.

Freeze a new 98-call screen against build 611, preserving every cohort and
threshold from the previous screen, including the 10% CPU limit against that
same control. Do not move the control to the retained partial build. Only a
passing screen enters a fresh Full43 comparison against build 594 under the
unchanged 5% material and query-regression gates. Preserve all earlier failures.

All 40 focused tests and all 98 complete screen calls pass correctness. The
target mean improves from 5.170966 to 3.605105 engine seconds (30.28%), with all
five pairs improved. Its frozen resource-efficiency check fails. Drop this
whole experiment and preserve its source, tests, binaries and observations;
its conditional Full43 comparison is not run. Restore every Rust source to
the previously measured 618 version before PR validation. The maintainer's
revised whole-workload gain bar does not waive this component's resource gate.

## Acceptance and retain/drop rule

Prove borrowed typed access against owned native access for selected multi-buffer
inline/outlined, Unicode, NUL and empty text. Preserve exact IDs, hash-collision
checks, malformed UTF-8 rejection, source-owner release and escaping ownership.
Check original integer widths and extremes, signed/unsigned identity, reordered
and duplicate selections, empty input, nullable rejection, materialized selected
keys, mid-selection bounds errors and exact partial state on count overflow.
Retain compact/wide partial equivalence, cancellation, one/multi-worker and
decoded-reference fixtures.

Each retained mechanism needs focused correctness and a frozen, unprofiled
complete-operation comparison. Preserve every attempt, failed gate and slower
sample. Related mechanisms may form one coherent batch, but individual focused
gains are not added into a whole-workload claim.

Final material acceptance uses the existing paired runner with all 43 queries,
two executions per query per build, counterbalanced ABBA/BAAB order, the same
resident native input, resource policy and complete results/schemas. The primary
score is the sum of per-query means of `engine_execution_seconds`. Require at
least 5% lower total, improvement in both paired totals, and no query whose mean
increase exceeds the larger of 10% and 0.15 engine seconds while both paired
deltas are positive. A smaller or inconsistent result requires a changed
mechanism or a drop, without changing that frozen rule. Retain all other clocks
and resource observations separately as diagnostics. No pacing, cache purge,
automatic retry or best-sample replacement is permitted.

The paragraph above records the original, predeclared measurement rule. On
October 17, after the complete 619 result and its failed 5% decision were
reported, the maintainer explicitly stated: "anything over 3% should be ok".
Apply that revised acceptance bar to the retained 4.206790% observation without
rerunning or relabeling the original gate. Correctness, both improved paired
totals, query-regression and component resource checks remain required.
The separate `native-shared-execution-acceptance-decision-624.json` binds this
authorization to the unchanged 619 review. It authorizes this optimization PR,
not package publication or a competitive superiority claim.

At the batch boundary, run required workspace/native gates, direct bindings,
public workflow and format/oracle coverage, align documentation and generated
site content, and complete PR checks, final-head review, installed wheels and
production verification. Existing storage, workload and process-cleanup guards
remain required. No new benchmark/search framework or package release belongs
to this batch.
