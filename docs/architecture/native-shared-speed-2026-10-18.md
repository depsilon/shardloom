<!-- SPDX-License-Identifier: Apache-2.0 -->

# Shared native text filter/projection decoding

Status: text filter/projection reuse passes local acceptance under PERF-01/04/05/10.
The matched all-43 engine comparison passes the prospective greater-than-3% gate;
the earlier mixed-key experiment remains rejected. After the native text-size
screen rejected all five alternatives, the maintainer redirected immediate work
to shared engine speed. The control runtime is PR #1571, merge `d2e6867d`.
Its retained 618 executable and exact comment-only source bridge remain the
control. The new ingest benchmark sources are test-only and change no production
execution. Broader PERF owners and CG-1 through CG-23 remain open.

The expert comparator is an encoded-columnar engine maintainer checking exact
key identity, source ownership, admitted allocation, failure order and complete
query cost. SQL, Python, DataFrame and CLI execution must continue to use the
same native operator families. Vortex input/output and no-fallback execution
remain unchanged; there is no new dependency, file encoding or public command.
Additive execution counters expose decoded chunks, projection reuse and sparse
mask handling. Original prospective contracts remain preserved byte-for-byte in
the [evidence packet](../benchmarks/evidence/native-text-scan-reuse-2026-10-18.json).

## Fresh attribution

Twelve serial diagnostic samples use the accepted executable for the twelve
largest candidate means in the complete 619 comparison. Every complete result
passes its retained reference. All 2,412 selected sources match the merged
runtime, with the original two-line comment correction verified byte-for-byte.
These sampled calls receive attribution credit only, not benchmark timing credit.

Q14 and Q17 expose native `ByteBuffer` acquisition, cloning and release inside
compound partial construction and partition reconciliation. The current code
owns `VarBinViewArray` for the partial lifetime but still acquires temporary
payload owners for hashing, equality, text partitioning and each reducer entry.
Its numeric owner also has one checked primitive dtype per chunk, yet every
40-byte partial slot repeats a signedness flag and its alignment padding.

Q19 exposes persistent triple-key hash-table updates and growth. Its exact key
stores two 64-bit values and a complete kind byte in a padded 24-byte record.
The complete diagnostic records 56,384,822 distinct keys; this is observed
workload shape, not a runtime admission condition. Nested/thread sample counts
and overlapping worker counters are not exclusive stage times.

## Borrow compound values and retain chunk-level signedness once

Use the existing native byte borrow while the partial owns its checked
`VarBinViewArray`. Keep the current UTF-8 validation and full byte equality.
Return a borrowed byte slice for reconciliation; persistent partition state
still copies escaping keys into its independently owned storage. No borrowed
slice may escape the partial's lifetime.

Retain the admitted numeric owner's signedness once in the partial and keep
its full 64-bit value bits in each slot. Reconstruct the original complete key
at visit/reconciliation boundaries. This reduces the partial slot from 40 to
32 bytes without a numeric-width or row-count restriction. Signedness remains
part of equality across source chunks. Charge capacity using the actual slot
size; preserve dictionary-domain binding, null refusal, cancellation, overflow,
provider-error classification and exact pressure prefix/suffix handling.

Vortex-first decision: use the existing Vortex 0.85 native primitive owner,
VarBinView buffers and checked text access. This is a ShardLoom partial-state
layout and ownership correction, not a new Vortex encoding or provider.

## Compact complete triple keys in persistent count tables

An interned string ID at most `u64::MAX >> 8` leaves room for the complete kind
byte in the same 64-bit word. Store that word beside the unchanged numeric bits
in a 16-byte internal key. Preserve every bit of signedness and the transformed
minute identity. Larger IDs use a separate full-width table with disjoint key
admission; they retain exact support without truncation or a global migration.

Both tables keep the existing pinned native hash map and count-overflow checks.
Reserve each required table's capacity before insertion, including old-plus-new
growth overlap, and retain both credits until retirement. The existing partition
mixer, 64 partitions, producer key routing, worker grants, ordered completion,
top-k comparator and complete-group count remain unchanged. Output reconstructs
the original key. A committed reservation failure still cancels and drains the
query without replay. This does not introduce spill or external execution.

Vortex-first decision: implement the private ShardLoom aggregate-state layout.
Vortex Dict codes remain bound to their value domains; a shared native array
provider does not replace this query-owned exact count table. Existing native
scan, primitive, text and allocation providers remain in their approved boundary.

## Frozen focused acceptance

The first comparison uses the same resident 99,997,497-row native input, complete
reference values/schemas, resource policy and counterbalanced public query
runner as the accepted control. It preserves every sample and failed gate.
There is no profiler, pacing, cache purge, automatic retry or best-sample choice.

Run five pairs for Q14/Q17/Q19 at parallelism 12, two single-worker pairs for
the same targets, and two pairs for controls Q2/Q10/Q16/Q23/Q28/Q29/Q33/Q34/Q35.
This is 78 complete native calls. The compound family requires at least 5% lower
sum of Q14/Q17 five-sample engine means, and the triple family requires at least
5% lower Q19 mean. Each target needs four of five positive pairs. No control may
regress by more than the larger of 10% and 0.15 engine seconds with both deltas
positive. Preserve the existing component gates: at most 10% target process-CPU
increase and peak-RSS increase no greater than the larger of 25% and 256 MiB.
These secondary resource measures stay in diagnostic evidence.

Final whole-workload acceptance requires a fresh paired all-43 comparison,
two executions per query per build, with greater than 3% reduction in the sum
of per-query mean engine times, both paired totals improved, and the same
material per-query regression rule. This prospectively uses the maintainer's
accepted greater-than-3% bar; previous failed experiments remain failed under
their original protocols. Focused gains do not add into a whole-workload claim.

Prove compact/full-width boundary identities, signed/unsigned extrema, all kind
bits, exact weighted counts, growth overlap and denial cleanup. Compound tests
must cover inline/outlined Unicode and NUL, independent dictionary domains,
mixed signedness across chunks, escaping ownership, collisions and pressure.
Run required workspace/native gates, direct bindings, public workflows and the
format/oracle matrix before PR acceptance. Review, hosted checks, installed
wheels and production verification remain merge gates. No package release is
authorized by this optimization work.

## Mixed-key screen outcome

The complete 78-call screen passes all result values, schemas, native clocks,
provider evidence and resource-policy equality checks. The compound-family sum
of engine means falls from 4.7565853418 to 4.4579609336 seconds (6.28%), but Q14
improves in only three of five pairs. Q17 improves 9.40% in all five pairs. The
triple-family mean falls from 6.9827489914 to 6.8894431998 seconds (1.34%), below
the frozen 5% requirement. The unchanged Q10 control regresses 10.83%, with both
pairs slower. All secondary resource gates pass. These observations fail the
predeclared component gates; neither mechanism is retained and the contingent
Full43 comparison is not run.

The candidate patch, complete source snapshots, executable, original protocol,
every sample and the rejection remain preserved. Focused validation passes 61
tests, including all five new tests, plus strict native all-target Clippy. The
first test run's empty-ByteBuffer pointer assertion and the initial Clippy
failure for a test-only helper are retained with their corrections. They do not
invalidate the later, source-bound comparison. Runtime sources are restored to
the accepted baseline before a different mechanism is developed.

The fresh profiles also show text decompression in both native filter evaluation
and later aggregate canonicalization. Source inspection confirms that the pinned
flat layout reader creates separate array futures for filter and projection;
its execution context has no canonical-result cache. This is a new investigation
lead, not proof of repeated decoding of an identical source range or an accepted
optimization. Further work must first prove that identity and preserve segment
pruning, native ownership, memory admission, cancellation and complete results.

## Shared text filter/projection investigation

The next mechanism is a query-owned adapter around the existing Vortex 0.85
`LayoutReader`, `ScanBuilder`, native expressions and allocator. First count
native view allocations for an identical single-range Zstd file with and without
a text filter, consuming the complete projected values in both cases. This
diagnostic receives no performance credit.

The real-file fixture confirms two native view allocations for the filtered
path versus one for the unfiltered path, with all complete values equal and
all owned memory returned. Prospective admission is a single text comparison
on a source column also projected by an aggregate, with no residual predicate
or winner-filter rewrite. The filtered text is reused; any other projected
columns are read through the original native reader only after the final mask
is known, then restored in the original declared column order.
Names, literals, row counts and query identities are not admission conditions.
The wrapper must delegate file/segment pruning and natural splits unchanged.
It may retain one decoded native text array only for its active scan-range task;
the projection future owns that lifetime, including empty, failed and cancelled
tasks. Filtered rows must be removed before projection. Dictionary and other
encoded paths must keep their provider behavior without eager canonicalization.
Partial incoming masks keep the original native filter/projection path so its
sparse-selection and provider failure ordering are preserved. Registry and
per-entry metadata are reserved before allocation; weak entries cannot retain
decoded buffers after an active projection future retires.
All decoded allocations remain charged by the same session allocator, and
range-cache metadata needs an explicit bounded reservation. No source-wide or
cross-query value cache is admitted. Vortex-first classification:
`use_vortex_native_provider`, with a private adapter inside `shardloom-vortex`.
There is no new dependency, external engine, Arrow conversion or persisted
encoding. Implementation and performance measurement remain contingent on the
small invocation-count proof and the ownership/failure contract.

The current resident file already rewrites empty-string predicates on columns
with embedded length metadata. Q28 therefore remains a control: its URL length
and predicate use the existing numeric provider. Q29 has no such rewrite and
retains its text input for domain grouping and MIN. The prospective focused
screen is five counterbalanced pairs for Q29 at parallelism 12, two pairs at
parallelism 1, and two pairs at parallelism 12 for Q2/Q10/Q11/Q12/Q13/Q14/Q16/
Q17/Q19/Q23/Q28/Q31/Q33/Q34/Q35. These are 74 complete public native calls.
Require at least 5% lower target engine mean with four of five positive pairs,
unchanged native output values/schemas, positive actual decode/reuse counters,
and the same CPU, RSS and material-control regression bars as the first screen.
The later all-43 greater-than-3% gate remains mandatory. No runtime admission
condition names these queries, columns, literals or input row counts.

## Retained text-scan outcome

The single-range persisted-file proof observes two native text-view allocations
on the original filtered path and one with reuse, with identical complete values
and all owned memory released. The 74-call focused screen passes every result,
schema, resource and control gate. Q29's five-pair engine mean decreases from
4.4667727914 to 2.929149933 seconds (34.423574%), with all five pairs improved;
both single-worker pairs also improve. Actual decode/reuse counters prove the
intended provider path ran. These focused observations are not added to other
gains.

The fresh all-43 comparison passes the original, prospective greater-than-3%
whole-workload gate. The sum of all 43 per-query two-sample engine means falls
from 57.3990281195 to 55.3462438375 seconds (3.576340%). Both paired totals
improve, all 172 complete results and schemas match, and no query crosses the
predefined material-regression rule. OS cache state is uncontrolled; this is a
bounded local comparison, not an external-engine superiority claim.

Seven new native tests cover repeated decoding, exact projection values, nulls,
Unicode, slices, column order, sparse masks, preserved dictionary encoding,
memory denial, duplicate active ranges, source failure, cancellation and owner
retirement. Required workspace gates, strict native all-target Clippy, native
Vortex/CLI tests, all 100 direct binding methods, all 4,827 public cases and all
18 format/workflow cohorts pass. The latter compare 704 native records with 704
independent reference records and reopen every cohort report.

The [acceptance report](../benchmarks/native-text-scan-reuse-2026-10-18.md) records
the mechanism, reproducible comparison, complete experiment history and proof
limits. The packet retains all 324 paired calls from the rejected and accepted
experiments, immutable contracts and source identities. Final-head hosted review,
three installed-wheel lanes and preview/production verification are separate
integration gates. No package publication or input rewrite is part of this work.
