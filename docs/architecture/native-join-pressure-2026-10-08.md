# Native joins under an explicit shared spill policy

Status: complete local runtime acceptance under PERF-02/03/06/10/12 and CG-20/21;
hosted integration remains pending. The
[acceptance report](../benchmarks/native-join-pressure-2026-10-08.md) binds runtime
`92ab9f20`, complete constrained workloads, public regressions and independent
packet inspection. The [phase plan](phased-execution-plan.md) owns progress.
This continues the maintainer's
[remaining local scope](native-local-completion-scope-2026-10-07.md) after
accepted general aggregation and streamed ordering. It does not close the
broader support envelope or establish a performance claim.

## Decision and reuse

The expert comparator is an external-memory join that preserves complete
relational semantics, output order and ownership under pressure. Extend the
existing native Join with an explicitly selected spill strategy. Without a
spill policy, retain its resident build/index strategy and deterministic growth
denial. Never catch an execution error and rerun through another strategy.

| Existing owner | Shared extension |
| --- | --- |
| Join binding, Batch/KeyColumn and RowIndex | Preserve all seven kinds, exact typed/nested key semantics, cross-integer equality and null nonmatches. Hashes narrow candidates; equality still decides every match. |
| Join Condition and payload gathering | Share bounded ON evaluation, outer/semi/anti decisions and output construction. Preserve ON candidate order and whole-batch evaluation before semi/anti short-circuiting. |
| Relational Ordering | Retain a completed bounded resident order or native sorted run for exact block lookup. Reuse stable sorting and adjacent merges for build records, matched ordinals and unmatched-right restoration. |
| QueryRunStore/Reader and spill State | Reuse one grant, quota, file format, source-generation validation, exact row-range scans, reader counters and cleanup. Add bounded positional reads over the same held validated file. |
| Native payload/private-record builders | Compact retained build payload and selected candidates into credited Vortex arrays. Share the existing private ordinal's structural-metadata ownership. |
| Prepared runner and finite input adapter | Execute the same tree once with one single-use batch source, on either join side, alongside admitted ordinary file/resident sources. Preserve full drain and final completion. |

## Vortex-first provider decision

Classification: `implement_shardloom_kernel` for relational join orchestration,
using existing pinned Vortex 0.85.0 native array and file providers. The existing
Vortex ArrayRef/DType/validity, native take/builders, Flat file writer and
`VortexFile::scan().with_row_range(...).with_split_by(RowCount(...))` already
supply representation, selection and bounded run I/O. They do not supply this
ShardLoom Join Spec's ON, output-order, quota, admission and publication contract.
Reuse those providers and the existing Join semantics; do not add another file,
reader, query route or external query-engine integration.

Keep the implementation in `shardloom-vortex`, behind `vortex-local-primitives`
and `vortex-write` for the spill strategy. There is no dependency/version change,
Arrow execution, unsafe code or answer cache. Native I/O and execution reports
must expose actual spill work and `fallback_attempted=false` /
`external_engine_invoked=false`.

## Build records and lookup

Consume the right relation once. Each private typed record carries the existing
nullable key hash, a checked original right ordinal, and the right payload as a
struct with its root validity and original child types. Private field names live
outside the user payload's namespace. Reserve descriptors and parent metadata
before construction; compact the payload so it cannot retain a private streamed
input. When the condition, keys and output need no right fields, an internal
Boolean column retains cardinality without copying payload or introducing a
zero-field nested struct. Exact key hashing uses the existing Batch contract.

Order by hash with explicit null placement. Stable ties preserve original right
order even for a hot key or hash collision. If the bounded order never spills,
retain its admitted compact resident result. Otherwise seal its native runs to
one run using the existing stable merger. No hash table or bitmap proportional
to the complete spilled build relation remains resident.

For each left row in original input order, find the hash range by exact binary
search over bounded blocks of that completed relation. A null key has no key
matches. With no equality keys, all right rows are candidates for Cross or ON.
Retain at most one search block; release it before loading a replacement.
Use the already-open generation-validated reader rather than reopening and
rehashing the entire run for each probe. Positional reads must be aligned,
bounded, cancellable and retain the same metadata/work/path owners as sequential
reads. Validate source generation before and after each read and before success.

Filter hash collisions through exact Batch equality. Compact selected candidate
rows before retaining them across block boundaries, so a small candidate batch
cannot pin arbitrarily many large blocks. Pack up to the existing `batch_rows`
exact-key candidates in original right order before evaluating ON. This preserves
the resident evaluator's batch/error behavior and semi/anti short-circuit boundary.
Unknown ON is not true. A key candidate rejected by ON does not mark the right
row matched and must not suppress an outer null extension.

Emit matches in left-input/right-duplicate order through the common join output
builder. Left/Full null extension and Semi/Anti emission follow the existing
decision. Delivered arrays keep independent native credits through retained
children, clones and slices.

## Right and full outer completion

For Right/Full joins only, append matched sorted-right positions to bounded
native buffers and feed them to the same Ordering. After the complete left side,
read those positions in sorted order and deduplicate adjacent positions. Merge
them against a sequential pass over the sorted right relation to find unmatched
rows. Feed unmatched records to an Ordering keyed by original right ordinal,
then null-extend the left side through the common join builder.

This preserves original unmatched-right order without a resident match bitmap.
Repeated matches can create repeated disk ordinals; actual cardinality and quota
remain explicit. Other join kinds do not create this state. Hot keys and Cross
joins may require quadratic candidate/output work; they must remain bounded and
cancellable, not be represented as a speed optimization.

## Streaming, ownership and failure boundaries

Admit a Join-containing tree only when the finite batch URI occurs exactly once.
Other admitted file/resident scans can occur normally. Count the batch URI in
the full plan before producer demand; repeated batch use and multiple batch
producers remain deterministic unsupported cases pending an explicit spool.
The existing four batch-input types, frame/batch-count limits and output
destination restrictions remain unchanged. Ordinary file-backed inputs do not
inherit the transport's finite limits.

If the batch source is the build side, both resident and spilled joins must
release its private input owner before requesting the next batch. If it is the
probe side, no candidate or delivered result may escape with that private owner.
Nested joins, aggregate/order composition and zero-output limits still execute
and drain the complete source once. Source statistics cannot stand in for its
end event. Final publication follows complete source validation, sink work and
owned spill cleanup.

Right records, current left input, candidate copies, ON temporaries, matching
ordinals, outer restoration, native readers/footers and output overlap under one
query grant and one disk quota. Flush thresholds estimate retained state; they
are not allocation or total-process RSS limits. A single oversized value, large
footer/schema, exhausted grant or quota can still deny deterministically.

Producer/schema errors, ON errors, failed reservations, cancellation, damaged or
replaced runs, source change, consumer failure and failed cleanup prevent success.
Existing destinations remain protected by the shared writer contract. Recovery
identifies owned dead-query runs for cleanup and restart; it does not resume a
partially completed join or certify automatic output-transaction recovery.

## Alternatives and risks

Hash partitioning would need a separate hot-key strategy and output reordering.
Sorting both sides would change probe/ON order unless followed by more retained
state. Replaying the complete unsorted build for every left row would preserve
order but impose full scans on selective equijoins. The chosen strategy reuses
existing sorted runs and only retains bounded search/candidate state, at the
cost of block lookups and repeated candidates. Preserve the resident default.

Matched-ordinal duplication and footer growth can exhaust resources. Exact
collision checks, null-root masking, semi/anti error order and retained owner
lifetimes are the primary correctness risks. Larger-than-grant acceptance must
use real complete workflows, not infer success from a temporary-file count.

## Acceptance and verification

- Compare resident and spill strategies with independent complete outputs for
  all seven join kinds, no-key ON, compound/nullable/colliding keys, duplicate
  hot keys, empty sides, root-null rows, typed/decimal/temporal/nested payloads,
  integer-domain boundaries and different input/output block sizes.
- Assert ON-versus-WHERE, true/false/unknown, evaluation errors, and semi/anti
  whole-candidate-batch short-circuit behavior. Verify original unmatched-right
  order when hash order differs and when matches repeat across probe batches.
- Prove constrained resident denial, ample resident control and complete native
  spill under the same constrained grant. Check real disk work, bounded open
  readers/blocks, source/input release and zero final native credits. Report
  public and native pressure grants separately.
- Exercise one-shot sources on either side, nested joins and aggregate/order/
  limit composition through incremental results and native write/reopen. Existing
  file/resident writers must retain complete typed/nested semantics. Repeated
  source rejection must precede producer consumption.
- Exercise late producer/ON/consumer errors, mid-probe cancellation, quota and
  allocation denial, corrupt/replaced real runs, source mutation, overwrite
  preservation and dead-owner cleanup/restart while retaining unknown files.
- Run focused native join/run-store/batch tests and exact Python/CLI coverage,
  required fmt/clippy/workspace gates, complete public/direct/adapter regressions
  and final frozen Full43 correctness acceptance under serial storage/process
  guards. Independently inspect the packet, review source and complete hosted
  integration before support claims.

Window/pivot pressure, repeated-source spooling, execution resume, compatibility
streaming writers/fanout, broader intake types, general allocation coverage and
platform/release acceptance retain their separate owners. Cost-aware merge
scheduling and the other conditional investigations keep their own frozen
retain/drop gates; no performance or version claim follows from this capability.
